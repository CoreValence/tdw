use opentelemetry::Context;
use opentelemetry::KeyValue;
use opentelemetry::trace::{Span, TraceContextExt, Tracer};
use pgrx::bgworkers::{BackgroundWorker, SignalWakeFlags};
use pgrx::prelude::*;
use std::collections::HashMap;
use std::net::ToSocketAddrs;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use crate::guc::{BATCH_MAX, BATCH_WAIT_MS, TB_ADDR, TB_CLUSTER_ID};
use crate::obs::{self, Instruments};
use crate::pack::{pack_account, pack_balance, pack_transfer};
use crate::shmem::{
    BATCH_LEG_LEN, CAPACITY, ERR_BUF_LEN, MAX_BATCH_LEGS, MAX_QUERY_ROWS, OP_CREATE_ACCOUNT,
    OP_CREATE_TRANSFER, OP_CREATE_TRANSFER_BATCH, OP_GET_ACCOUNT_BALANCES,
    OP_GET_ACCOUNT_TRANSFERS, OP_LOOKUP_ACCOUNT, OP_LOOKUP_TRANSFER, OP_QUERY_ACCOUNTS,
    OP_QUERY_TRANSFERS, RECORD_LEN, RESULT_BUF_LEN, RESULTS, RING, S_DONE, S_ERROR, S_IN_FLIGHT,
    S_PENDING, WORKER_LATCH,
};

// pgrx's attach_signal_handlers gets clobbered because TB's Zig runtime sets
// up its own signal mask/handlers after Client::new. Install our own raw
// handler AFTER the TB client is up and check this atomic directly.
static GOT_SIGTERM: AtomicBool = AtomicBool::new(false);

extern "C" fn beetle_sigterm_handler(_: libc::c_int) {
    GOT_SIGTERM.store(true, Ordering::SeqCst);
}

unsafe fn install_sigterm_handler() {
    unsafe {
        let mut sa: libc::sigaction = std::mem::zeroed();
        sa.sa_sigaction = beetle_sigterm_handler as *const () as usize;
        libc::sigemptyset(&mut sa.sa_mask);
        sa.sa_flags = 0;
        libc::sigaction(libc::SIGTERM, &sa, std::ptr::null_mut());

        // Unblock SIGTERM on the current thread in case Zig left it blocked.
        let mut unblock: libc::sigset_t = std::mem::zeroed();
        libc::sigemptyset(&mut unblock);
        libc::sigaddset(&mut unblock, libc::SIGTERM);
        libc::pthread_sigmask(libc::SIG_UNBLOCK, &unblock, std::ptr::null_mut());
    }
}

// TigerBeetle's client parses a comma-separated list of `port` or `ip:port`
// entries, but will not resolve DNS. If a user supplies `host:port`, look up
// the hostname via the resolver and rewrite it to `ip:port` before handing
// the string to the TB client.
fn resolve_tb_addr(raw: &str) -> String {
    raw.split(',')
        .map(|entry| {
            let e = entry.trim();
            if e.parse::<std::net::SocketAddr>().is_ok() || e.parse::<u16>().is_ok() {
                return e.to_string();
            }
            match e.to_socket_addrs() {
                Ok(mut it) => match it.next() {
                    Some(sa) => sa.to_string(),
                    None => error!("beetle: could not resolve {:?}: no addresses returned", e),
                },
                Err(err) => error!("beetle: could not resolve {:?}: {}", e, err),
            }
        })
        .collect::<Vec<_>>()
        .join(",")
}

struct Pending {
    slot_idx: usize,
    op: u8,
    id: u128,
    debit_id: u128,
    credit_id: u128,
    pending_id: u128,
    amount: u128,
    ledger: u32,
    code: u16,
    flags: u32,
    limit: u32,
    // CLOCK_MONOTONIC ns stamped by the backend at S_PENDING; diffed against
    // now() at drain to derive pending_wait.
    pending_ns: u64,
    // OP_CREATE_TRANSFER_BATCH only: leg descriptors unpacked from result_buf.
    batch_legs: Vec<BatchLeg>,
}

struct BatchLeg {
    id: u128,
    debit_id: u128,
    credit_id: u128,
    pending_id: u128,
    amount: u128,
    ledger: u32,
    code: u16,
    flags: u16,
}

enum Outcome {
    // Single-record writes: 16-byte id packed as one record (count = 1).
    // Single-row reads: one RECORD_LEN record (count = 1).
    // Multi-row reads: N records (each RECORD_LEN bytes) concatenated.
    // Batch writes: N × 16-byte ids packed back-to-back (count = N).
    Ok { count: u32, bytes: Vec<u8> },
    Err(String),
}

fn outcome_id(id: u128) -> Outcome {
    let mut bytes = vec![0u8; RECORD_LEN];
    bytes[..16].copy_from_slice(&id.to_le_bytes());
    Outcome::Ok { count: 1, bytes }
}

fn outcome_ids(ids: &[u128]) -> Outcome {
    // Pad each id out to RECORD_LEN so the client side can walk them with
    // ReadBack::record() at the usual RECORD_LEN stride, same as any other
    // read. Only the first 16 bytes of each slot carry data.
    let mut bytes = vec![0u8; ids.len() * RECORD_LEN];
    for (i, id) in ids.iter().enumerate() {
        let o = i * RECORD_LEN;
        bytes[o..o + 16].copy_from_slice(&id.to_le_bytes());
    }
    Outcome::Ok {
        count: ids.len() as u32,
        bytes,
    }
}

fn outcome_single(record: [u8; RECORD_LEN]) -> Outcome {
    Outcome::Ok {
        count: 1,
        bytes: record.to_vec(),
    }
}

enum BatchErr {
    SendAll(String),
    PerIndex(Vec<(u32, String)>),
}

#[pg_guard]
#[unsafe(no_mangle)]
pub extern "C-unwind" fn beetle_worker_main(_arg: pg_sys::Datum) {
    unsafe {
        *WORKER_LATCH.exclusive() = pg_sys::MyLatch as usize;
    }

    let tb_addr_raw = TB_ADDR
        .get()
        .and_then(|s| s.into_string().ok())
        .unwrap_or_default();
    if tb_addr_raw.is_empty() {
        error!("beetle: beetle.tb_addr must be set (e.g. 'host:port' or 'port')");
    }
    let tb_addr = resolve_tb_addr(&tb_addr_raw);

    let cluster_id_raw = TB_CLUSTER_ID
        .get()
        .and_then(|s| s.into_string().ok())
        .unwrap_or_default();
    let cluster_id: u128 = cluster_id_raw.parse().unwrap_or_else(|e| {
        error!(
            "beetle: invalid beetle.tb_cluster_id={:?}: {}",
            cluster_id_raw, e
        )
    });

    log!(
        "beetle: connecting to TigerBeetle at {} (resolved from {}) cluster {}",
        tb_addr,
        tb_addr_raw,
        cluster_id
    );

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("beetle: tokio rt");

    let client = match tigerbeetle_unofficial::Client::new(cluster_id, tb_addr.as_bytes()) {
        Ok(c) => c,
        Err(e) => error!("beetle: failed to connect to TigerBeetle: {}", e),
    };

    // Initialize OTLP exporters inside rt.block_on so tonic sees a reactor.
    // No-op if beetle.otlp_endpoint is empty.
    obs::init(&rt);

    // Install our own SIGTERM handler AFTER Client::new. pgrx's
    // attach_signal_handlers is insufficient because TB's Zig runtime blocks
    // SIGTERM on every thread it touches; checking pgrx's flag stays false
    // indefinitely during postmaster shutdown.
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP);
    unsafe { install_sigterm_handler() };

    log!("beetle: worker ready (capacity={})", CAPACITY);

    loop {
        if GOT_SIGTERM.load(Ordering::SeqCst) {
            log!("beetle: sigterm observed");
            break;
        }
        let _ = BackgroundWorker::sighup_received();

        let pending = drain_pending();
        if !pending.is_empty() {
            let inst = obs::instruments();

            // Drain-time metrics: pending_wait per slot, submit_count per op,
            // drain_size, plus refresh the occupancy gauge. Done before the
            // TB calls so the histograms reflect queue wait, not TB cost.
            let drain_cx = inst.map(|i| {
                let now = obs::now_monotonic_ns();
                for p in &pending {
                    let wait_ns = now.saturating_sub(p.pending_ns);
                    i.pending_wait_ms.record(
                        wait_ns as f64 / 1_000_000.0,
                        &[KeyValue::new("op", obs::op_label(p.op))],
                    );
                    i.submit_count
                        .add(1, &[KeyValue::new("op", obs::op_label(p.op))]);
                }
                i.drain_size.record(pending.len() as u64, &[]);
                let (occ_p, occ_f) = count_occupancy();
                obs::set_occupancy(occ_p, occ_f);

                let mut span = i.tracer.start("worker.drain");
                span.set_attribute(KeyValue::new("drain.size", pending.len() as i64));
                Context::current_with_span(span)
            });
            // Attach drain_cx as current context so TB-call child spans parent
            // off it. Cloned because drain_cx itself is needed later to call
            // .span().end().
            let _attach = drain_cx.as_ref().map(|cx| cx.clone().attach());

            let outcomes = run_batch(&rt, &client, &pending, inst);

            let publish_start = Instant::now();
            let latches = publish_outcomes(pending, outcomes, inst);
            if let Some(i) = inst {
                i.publish_ms
                    .record(publish_start.elapsed().as_secs_f64() * 1000.0, &[]);
            }

            drop(_attach);
            if let Some(cx) = drain_cx {
                cx.span().end();
            }

            for latch in latches {
                unsafe {
                    pg_sys::SetLatch(latch as *mut pg_sys::Latch);
                }
            }
            continue;
        }

        BackgroundWorker::wait_latch(Some(Duration::from_millis(BATCH_WAIT_MS.get() as u64)));
    }

    log!("beetle: worker exiting");
    // Force-exit: the TB client spawns a native Zig thread that survives
    // Rust's drop path and blocks the process from terminating, which made
    // postmaster shutdown hang past pg_ctl's timeout. OS cleans up on exit.
    std::mem::forget(client);
    std::mem::forget(rt);
    std::process::exit(0);
}

fn drain_pending() -> Vec<Pending> {
    let max_batch = BATCH_MAX.get() as usize;
    let mut out: Vec<Pending> = Vec::with_capacity(max_batch);
    // Track slots that need leg data copied out of the pool.
    let mut batch_slots: Vec<(usize, usize)> = Vec::new();
    {
        let mut guard = RING.exclusive();
        for i in 0..CAPACITY {
            if out.len() >= max_batch {
                break;
            }
            let s = &mut guard.slots[i];
            if s.state == S_PENDING {
                s.state = S_IN_FLIGHT;
                let is_batch = s.op == OP_CREATE_TRANSFER_BATCH;
                let idx_in_out = out.len();
                out.push(Pending {
                    slot_idx: i,
                    op: s.op,
                    id: u128::from_le_bytes(s.id),
                    debit_id: u128::from_le_bytes(s.debit_id),
                    credit_id: u128::from_le_bytes(s.credit_id),
                    pending_id: u128::from_le_bytes(s.pending_id),
                    amount: s.amount,
                    ledger: s.ledger,
                    code: s.code,
                    flags: s.flags,
                    limit: s.limit,
                    pending_ns: s.pending_ns,
                    batch_legs: Vec::new(),
                });
                if is_batch {
                    batch_slots.push((idx_in_out, i));
                }
            }
        }
    }

    // Copy leg data out of the pool without holding RING. Pool entries for
    // slots in IN_FLIGHT are stable against other parties until we publish
    // the result, so a shared read is enough.
    if !batch_slots.is_empty() {
        let pool = RESULTS.share();
        for (out_idx, slot_idx) in batch_slots {
            let p = &mut out[out_idx];
            let n = (p.limit as usize).min(MAX_BATCH_LEGS);
            p.batch_legs = (0..n)
                .map(|k| unpack_batch_leg(&pool.bufs[slot_idx], k))
                .collect();
        }
    }

    out
}

// Observable-gauge callback reads occupancy from worker-local statics rather
// than re-taking RING from the export thread; the worker refreshes those
// statics each drain by calling us under RING.share().
fn count_occupancy() -> (u64, u64) {
    let guard = RING.share();
    let mut pending = 0u64;
    let mut in_flight = 0u64;
    for s in &guard.slots {
        match s.state {
            S_PENDING => pending += 1,
            S_IN_FLIGHT => in_flight += 1,
            _ => {}
        }
    }
    (pending, in_flight)
}

// Wrap one TB call group in a child span (parented off worker.drain via the
// attached context) plus a tb_call_ms histogram entry. No-op when telemetry
// is disabled.
fn time_tb<T>(
    inst: Option<&'static Instruments>,
    op_label: &'static str,
    f: impl FnOnce() -> T,
) -> T {
    let start = Instant::now();
    let mut span = inst.map(|i| i.tracer.start(format!("tb.{op_label}")));
    let out = f();
    if let Some(s) = span.as_mut() {
        s.end();
    }
    if let Some(i) = inst {
        i.tb_call_ms.record(
            start.elapsed().as_secs_f64() * 1000.0,
            &[KeyValue::new("op", op_label)],
        );
    }
    out
}

fn unpack_batch_leg(buf: &[u8; RESULT_BUF_LEN], idx: usize) -> BatchLeg {
    let o = idx * BATCH_LEG_LEN;
    BatchLeg {
        id: u128::from_le_bytes(buf[o..o + 16].try_into().unwrap()),
        debit_id: u128::from_le_bytes(buf[o + 16..o + 32].try_into().unwrap()),
        credit_id: u128::from_le_bytes(buf[o + 32..o + 48].try_into().unwrap()),
        pending_id: u128::from_le_bytes(buf[o + 48..o + 64].try_into().unwrap()),
        amount: u128::from_le_bytes(buf[o + 64..o + 80].try_into().unwrap()),
        ledger: u32::from_le_bytes(buf[o + 80..o + 84].try_into().unwrap()),
        code: u16::from_le_bytes(buf[o + 84..o + 86].try_into().unwrap()),
        flags: u16::from_le_bytes(buf[o + 86..o + 88].try_into().unwrap()),
    }
}

fn run_batch(
    rt: &tokio::runtime::Runtime,
    client: &tigerbeetle_unofficial::Client,
    pending: &[Pending],
    inst: Option<&'static Instruments>,
) -> Vec<Outcome> {
    let mut outcomes: Vec<Outcome> = (0..pending.len())
        .map(|_| Outcome::Err("not processed".into()))
        .collect();

    let mut account_idx: Vec<usize> = Vec::new();
    let mut transfer_idx: Vec<usize> = Vec::new();
    let mut lookup_account_idx: Vec<usize> = Vec::new();
    let mut lookup_transfer_idx: Vec<usize> = Vec::new();
    for (i, p) in pending.iter().enumerate() {
        match p.op {
            OP_CREATE_ACCOUNT => account_idx.push(i),
            OP_CREATE_TRANSFER => transfer_idx.push(i),
            OP_LOOKUP_ACCOUNT => lookup_account_idx.push(i),
            OP_LOOKUP_TRANSFER => lookup_transfer_idx.push(i),
            _ => {}
        }
    }

    if !account_idx.is_empty() {
        let accounts: Vec<_> = account_idx
            .iter()
            .map(|&i| {
                let p = &pending[i];
                tigerbeetle_unofficial::Account::new(p.id, p.ledger, p.code).with_flags(
                    tigerbeetle_unofficial::account::Flags::from_bits_retain(p.flags as u16),
                )
            })
            .collect();
        let res = time_tb(inst, "create_accounts", || {
            rt.block_on(client.create_accounts(accounts))
        });
        apply_batch_ids(
            &account_idx,
            pending,
            &mut outcomes,
            res,
            format_accounts_err,
        );
    }

    if !transfer_idx.is_empty() {
        let transfers: Vec<_> = transfer_idx
            .iter()
            .map(|&i| {
                let p = &pending[i];
                let mut t = tigerbeetle_unofficial::Transfer::new(p.id)
                    .with_debit_account_id(p.debit_id)
                    .with_credit_account_id(p.credit_id)
                    .with_amount(p.amount)
                    .with_ledger(p.ledger)
                    .with_code(p.code)
                    .with_flags(tigerbeetle_unofficial::transfer::Flags::from_bits_retain(
                        p.flags as u16,
                    ));
                if p.pending_id != 0 {
                    t = t.with_pending_id(p.pending_id);
                }
                t
            })
            .collect();
        let res = time_tb(inst, "create_transfers", || {
            rt.block_on(client.create_transfers(transfers))
        });
        apply_batch_ids(
            &transfer_idx,
            pending,
            &mut outcomes,
            res,
            format_transfers_err,
        );
    }

    // Each batch slot runs as its own create_transfers call so that a LINKED
    // chain inside the batch stays contiguous in the TB request. Folding batch
    // legs into the shared transfer_idx call would work but makes the error
    // index → slot mapping ugly; the extra round-trip is cheap relative to
    // the typical cost of building such a batch.
    for (i, p) in pending.iter().enumerate() {
        if p.op != OP_CREATE_TRANSFER_BATCH {
            continue;
        }
        let transfers: Vec<_> = p
            .batch_legs
            .iter()
            .map(|leg| {
                let mut t = tigerbeetle_unofficial::Transfer::new(leg.id)
                    .with_debit_account_id(leg.debit_id)
                    .with_credit_account_id(leg.credit_id)
                    .with_amount(leg.amount)
                    .with_ledger(leg.ledger)
                    .with_code(leg.code)
                    .with_flags(tigerbeetle_unofficial::transfer::Flags::from_bits_retain(
                        leg.flags,
                    ));
                if leg.pending_id != 0 {
                    t = t.with_pending_id(leg.pending_id);
                }
                t
            })
            .collect();
        outcomes[i] = match time_tb(inst, "create_transfer_batch", || {
            rt.block_on(client.create_transfers(transfers))
        }) {
            Ok(()) => {
                let ids: Vec<u128> = p.batch_legs.iter().map(|leg| leg.id).collect();
                outcome_ids(&ids)
            }
            Err(e) => match format_transfers_err(&e) {
                // Any per-index error fails the whole batch from the client's
                // point of view — LINKED rollback means no leg in the chain
                // actually committed, so returning partial ids would be a lie.
                BatchErr::SendAll(msg) => Outcome::Err(msg),
                BatchErr::PerIndex(errs) => {
                    let first = errs
                        .iter()
                        .min_by_key(|(idx, _)| *idx)
                        .map(|(idx, msg)| format!("leg {}: {}", idx, msg))
                        .unwrap_or_else(|| "batch failed".into());
                    Outcome::Err(first)
                }
            },
        };
    }

    if !lookup_account_idx.is_empty() {
        let ids: Vec<u128> = lookup_account_idx.iter().map(|&i| pending[i].id).collect();
        match time_tb(inst, "lookup_accounts", || {
            rt.block_on(client.lookup_accounts(ids))
        }) {
            Ok(found) => {
                let by_id: HashMap<u128, tigerbeetle_unofficial::Account> =
                    found.into_iter().map(|a| (a.id(), a)).collect();
                for &i in &lookup_account_idx {
                    outcomes[i] = match by_id.get(&pending[i].id) {
                        Some(a) => outcome_single(pack_account(a)),
                        None => Outcome::Err("not found".into()),
                    };
                }
            }
            Err(e) => {
                for &i in &lookup_account_idx {
                    outcomes[i] = Outcome::Err(format!("{e}"));
                }
            }
        }
    }

    if !lookup_transfer_idx.is_empty() {
        let ids: Vec<u128> = lookup_transfer_idx.iter().map(|&i| pending[i].id).collect();
        match time_tb(inst, "lookup_transfers", || {
            rt.block_on(client.lookup_transfers(ids))
        }) {
            Ok(found) => {
                let by_id: HashMap<u128, tigerbeetle_unofficial::Transfer> =
                    found.into_iter().map(|t| (t.id(), t)).collect();
                for &i in &lookup_transfer_idx {
                    outcomes[i] = match by_id.get(&pending[i].id) {
                        Some(t) => outcome_single(pack_transfer(t)),
                        None => Outcome::Err("not found".into()),
                    };
                }
            }
            Err(e) => {
                for &i in &lookup_transfer_idx {
                    outcomes[i] = Outcome::Err(format!("{e}"));
                }
            }
        }
    }

    // Query-shape ops are issued one-per-slot: TB's get_/query_ calls take a
    // single filter per request, so there's nothing to batch across slots.
    for (i, p) in pending.iter().enumerate() {
        match p.op {
            OP_GET_ACCOUNT_TRANSFERS => {
                outcomes[i] = time_tb(inst, "get_account_transfers", || {
                    run_account_transfers(rt, client, p)
                });
            }
            OP_GET_ACCOUNT_BALANCES => {
                outcomes[i] = time_tb(inst, "get_account_balances", || {
                    run_account_balances(rt, client, p)
                });
            }
            OP_QUERY_ACCOUNTS => {
                outcomes[i] = time_tb(inst, "query_accounts", || run_query_accounts(rt, client, p));
            }
            OP_QUERY_TRANSFERS => {
                outcomes[i] = time_tb(inst, "query_transfers", || {
                    run_query_transfers(rt, client, p)
                });
            }
            _ => {}
        }
    }

    outcomes
}

fn pack_records<T, F>(records: &[T], pack: F) -> (u32, Vec<u8>)
where
    F: Fn(&T) -> [u8; RECORD_LEN],
{
    let capped = records.len().min(MAX_QUERY_ROWS as usize);
    let mut bytes = Vec::with_capacity(capped * RECORD_LEN);
    for r in records.iter().take(capped) {
        bytes.extend_from_slice(&pack(r));
    }
    (capped as u32, bytes)
}

fn run_account_transfers(
    rt: &tokio::runtime::Runtime,
    client: &tigerbeetle_unofficial::Client,
    p: &Pending,
) -> Outcome {
    let limit = p.limit.clamp(1, MAX_QUERY_ROWS);
    let filter = Box::new(
        tigerbeetle_unofficial::account::Filter::new(p.id, limit)
            .with_flags(tigerbeetle_unofficial::account::FilterFlags::from_bits_retain(p.flags)),
    );
    match rt.block_on(client.get_account_transfers(filter)) {
        Ok(v) => {
            let (count, bytes) = pack_records(&v, pack_transfer);
            Outcome::Ok { count, bytes }
        }
        Err(e) => Outcome::Err(format!("{e}")),
    }
}

fn run_account_balances(
    rt: &tokio::runtime::Runtime,
    client: &tigerbeetle_unofficial::Client,
    p: &Pending,
) -> Outcome {
    let limit = p.limit.clamp(1, MAX_QUERY_ROWS);
    let filter = Box::new(
        tigerbeetle_unofficial::account::Filter::new(p.id, limit)
            .with_flags(tigerbeetle_unofficial::account::FilterFlags::from_bits_retain(p.flags)),
    );
    match rt.block_on(client.get_account_balances(filter)) {
        Ok(v) => {
            let (count, bytes) = pack_records(&v, pack_balance);
            Outcome::Ok { count, bytes }
        }
        Err(e) => Outcome::Err(format!("{e}")),
    }
}

fn run_query_accounts(
    rt: &tokio::runtime::Runtime,
    client: &tigerbeetle_unofficial::Client,
    p: &Pending,
) -> Outcome {
    let limit = p.limit.clamp(1, MAX_QUERY_ROWS);
    let mut filter = tigerbeetle_unofficial::QueryFilter::new(limit)
        .with_flags(tigerbeetle_unofficial::core::query_filter::Flags::from_bits_retain(p.flags));
    if p.ledger != 0 {
        filter.set_ledger(p.ledger);
    }
    if p.code != 0 {
        filter.set_code(p.code);
    }
    match rt.block_on(client.query_accounts(Box::new(filter))) {
        Ok(v) => {
            let (count, bytes) = pack_records(&v, pack_account);
            Outcome::Ok { count, bytes }
        }
        Err(e) => Outcome::Err(format!("{e}")),
    }
}

fn run_query_transfers(
    rt: &tokio::runtime::Runtime,
    client: &tigerbeetle_unofficial::Client,
    p: &Pending,
) -> Outcome {
    let limit = p.limit.clamp(1, MAX_QUERY_ROWS);
    let mut filter = tigerbeetle_unofficial::QueryFilter::new(limit)
        .with_flags(tigerbeetle_unofficial::core::query_filter::Flags::from_bits_retain(p.flags));
    if p.ledger != 0 {
        filter.set_ledger(p.ledger);
    }
    if p.code != 0 {
        filter.set_code(p.code);
    }
    match rt.block_on(client.query_transfers(Box::new(filter))) {
        Ok(v) => {
            let (count, bytes) = pack_records(&v, pack_transfer);
            Outcome::Ok { count, bytes }
        }
        Err(e) => Outcome::Err(format!("{e}")),
    }
}

fn apply_batch_ids<E, F>(
    indices: &[usize],
    pending: &[Pending],
    outcomes: &mut [Outcome],
    result: Result<(), E>,
    format: F,
) where
    F: Fn(&E) -> BatchErr,
{
    match result {
        Ok(()) => {
            for &i in indices {
                outcomes[i] = outcome_id(pending[i].id);
            }
        }
        Err(e) => match format(&e) {
            BatchErr::SendAll(msg) => {
                for &i in indices {
                    outcomes[i] = Outcome::Err(msg.clone());
                }
            }
            BatchErr::PerIndex(mut errs) => {
                // Per-index errors come in a short list (<= batch size), so a
                // linear scan + swap_remove is cheaper than a HashMap.
                for (offset, &i) in indices.iter().enumerate() {
                    let offset_u32 = offset as u32;
                    outcomes[i] = match errs.iter().position(|(idx, _)| *idx == offset_u32) {
                        Some(pos) => Outcome::Err(errs.swap_remove(pos).1),
                        None => outcome_id(pending[i].id),
                    };
                }
            }
        },
    }
}

fn format_accounts_err(e: &tigerbeetle_unofficial::error::CreateAccountsError) -> BatchErr {
    use tigerbeetle_unofficial::error::CreateAccountsError as E;
    match e {
        E::Send(s) => BatchErr::SendAll(s.to_string()),
        E::Api(api) => BatchErr::PerIndex(
            api.as_slice()
                .iter()
                .map(|x| (x.index(), format!("{}", x.inner())))
                .collect(),
        ),
        other => BatchErr::SendAll(format!("{other}")),
    }
}

fn format_transfers_err(e: &tigerbeetle_unofficial::error::CreateTransfersError) -> BatchErr {
    use tigerbeetle_unofficial::error::CreateTransfersError as E;
    match e {
        E::Send(s) => BatchErr::SendAll(s.to_string()),
        E::Api(api) => BatchErr::PerIndex(
            api.as_slice()
                .iter()
                .map(|x| (x.index(), format!("{}", x.inner())))
                .collect(),
        ),
        other => BatchErr::SendAll(format!("{other}")),
    }
}

fn publish_outcomes(
    pending: Vec<Pending>,
    outcomes: Vec<Outcome>,
    inst: Option<&'static Instruments>,
) -> Vec<usize> {
    // Bump submit_errors before taking any locks; outcomes are owned here so
    // this is just a read over the vec.
    if let Some(i) = inst {
        for (p, outcome) in pending.iter().zip(outcomes.iter()) {
            if matches!(outcome, Outcome::Err(_)) {
                i.submit_errors
                    .add(1, &[KeyValue::new("op", obs::op_label(p.op))]);
            }
        }
    }

    // Write pool entries for successful outcomes under RESULTS first, then
    // flip state (plus error payloads, which live in Slot, not the pool)
    // under RING. Order matters: the backend reads state under RING and the
    // pool under RESULTS, so the pool must be settled before state=DONE is
    // visible.
    {
        let mut pool = RESULTS.exclusive();
        for (p, outcome) in pending.iter().zip(outcomes.iter()) {
            if let Outcome::Ok { bytes, .. } = outcome {
                let buf = &mut pool.bufs[p.slot_idx];
                let n = bytes.len().min(RESULT_BUF_LEN);
                buf[..n].copy_from_slice(&bytes[..n]);
                if n < RESULT_BUF_LEN {
                    buf[n..].fill(0);
                }
            }
        }
    }

    let mut latches = Vec::with_capacity(pending.len());
    let mut guard = RING.exclusive();
    for (p, outcome) in pending.into_iter().zip(outcomes) {
        let s = &mut guard.slots[p.slot_idx];
        match outcome {
            Outcome::Ok { count, .. } => {
                s.result_count = count;
                s.state = S_DONE;
            }
            Outcome::Err(msg) => {
                let bytes = msg.as_bytes();
                let n = bytes.len().min(ERR_BUF_LEN);
                s.error_msg[..n].copy_from_slice(&bytes[..n]);
                s.error_len = n as u16;
                s.state = S_ERROR;
            }
        }
        if s.session_latch != 0 {
            latches.push(s.session_latch);
        }
    }
    latches
}
