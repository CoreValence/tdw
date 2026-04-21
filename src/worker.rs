use pgrx::bgworkers::{BackgroundWorker, SignalWakeFlags};
use pgrx::prelude::*;
use std::collections::HashMap;
use std::net::ToSocketAddrs;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::guc::{BATCH_MAX, BATCH_WAIT_MS, TB_ADDR, TB_CLUSTER_ID};
use crate::pack::{pack_account, pack_balance, pack_transfer};
use crate::shmem::{
    CAPACITY, ERR_BUF_LEN, MAX_QUERY_ROWS, OP_CREATE_ACCOUNT, OP_CREATE_TRANSFER,
    OP_GET_ACCOUNT_BALANCES, OP_GET_ACCOUNT_TRANSFERS, OP_LOOKUP_ACCOUNT, OP_LOOKUP_TRANSFER,
    OP_QUERY_ACCOUNTS, OP_QUERY_TRANSFERS, RECORD_LEN, RESULT_BUF_LEN, RING, S_DONE, S_ERROR,
    S_IN_FLIGHT, S_PENDING, WORKER_LATCH,
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
    debit_id: u128,
    credit_id: u128,
    amount: u128,
    ledger: u32,
    code: u16,
    flags: u32,
    limit: u32,
    assigned_id: u128,
}

enum Outcome {
    // Write ops: 16-byte id packed as one record (count = 1).
    // Single-row reads: one RECORD_LEN record (count = 1).
    // Multi-row reads: N records (each RECORD_LEN bytes) concatenated.
    Ok { count: u32, bytes: Vec<u8> },
    Err(String),
}

fn outcome_id(id: u128) -> Outcome {
    let mut bytes = vec![0u8; RECORD_LEN];
    bytes[..16].copy_from_slice(&id.to_le_bytes());
    Outcome::Ok { count: 1, bytes }
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
        error!("beetle: invalid beetle.tb_cluster_id={:?}: {}", cluster_id_raw, e)
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
            let outcomes = run_batch(&rt, &client, &pending);
            let latches = publish_outcomes(pending, outcomes);
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
    let mut out = Vec::new();
    let max_batch = BATCH_MAX.get() as usize;
    let mut guard = RING.exclusive();
    for i in 0..CAPACITY {
        if out.len() >= max_batch {
            break;
        }
        let s = &mut guard.slots[i];
        if s.state == S_PENDING {
            s.state = S_IN_FLIGHT;
            let assigned = if s.op == OP_CREATE_TRANSFER {
                next_transfer_id()
            } else {
                u128::from_le_bytes(s.debit_id)
            };
            out.push(Pending {
                slot_idx: i,
                op: s.op,
                debit_id: u128::from_le_bytes(s.debit_id),
                credit_id: u128::from_le_bytes(s.credit_id),
                amount: s.amount,
                ledger: s.ledger,
                code: s.code,
                flags: s.flags,
                limit: s.limit,
                assigned_id: assigned,
            });
        }
    }
    out
}

fn run_batch(
    rt: &tokio::runtime::Runtime,
    client: &tigerbeetle_unofficial::Client,
    pending: &[Pending],
) -> Vec<Outcome> {
    let mut outcomes: Vec<Outcome> = (0..pending.len())
        .map(|_| Outcome::Err("not processed".into()))
        .collect();

    let lookup_account_idx: Vec<usize> = pending
        .iter()
        .enumerate()
        .filter_map(|(i, p)| (p.op == OP_LOOKUP_ACCOUNT).then_some(i))
        .collect();
    let lookup_transfer_idx: Vec<usize> = pending
        .iter()
        .enumerate()
        .filter_map(|(i, p)| (p.op == OP_LOOKUP_TRANSFER).then_some(i))
        .collect();

    let account_idx: Vec<usize> = pending
        .iter()
        .enumerate()
        .filter_map(|(i, p)| (p.op == OP_CREATE_ACCOUNT).then_some(i))
        .collect();
    let transfer_idx: Vec<usize> = pending
        .iter()
        .enumerate()
        .filter_map(|(i, p)| (p.op == OP_CREATE_TRANSFER).then_some(i))
        .collect();

    if !account_idx.is_empty() {
        let accounts: Vec<_> = account_idx
            .iter()
            .map(|&i| {
                let p = &pending[i];
                tigerbeetle_unofficial::Account::new(p.assigned_id, p.ledger, p.code).with_flags(
                    tigerbeetle_unofficial::account::Flags::from_bits_retain(p.flags as u16),
                )
            })
            .collect();
        let res = rt.block_on(client.create_accounts(accounts));
        apply_batch(
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
                tigerbeetle_unofficial::Transfer::new(p.assigned_id)
                    .with_debit_account_id(p.debit_id)
                    .with_credit_account_id(p.credit_id)
                    .with_amount(p.amount)
                    .with_ledger(p.ledger)
                    .with_code(p.code)
                    .with_flags(tigerbeetle_unofficial::transfer::Flags::from_bits_retain(
                        p.flags as u16,
                    ))
            })
            .collect();
        let res = rt.block_on(client.create_transfers(transfers));
        apply_batch(
            &transfer_idx,
            pending,
            &mut outcomes,
            res,
            format_transfers_err,
        );
    }

    if !lookup_account_idx.is_empty() {
        let ids: Vec<u128> = lookup_account_idx
            .iter()
            .map(|&i| pending[i].debit_id)
            .collect();
        match rt.block_on(client.lookup_accounts(ids.clone())) {
            Ok(found) => {
                let by_id: HashMap<u128, tigerbeetle_unofficial::Account> =
                    found.into_iter().map(|a| (a.id(), a)).collect();
                for &i in &lookup_account_idx {
                    outcomes[i] = match by_id.get(&pending[i].debit_id) {
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
        let ids: Vec<u128> = lookup_transfer_idx
            .iter()
            .map(|&i| pending[i].debit_id)
            .collect();
        match rt.block_on(client.lookup_transfers(ids.clone())) {
            Ok(found) => {
                let by_id: HashMap<u128, tigerbeetle_unofficial::Transfer> =
                    found.into_iter().map(|t| (t.id(), t)).collect();
                for &i in &lookup_transfer_idx {
                    outcomes[i] = match by_id.get(&pending[i].debit_id) {
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
                outcomes[i] = run_account_transfers(rt, client, p);
            }
            OP_GET_ACCOUNT_BALANCES => {
                outcomes[i] = run_account_balances(rt, client, p);
            }
            OP_QUERY_ACCOUNTS => {
                outcomes[i] = run_query_accounts(rt, client, p);
            }
            OP_QUERY_TRANSFERS => {
                outcomes[i] = run_query_transfers(rt, client, p);
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
        tigerbeetle_unofficial::account::Filter::new(p.debit_id, limit)
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
        tigerbeetle_unofficial::account::Filter::new(p.debit_id, limit)
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

fn apply_batch<E, F>(
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
                outcomes[i] = outcome_id(pending[i].assigned_id);
            }
        }
        Err(e) => match format(&e) {
            BatchErr::SendAll(msg) => {
                for &i in indices {
                    outcomes[i] = Outcome::Err(msg.clone());
                }
            }
            BatchErr::PerIndex(errs) => {
                let mut err_map: HashMap<u32, String> = errs.into_iter().collect();
                for (offset, &i) in indices.iter().enumerate() {
                    if let Some(msg) = err_map.remove(&(offset as u32)) {
                        outcomes[i] = Outcome::Err(msg);
                    } else {
                        outcomes[i] = outcome_id(pending[i].assigned_id);
                    }
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

fn publish_outcomes(pending: Vec<Pending>, outcomes: Vec<Outcome>) -> Vec<usize> {
    let mut latches = Vec::with_capacity(pending.len());
    let mut guard = RING.exclusive();
    for (p, outcome) in pending.into_iter().zip(outcomes.into_iter()) {
        let s = &mut guard.slots[p.slot_idx];
        match outcome {
            Outcome::Ok { count, bytes } => {
                let n = bytes.len().min(RESULT_BUF_LEN);
                s.result_buf[..n].copy_from_slice(&bytes[..n]);
                if n < RESULT_BUF_LEN {
                    s.result_buf[n..].fill(0);
                }
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

fn next_transfer_id() -> u128 {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(1);
    let bump = COUNTER.fetch_add(1, Ordering::Relaxed) as u128;
    let id = now.saturating_add(bump);
    if id == 0 {
        1
    } else if id == u128::MAX {
        u128::MAX - 1
    } else {
        id
    }
}
