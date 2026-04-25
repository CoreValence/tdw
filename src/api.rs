#![allow(clippy::type_complexity)]

use pgrx::prelude::*;

use crate::shmem::{
    BATCH_LEG_LEN, MAX_QUERY_ROWS, OP_CREATE_TRANSFER, OP_CREATE_TRANSFER_BATCH, OP_LOOKUP_ACCOUNT,
};
use crate::submit::submit_and_wait;

// TB transfer flags we care about. Mirrors tb_client.h enum.
const TF_LINKED: u16 = 1 << 0;
const TF_PENDING: u16 = 1 << 1;
const TF_POST_PENDING: u16 = 1 << 2;
const TF_VOID_PENDING: u16 = 1 << 3;
const TF_BALANCING_DEBIT: u16 = 1 << 4;
const TF_BALANCING_CREDIT: u16 = 1 << 5;

// Matches TB_ACCOUNT_FLAGS: LINKED(1) | DMNEC(2) | CMNED(4) | HISTORY(8) | IMPORTED(16) | CLOSED(32).
const ACCOUNT_FLAG_BITS: u32 = 0x3f;

// Matches TB_TRANSFER_FLAGS bits 0..=8.
const TRANSFER_FLAG_BITS: u32 = 0x1ff;

// Matches TB_ACCOUNT_FILTER_FLAGS: DEBITS(1) | CREDITS(2) | REVERSED(4).
const ACCOUNT_FILTER_FLAG_BITS: u32 = 0x7;

// Matches TB_QUERY_FILTER_FLAGS: REVERSED(1).
const QUERY_FILTER_FLAG_BITS: u32 = 0x1;

fn flags_from_i32(flags: i32, max_bits: u32) -> u32 {
    if flags < 0 {
        error!("flags must be non-negative");
    }
    let f = flags as u32;
    if f & !max_bits != 0 {
        error!(
            "flags 0x{:x} contains unsupported bits (allowed: 0x{:x})",
            f, max_bits
        );
    }
    f
}

fn check_filter_args(limit: i32, flags: i32, max_flag_bits: u32) -> (u32, u32) {
    if limit < 1 {
        error!("limit must be >= 1");
    }
    if (limit as u32) > MAX_QUERY_ROWS {
        error!(
            "limit {} exceeds MAX_QUERY_ROWS={}; raise the constant to allow more",
            limit, MAX_QUERY_ROWS
        );
    }
    let flag_bits = flags_from_i32(flags, max_flag_bits);
    (limit as u32, flag_bits)
}

fn check_ledger_code(ledger: i32, code: i32) {
    if ledger < 0 {
        error!("ledger must be non-negative");
    }
    if !(0..=u16::MAX as i32).contains(&code) {
        error!("code must fit in u16");
    }
}

fn check_transfer_args(amount: i64, ledger: i32, code: i32) {
    if amount < 0 {
        error!("amount must be non-negative");
    }
    check_ledger_code(ledger, code);
}

#[allow(clippy::too_many_arguments)]
fn submit_create_transfer(
    id: [u8; 16],
    debit_id: [u8; 16],
    credit_id: [u8; 16],
    pending_id: [u8; 16],
    amount: u128,
    ledger: u32,
    code: u16,
    flags: u32,
) -> pgrx::Uuid {
    match submit_and_wait(|slot| {
        slot.op = OP_CREATE_TRANSFER;
        slot.id = id;
        slot.debit_id = debit_id;
        slot.credit_id = credit_id;
        slot.pending_id = pending_id;
        slot.amount = amount;
        slot.ledger = ledger;
        slot.code = code;
        slot.flags = flags;
    }) {
        Ok(rb) => {
            let rec = rb.record(0).expect("create_transfer returns one record");
            pgrx::Uuid::from_bytes(rec[..16].try_into().unwrap())
        }
        Err(msg) => error!("tdw: {}", msg),
    }
}

fn parse_leg(
    leg: &serde_json::Value,
    i: usize,
) -> (
    uuid::Uuid,
    uuid::Uuid,
    uuid::Uuid,
    uuid::Uuid,
    u128,
    u32,
    u16,
    u16,
) {
    let id = parse_uuid_field(leg, "id", i);
    let debit = parse_uuid_field(leg, "debit", i);
    let credit = parse_uuid_field(leg, "credit", i);
    let pending = leg
        .get("pending_id")
        .and_then(|v| v.as_str())
        .map(|s| match uuid::Uuid::parse_str(s) {
            Ok(u) => u,
            Err(e) => error!("legs[{i}].pending_id: {e}"),
        })
        .unwrap_or_else(uuid::Uuid::nil);
    let amount = match leg.get("amount").and_then(|v| v.as_i64()) {
        Some(a) if a >= 0 => a as u128,
        _ => error!("legs[{i}].amount must be a non-negative integer"),
    };
    let ledger = match leg.get("ledger").and_then(|v| v.as_i64()) {
        Some(l) if l >= 0 => l as u32,
        _ => error!("legs[{i}].ledger must be a non-negative integer"),
    };
    let code = match leg.get("code").and_then(|v| v.as_i64()) {
        Some(c) if (0..=u16::MAX as i64).contains(&c) => c as u16,
        _ => error!("legs[{i}].code must be a u16"),
    };
    let flags = leg
        .get("flags")
        .and_then(|v| v.as_i64())
        .map(|f| {
            if !(0..=u16::MAX as i64).contains(&f) {
                error!("legs[{i}].flags must be a u16");
            }
            f as u16
        })
        .unwrap_or(0);
    (id, debit, credit, pending, amount, ledger, code, flags)
}

fn parse_uuid_field(obj: &serde_json::Value, key: &str, i: usize) -> uuid::Uuid {
    match obj.get(key).and_then(|v| v.as_str()) {
        Some(s) => match uuid::Uuid::parse_str(s) {
            Ok(u) => u,
            Err(e) => error!("legs[{i}].{key}: {e}"),
        },
        None => error!("legs[{i}].{key} is required"),
    }
}

#[allow(clippy::too_many_arguments)]
fn write_leg(
    buf: &mut [u8],
    id: uuid::Uuid,
    debit: uuid::Uuid,
    credit: uuid::Uuid,
    pending: uuid::Uuid,
    amount: u128,
    ledger: u32,
    code: u16,
    flags: u16,
) {
    buf[0..16].copy_from_slice(id.as_bytes());
    buf[16..32].copy_from_slice(debit.as_bytes());
    buf[32..48].copy_from_slice(credit.as_bytes());
    buf[48..64].copy_from_slice(pending.as_bytes());
    buf[64..80].copy_from_slice(&amount.to_le_bytes());
    buf[80..84].copy_from_slice(&ledger.to_le_bytes());
    buf[84..86].copy_from_slice(&code.to_le_bytes());
    buf[86..88].copy_from_slice(&flags.to_le_bytes());
    // [88..96] padding, left zero
}

fn submit_batch(packed: &[u8], count: u32) -> Vec<pgrx::Uuid> {
    match crate::submit::submit_and_wait_with_legs(
        |slot| {
            slot.op = OP_CREATE_TRANSFER_BATCH;
            slot.limit = count;
        },
        // Legs are packed into the slot's pool entry on the way in; the
        // worker reads them at drain time and overwrites the entry with ids
        // on the way out.
        |buf| {
            buf[..packed.len()].copy_from_slice(packed);
        },
    ) {
        Ok(rb) => (0..rb.count as usize)
            .map(|i| {
                let rec = rb.record(i).expect("batch id record");
                pgrx::Uuid::from_bytes(rec[..16].try_into().unwrap())
            })
            .collect(),
        Err(msg) => error!("tdw: {}", msg),
    }
}

// Look up an account and return its debit-available balance
//   credits_posted - debits_posted - debits_pending
// saturating at 0. Errors if the account doesn't exist.
fn drain_balance(account: uuid::Uuid) -> u128 {
    let rb = match submit_and_wait(|slot| {
        slot.op = OP_LOOKUP_ACCOUNT;
        slot.id = *account.as_bytes();
    }) {
        Ok(rb) => rb,
        Err(msg) if msg == "not found" => {
            error!("waterfall: source {account} not found");
        }
        Err(msg) => error!("tdw: {}", msg),
    };
    let rec = match rb.record(0) {
        Some(r) => r,
        None => error!("waterfall: source {account} not found"),
    };
    let debits_posted = u128::from_le_bytes(rec[16..32].try_into().unwrap());
    let credits_posted = u128::from_le_bytes(rec[32..48].try_into().unwrap());
    let debits_pending = u128::from_le_bytes(rec[48..64].try_into().unwrap());
    credits_posted
        .saturating_sub(debits_posted)
        .saturating_sub(debits_pending)
}

// Single-source → many-destinations batch with fresh leg ids and auto-LINKED.
fn pack_and_submit_legs(
    source: uuid::Uuid,
    legs: &[(uuid::Uuid, u128)],
    ledger: u32,
    code: u16,
) -> Vec<pgrx::Uuid> {
    let mut packed = vec![0u8; legs.len() * BATCH_LEG_LEN];
    let last = legs.len() - 1;
    for (i, (to, amount)) in legs.iter().enumerate() {
        let flags = if i < last { TF_LINKED } else { 0 };
        write_leg(
            &mut packed[i * BATCH_LEG_LEN..(i + 1) * BATCH_LEG_LEN],
            uuid::Uuid::new_v4(),
            source,
            *to,
            uuid::Uuid::nil(),
            *amount,
            ledger,
            code,
            flags,
        );
    }
    submit_batch(&packed, legs.len() as u32)
}

// Many-sources → single-destination variant. Source varies per leg, destination
// is fixed.
fn pack_and_submit_legs_with_fixed_credit(
    destination: uuid::Uuid,
    legs: &[(uuid::Uuid, u128)],
    ledger: u32,
    code: u16,
) -> Vec<pgrx::Uuid> {
    let mut packed = vec![0u8; legs.len() * BATCH_LEG_LEN];
    let last = legs.len() - 1;
    for (i, (from, amount)) in legs.iter().enumerate() {
        let flags = if i < last { TF_LINKED } else { 0 };
        write_leg(
            &mut packed[i * BATCH_LEG_LEN..(i + 1) * BATCH_LEG_LEN],
            uuid::Uuid::new_v4(),
            *from,
            destination,
            uuid::Uuid::nil(),
            *amount,
            ledger,
            code,
            flags,
        );
    }
    submit_batch(&packed, legs.len() as u32)
}

#[pg_schema]
mod accounts {
    use pgrx::prelude::*;

    use crate::pack::{
        AccountRow, BalanceRow, TransferRow, unpack_account_row, unpack_balance_row,
        unpack_transfer_row,
    };
    use crate::shmem::{
        OP_CREATE_ACCOUNT, OP_GET_ACCOUNT_BALANCES, OP_GET_ACCOUNT_TRANSFERS, OP_LOOKUP_ACCOUNT,
        OP_QUERY_ACCOUNTS,
    };
    use crate::submit::submit_and_wait;

    use super::{
        ACCOUNT_FILTER_FLAG_BITS, ACCOUNT_FLAG_BITS, QUERY_FILTER_FLAG_BITS, check_filter_args,
        check_ledger_code, flags_from_i32,
    };

    #[pg_extern]
    fn open(id: pgrx::Uuid, ledger: i32, code: i32, flags: pgrx::default!(i32, 0)) -> pgrx::Uuid {
        check_ledger_code(ledger, code);
        let flag_bits = flags_from_i32(flags, ACCOUNT_FLAG_BITS);

        let id_bytes = *id.as_bytes();
        match submit_and_wait(|slot| {
            slot.op = OP_CREATE_ACCOUNT;
            slot.id = id_bytes;
            slot.ledger = ledger as u32;
            slot.code = code as u16;
            slot.flags = flag_bits;
        }) {
            Ok(rb) => {
                let rec = rb.record(0).expect("accounts.open returns one record");
                pgrx::Uuid::from_bytes(rec[..16].try_into().unwrap())
            }
            Err(msg) => error!("tdw: {}", msg),
        }
    }

    #[pg_extern(name = "get")]
    fn account_get(
        id: pgrx::Uuid,
    ) -> pgrx::iter::TableIterator<
        'static,
        (
            pgrx::name!(id, pgrx::Uuid),
            pgrx::name!(ledger, i32),
            pgrx::name!(code, i32),
            pgrx::name!(debits_posted, pgrx::AnyNumeric),
            pgrx::name!(credits_posted, pgrx::AnyNumeric),
            pgrx::name!(debits_pending, pgrx::AnyNumeric),
            pgrx::name!(credits_pending, pgrx::AnyNumeric),
            pgrx::name!(flags, i32),
            pgrx::name!(timestamp, i64),
        ),
    > {
        let id_bytes = *id.as_bytes();
        match submit_and_wait(|slot| {
            slot.op = OP_LOOKUP_ACCOUNT;
            slot.id = id_bytes;
        }) {
            Ok(rb) => match rb.record(0) {
                Some(rec) => pgrx::iter::TableIterator::once(unpack_account_row(rec)),
                None => pgrx::iter::TableIterator::empty(),
            },
            Err(msg) if msg == "not found" => pgrx::iter::TableIterator::empty(),
            Err(msg) => error!("tdw: {}", msg),
        }
    }

    #[pg_extern]
    fn ledger(
        account_id: pgrx::Uuid,
        limit: pgrx::default!(i32, 10),
        flags: pgrx::default!(i32, 3),
    ) -> pgrx::iter::TableIterator<
        'static,
        (
            pgrx::name!(id, pgrx::Uuid),
            pgrx::name!(debit, pgrx::Uuid),
            pgrx::name!(credit, pgrx::Uuid),
            pgrx::name!(amount, pgrx::AnyNumeric),
            pgrx::name!(ledger, i32),
            pgrx::name!(code, i32),
            pgrx::name!(flags, i32),
            pgrx::name!(timestamp, i64),
        ),
    > {
        let (lim, flag_bits) = check_filter_args(limit, flags, ACCOUNT_FILTER_FLAG_BITS);
        if flag_bits & 0x3 == 0 {
            error!("accounts.ledger flags must include DEBITS(1) and/or CREDITS(2)");
        }
        let id_bytes = *account_id.as_bytes();
        let rb = match submit_and_wait(|slot| {
            slot.op = OP_GET_ACCOUNT_TRANSFERS;
            slot.id = id_bytes;
            slot.limit = lim;
            slot.flags = flag_bits;
        }) {
            Ok(rb) => rb,
            Err(msg) => error!("tdw: {}", msg),
        };
        let rows: Vec<TransferRow> = (0..rb.count as usize)
            .filter_map(|i| rb.record(i).map(unpack_transfer_row))
            .collect();
        pgrx::iter::TableIterator::new(rows)
    }

    #[pg_extern]
    fn history(
        account_id: pgrx::Uuid,
        limit: pgrx::default!(i32, 10),
        flags: pgrx::default!(i32, 3),
    ) -> pgrx::iter::TableIterator<
        'static,
        (
            pgrx::name!(debits_posted, pgrx::AnyNumeric),
            pgrx::name!(credits_posted, pgrx::AnyNumeric),
            pgrx::name!(debits_pending, pgrx::AnyNumeric),
            pgrx::name!(credits_pending, pgrx::AnyNumeric),
            pgrx::name!(timestamp, i64),
        ),
    > {
        let (lim, flag_bits) = check_filter_args(limit, flags, ACCOUNT_FILTER_FLAG_BITS);
        if flag_bits & 0x3 == 0 {
            error!("accounts.history flags must include DEBITS(1) and/or CREDITS(2)");
        }
        let id_bytes = *account_id.as_bytes();
        let rb = match submit_and_wait(|slot| {
            slot.op = OP_GET_ACCOUNT_BALANCES;
            slot.id = id_bytes;
            slot.limit = lim;
            slot.flags = flag_bits;
        }) {
            Ok(rb) => rb,
            Err(msg) => error!("tdw: {}", msg),
        };
        let rows: Vec<BalanceRow> = (0..rb.count as usize)
            .filter_map(|i| rb.record(i).map(unpack_balance_row))
            .collect();
        pgrx::iter::TableIterator::new(rows)
    }

    #[pg_extern(name = "search")]
    fn account_search(
        ledger: pgrx::default!(i32, 0),
        code: pgrx::default!(i32, 0),
        limit: pgrx::default!(i32, 10),
        flags: pgrx::default!(i32, 0),
    ) -> pgrx::iter::TableIterator<
        'static,
        (
            pgrx::name!(id, pgrx::Uuid),
            pgrx::name!(ledger, i32),
            pgrx::name!(code, i32),
            pgrx::name!(debits_posted, pgrx::AnyNumeric),
            pgrx::name!(credits_posted, pgrx::AnyNumeric),
            pgrx::name!(debits_pending, pgrx::AnyNumeric),
            pgrx::name!(credits_pending, pgrx::AnyNumeric),
            pgrx::name!(flags, i32),
            pgrx::name!(timestamp, i64),
        ),
    > {
        check_ledger_code(ledger, code);
        let (lim, flag_bits) = check_filter_args(limit, flags, QUERY_FILTER_FLAG_BITS);
        let rb = match submit_and_wait(|slot| {
            slot.op = OP_QUERY_ACCOUNTS;
            slot.ledger = ledger as u32;
            slot.code = code as u16;
            slot.limit = lim;
            slot.flags = flag_bits;
        }) {
            Ok(rb) => rb,
            Err(msg) => error!("tdw: {}", msg),
        };
        let rows: Vec<AccountRow> = (0..rb.count as usize)
            .filter_map(|i| rb.record(i).map(unpack_account_row))
            .collect();
        pgrx::iter::TableIterator::new(rows)
    }
}

#[pg_schema]
mod transfers {
    use pgrx::prelude::*;

    use crate::pack::{TransferRow, unpack_transfer_row};
    use crate::shmem::{BATCH_LEG_LEN, MAX_BATCH_LEGS, OP_LOOKUP_TRANSFER, OP_QUERY_TRANSFERS};
    use crate::submit::submit_and_wait;

    use super::{
        QUERY_FILTER_FLAG_BITS, TF_BALANCING_CREDIT, TF_BALANCING_DEBIT, TF_LINKED, TF_PENDING,
        TF_POST_PENDING, TF_VOID_PENDING, TRANSFER_FLAG_BITS, check_filter_args, check_ledger_code,
        check_transfer_args, drain_balance, flags_from_i32, pack_and_submit_legs,
        pack_and_submit_legs_with_fixed_credit, parse_leg, parse_uuid_field, submit_batch,
        submit_create_transfer, write_leg,
    };

    #[pg_extern]
    fn post(
        id: pgrx::Uuid,
        debit: pgrx::Uuid,
        credit: pgrx::Uuid,
        amount: i64,
        ledger: i32,
        code: i32,
        flags: pgrx::default!(i32, 0),
    ) -> pgrx::Uuid {
        check_transfer_args(amount, ledger, code);
        let flag_bits = flags_from_i32(flags, TRANSFER_FLAG_BITS);
        submit_create_transfer(
            *id.as_bytes(),
            *debit.as_bytes(),
            *credit.as_bytes(),
            [0u8; 16],
            amount as u128,
            ledger as u32,
            code as u16,
            flag_bits,
        )
    }

    #[pg_extern]
    fn hold(
        id: pgrx::Uuid,
        debit: pgrx::Uuid,
        credit: pgrx::Uuid,
        amount: i64,
        ledger: i32,
        code: i32,
    ) -> pgrx::Uuid {
        check_transfer_args(amount, ledger, code);
        submit_create_transfer(
            *id.as_bytes(),
            *debit.as_bytes(),
            *credit.as_bytes(),
            [0u8; 16],
            amount as u128,
            ledger as u32,
            code as u16,
            TF_PENDING as u32,
        )
    }

    #[pg_extern]
    fn capture(
        id: pgrx::Uuid,
        pending_id: pgrx::Uuid,
        amount: pgrx::default!(Option<i64>, "NULL"),
    ) -> pgrx::Uuid {
        // TB: POST_PENDING with amount=0 posts the full pending amount. A smaller
        // non-zero amount posts a partial and auto-voids the remainder. Leave
        // ledger/code/debit/credit zero — TB inherits them from the pending row.
        let amt = match amount {
            Some(a) if a < 0 => error!("amount must be non-negative"),
            Some(a) => a as u128,
            None => 0u128,
        };
        submit_create_transfer(
            *id.as_bytes(),
            [0u8; 16],
            [0u8; 16],
            *pending_id.as_bytes(),
            amt,
            0,
            0,
            TF_POST_PENDING as u32,
        )
    }

    #[pg_extern]
    fn release(id: pgrx::Uuid, pending_id: pgrx::Uuid) -> pgrx::Uuid {
        submit_create_transfer(
            *id.as_bytes(),
            [0u8; 16],
            [0u8; 16],
            *pending_id.as_bytes(),
            0,
            0,
            0,
            TF_VOID_PENDING as u32,
        )
    }

    #[pg_extern]
    fn sweep_from(
        id: pgrx::Uuid,
        debit: pgrx::Uuid,
        credit: pgrx::Uuid,
        amount: i64,
        ledger: i32,
        code: i32,
    ) -> pgrx::Uuid {
        check_transfer_args(amount, ledger, code);
        submit_create_transfer(
            *id.as_bytes(),
            *debit.as_bytes(),
            *credit.as_bytes(),
            [0u8; 16],
            amount as u128,
            ledger as u32,
            code as u16,
            TF_BALANCING_DEBIT as u32,
        )
    }

    #[pg_extern]
    fn sweep_to(
        id: pgrx::Uuid,
        debit: pgrx::Uuid,
        credit: pgrx::Uuid,
        amount: i64,
        ledger: i32,
        code: i32,
    ) -> pgrx::Uuid {
        check_transfer_args(amount, ledger, code);
        submit_create_transfer(
            *id.as_bytes(),
            *debit.as_bytes(),
            *credit.as_bytes(),
            [0u8; 16],
            amount as u128,
            ledger as u32,
            code as u16,
            TF_BALANCING_CREDIT as u32,
        )
    }

    // Shape each leg: jsonb object with keys id, debit, credit, amount, ledger, code.
    // Optional: pending_id, flags.
    // The last leg's LINKED bit is forced off; every prior leg has LINKED forced on
    // so TB treats the batch as one atomic chain.
    #[pg_extern]
    fn journal(legs: pgrx::JsonB) -> Vec<pgrx::Uuid> {
        let arr = match legs.0.as_array() {
            Some(a) => a,
            None => error!("transfers.journal: legs must be a JSON array"),
        };
        if arr.is_empty() {
            error!("transfers.journal: legs array is empty");
        }
        if arr.len() > MAX_BATCH_LEGS {
            error!(
                "transfers.journal: {} legs exceeds MAX_BATCH_LEGS={}",
                arr.len(),
                MAX_BATCH_LEGS
            );
        }
        let mut packed = vec![0u8; arr.len() * BATCH_LEG_LEN];
        let last = arr.len() - 1;
        for (i, leg) in arr.iter().enumerate() {
            let (id, debit, credit, pending, amount, ledger, code, mut flags) = parse_leg(leg, i);
            if i < last {
                flags |= TF_LINKED;
            } else {
                flags &= !TF_LINKED;
            }
            write_leg(
                &mut packed[i * BATCH_LEG_LEN..(i + 1) * BATCH_LEG_LEN],
                id,
                debit,
                credit,
                pending,
                amount,
                ledger,
                code,
                flags,
            );
        }
        submit_batch(&packed, arr.len() as u32)
    }

    // destinations: JSON array of {"to": uuid-string, "pct": number}. Pct values
    // must sum to exactly 100. Amounts are floor(total * pct / 100); the rounding
    // remainder goes to the account at `remainder_to`.
    #[pg_extern]
    fn split(
        source: pgrx::Uuid,
        destinations: pgrx::JsonB,
        total: i64,
        ledger: i32,
        code: i32,
        remainder_to: pgrx::Uuid,
    ) -> Vec<pgrx::Uuid> {
        check_transfer_args(total, ledger, code);
        let arr = match destinations.0.as_array() {
            Some(a) => a,
            None => error!("transfers.split: destinations must be a JSON array"),
        };
        if arr.is_empty() {
            error!("transfers.split: destinations is empty");
        }
        if arr.len() > MAX_BATCH_LEGS {
            error!(
                "transfers.split: {} destinations exceeds MAX_BATCH_LEGS={}",
                arr.len(),
                MAX_BATCH_LEGS
            );
        }

        // Compute leg amounts with floor division; the dust leg catches the
        // difference so the total is conserved exactly.
        let mut computed: Vec<(uuid::Uuid, u128)> = Vec::with_capacity(arr.len());
        let mut pct_sum: i64 = 0;
        let mut amount_sum: u128 = 0;
        for (i, d) in arr.iter().enumerate() {
            let to = parse_uuid_field(d, "to", i);
            let pct = match d.get("pct").and_then(|v| v.as_i64()) {
                Some(p) if (0..=100).contains(&p) => p,
                _ => error!("transfers.split: destinations[{i}].pct must be int in 0..=100"),
            };
            pct_sum += pct;
            let leg_amount = (total as u128) * (pct as u128) / 100u128;
            amount_sum += leg_amount;
            computed.push((to, leg_amount));
        }
        if pct_sum != 100 {
            error!("transfers.split: pct values sum to {pct_sum}, expected 100");
        }

        let source_u = uuid::Uuid::from_bytes(*source.as_bytes());
        let remainder_u = uuid::Uuid::from_bytes(*remainder_to.as_bytes());
        let dust = (total as u128).saturating_sub(amount_sum);
        // One leg per destination, plus a separate dust leg if rounding lost anything
        // AND the dust account isn't already one of the destinations (in which case
        // we add the dust to that destination's leg to save a TB transfer).
        if dust > 0 {
            if let Some(existing) = computed.iter_mut().find(|(to, _)| *to == remainder_u) {
                existing.1 += dust;
            } else {
                computed.push((remainder_u, dust));
            }
        }

        let last = computed.len() - 1;
        let mut packed = vec![0u8; computed.len() * BATCH_LEG_LEN];
        for (i, (to, amount)) in computed.iter().enumerate() {
            let leg_id = uuid::Uuid::new_v4();
            let flags = if i < last { TF_LINKED } else { 0 };
            write_leg(
                &mut packed[i * BATCH_LEG_LEN..(i + 1) * BATCH_LEG_LEN],
                leg_id,
                source_u,
                *to,
                uuid::Uuid::nil(),
                *amount,
                ledger as u32,
                code as u16,
                flags,
            );
        }
        submit_batch(&packed, computed.len() as u32)
    }

    // Fixed/percent/remainder destination spec. Each jsonb entry is exactly one of:
    //   {"to": uuid, "amount":    bigint}   fixed amount leg
    //   {"to": uuid, "pct":       int}      percent-of-total leg (floor division)
    //   {"to": uuid, "remainder": true}     catches total - (fixed + pct sums) + dust
    //
    // At most one remainder entry. If none and the fixed+pct amounts don't equal
    // `total`, we error rather than silently under/over-moving.
    #[pg_extern]
    fn allocate(
        source: pgrx::Uuid,
        destinations: pgrx::JsonB,
        total: i64,
        ledger: i32,
        code: i32,
    ) -> Vec<pgrx::Uuid> {
        check_transfer_args(total, ledger, code);
        let arr = match destinations.0.as_array() {
            Some(a) => a,
            None => error!("transfers.allocate: destinations must be a JSON array"),
        };
        if arr.is_empty() {
            error!("transfers.allocate: destinations is empty");
        }

        let mut planned: Vec<(uuid::Uuid, u128)> = Vec::with_capacity(arr.len());
        let mut remainder_to: Option<uuid::Uuid> = None;
        let mut claimed: u128 = 0;
        let total_u = total as u128;
        for (i, d) in arr.iter().enumerate() {
            let to = parse_uuid_field(d, "to", i);
            let has_amount = d.get("amount").is_some();
            let has_pct = d.get("pct").is_some();
            let has_remainder = d
                .get("remainder")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            let selectors = [has_amount, has_pct, has_remainder]
                .iter()
                .filter(|b| **b)
                .count();
            if selectors != 1 {
                error!(
                    "transfers.allocate: destinations[{i}] must have exactly one of 'amount', 'pct', or 'remainder':true"
                );
            }
            if has_amount {
                let a = match d.get("amount").and_then(|v| v.as_i64()) {
                    Some(a) if a >= 0 => a as u128,
                    _ => error!(
                        "transfers.allocate: destinations[{i}].amount must be a non-negative integer"
                    ),
                };
                planned.push((to, a));
                claimed = claimed.saturating_add(a);
            } else if has_pct {
                let p = match d.get("pct").and_then(|v| v.as_i64()) {
                    Some(p) if (0..=100).contains(&p) => p as u128,
                    _ => error!("transfers.allocate: destinations[{i}].pct must be int in 0..=100"),
                };
                let a = total_u * p / 100u128;
                planned.push((to, a));
                claimed = claimed.saturating_add(a);
            } else {
                // remainder
                if remainder_to.is_some() {
                    error!("transfers.allocate: at most one remainder destination allowed");
                }
                remainder_to = Some(to);
            }
        }
        if claimed > total_u {
            error!(
                "transfers.allocate: fixed + pct amounts sum to {claimed}, which exceeds total={total}"
            );
        }
        let leftover = total_u - claimed;
        match (remainder_to, leftover) {
            (Some(to), l) => planned.push((to, l)),
            (None, 0) => {}
            (None, l) => error!(
                "transfers.allocate: destinations underspend total by {l}; add a remainder entry or matching amount"
            ),
        }

        if planned.len() > MAX_BATCH_LEGS {
            error!(
                "transfers.allocate: {} legs exceeds MAX_BATCH_LEGS={}",
                planned.len(),
                MAX_BATCH_LEGS
            );
        }

        let source_u = uuid::Uuid::from_bytes(*source.as_bytes());
        pack_and_submit_legs(source_u, &planned, ledger as u32, code as u16)
    }

    // Ordered fallback sources. Each entry is:
    //   {"from": uuid, "max": bigint | null}
    //
    // `max = bigint` caps this source's contribution to `max`. `max = null` drains
    // this source — we look up its current available balance (credits_posted -
    // debits_posted - debits_pending, saturating at 0) and use that as the cap.
    //
    // Walk sources in order, each contributes min(remaining, cap), until the total
    // is filled. Errors if combined caps can't meet the total. Zero-contribution
    // sources are skipped (no empty leg submitted to TB).
    #[pg_extern]
    fn waterfall(
        destination: pgrx::Uuid,
        sources: pgrx::JsonB,
        total: i64,
        ledger: i32,
        code: i32,
    ) -> Vec<pgrx::Uuid> {
        check_transfer_args(total, ledger, code);
        let arr = match sources.0.as_array() {
            Some(a) => a,
            None => error!("transfers.waterfall: sources must be a JSON array"),
        };
        if arr.is_empty() {
            error!("transfers.waterfall: sources is empty");
        }

        let mut planned: Vec<(uuid::Uuid, u128)> = Vec::with_capacity(arr.len());
        let mut remaining: u128 = total as u128;
        for (i, s) in arr.iter().enumerate() {
            if remaining == 0 {
                break;
            }
            let from = parse_uuid_field(s, "from", i);
            let max_v = s.get("max");
            let cap: u128 = match max_v {
                Some(v) if v.is_null() => drain_balance(from),
                Some(v) => match v.as_i64() {
                    Some(m) if m >= 0 => m as u128,
                    _ => error!(
                        "transfers.waterfall: sources[{i}].max must be non-negative integer or null"
                    ),
                },
                None => {
                    error!("transfers.waterfall: sources[{i}].max is required (use null to drain)")
                }
            };
            let take = cap.min(remaining);
            if take > 0 {
                planned.push((from, take));
                remaining -= take;
            }
        }
        if remaining > 0 {
            error!(
                "transfers.waterfall: sources cover {} of {total}; short by {remaining}",
                total as u128 - remaining
            );
        }
        if planned.len() > MAX_BATCH_LEGS {
            error!(
                "transfers.waterfall: {} legs exceeds MAX_BATCH_LEGS={}",
                planned.len(),
                MAX_BATCH_LEGS
            );
        }

        let dest_u = uuid::Uuid::from_bytes(*destination.as_bytes());
        // sources are debits, so `debit` varies, `credit` is fixed.
        pack_and_submit_legs_with_fixed_credit(dest_u, &planned, ledger as u32, code as u16)
    }

    #[pg_extern(name = "get")]
    fn transfer_get(
        id: pgrx::Uuid,
    ) -> pgrx::iter::TableIterator<
        'static,
        (
            pgrx::name!(id, pgrx::Uuid),
            pgrx::name!(debit, pgrx::Uuid),
            pgrx::name!(credit, pgrx::Uuid),
            pgrx::name!(amount, pgrx::AnyNumeric),
            pgrx::name!(ledger, i32),
            pgrx::name!(code, i32),
            pgrx::name!(flags, i32),
            pgrx::name!(timestamp, i64),
        ),
    > {
        let id_bytes = *id.as_bytes();
        match submit_and_wait(|slot| {
            slot.op = OP_LOOKUP_TRANSFER;
            slot.id = id_bytes;
        }) {
            Ok(rb) => match rb.record(0) {
                Some(rec) => pgrx::iter::TableIterator::once(unpack_transfer_row(rec)),
                None => pgrx::iter::TableIterator::empty(),
            },
            Err(msg) if msg == "not found" => pgrx::iter::TableIterator::empty(),
            Err(msg) => error!("tdw: {}", msg),
        }
    }

    #[pg_extern(name = "search")]
    fn transfer_search(
        ledger: pgrx::default!(i32, 0),
        code: pgrx::default!(i32, 0),
        limit: pgrx::default!(i32, 10),
        flags: pgrx::default!(i32, 0),
    ) -> pgrx::iter::TableIterator<
        'static,
        (
            pgrx::name!(id, pgrx::Uuid),
            pgrx::name!(debit, pgrx::Uuid),
            pgrx::name!(credit, pgrx::Uuid),
            pgrx::name!(amount, pgrx::AnyNumeric),
            pgrx::name!(ledger, i32),
            pgrx::name!(code, i32),
            pgrx::name!(flags, i32),
            pgrx::name!(timestamp, i64),
        ),
    > {
        check_ledger_code(ledger, code);
        let (lim, flag_bits) = check_filter_args(limit, flags, QUERY_FILTER_FLAG_BITS);
        let rb = match submit_and_wait(|slot| {
            slot.op = OP_QUERY_TRANSFERS;
            slot.ledger = ledger as u32;
            slot.code = code as u16;
            slot.limit = lim;
            slot.flags = flag_bits;
        }) {
            Ok(rb) => rb,
            Err(msg) => error!("tdw: {}", msg),
        };
        let rows: Vec<TransferRow> = (0..rb.count as usize)
            .filter_map(|i| rb.record(i).map(unpack_transfer_row))
            .collect();
        pgrx::iter::TableIterator::new(rows)
    }

    // Post an opposite-direction transfer against the original. The reversal
    // has a fresh client-provided id (for its own idempotency) and references
    // the original's id via... nothing explicit yet — TB has no "prior" pointer
    // on a transfer, so the relationship is implicit: same amount + swapped
    // debit/credit + same ledger/code, both queryable in history. Rejects
    // pending / post-pending / void-pending originals (those have their own
    // lifecycle verbs; use transfers.release / transfers.capture instead).
    #[pg_extern]
    fn reverse(id: pgrx::Uuid, original_id: pgrx::Uuid) -> pgrx::Uuid {
        let rb = match submit_and_wait(|slot| {
            slot.op = OP_LOOKUP_TRANSFER;
            slot.id = *original_id.as_bytes();
        }) {
            Ok(rb) => rb,
            Err(msg) if msg == "not found" => {
                error!("transfers.reverse: original {original_id} not found")
            }
            Err(msg) => error!("tdw: {}", msg),
        };
        let rec = match rb.record(0) {
            Some(r) => r,
            None => error!("transfers.reverse: original {original_id} not found"),
        };
        // Transfer record layout (pack::pack_transfer):
        //   [16..32] debit, [32..48] credit, [48..64] amount (u128 LE),
        //   [64..68] ledger (u32), [68..70] code (u16), [70..72] flags (u16)
        let flags = u16::from_le_bytes(rec[70..72].try_into().unwrap());
        if flags & (TF_PENDING | TF_POST_PENDING | TF_VOID_PENDING) != 0 {
            error!(
                "transfers.reverse: {original_id} is a two-phase transfer (flags=0x{flags:x}); use transfers.release or transfers.capture instead"
            );
        }
        let debit: [u8; 16] = rec[16..32].try_into().unwrap();
        let credit: [u8; 16] = rec[32..48].try_into().unwrap();
        let amount = u128::from_le_bytes(rec[48..64].try_into().unwrap());
        let ledger = u32::from_le_bytes(rec[64..68].try_into().unwrap());
        let code = u16::from_le_bytes(rec[68..70].try_into().unwrap());

        submit_create_transfer(
            *id.as_bytes(),
            credit,
            debit,
            [0u8; 16],
            amount,
            ledger,
            code,
            0,
        )
    }
}
