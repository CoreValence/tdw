#![allow(clippy::type_complexity)]

use pgrx::prelude::*;

use crate::pack::{
    AccountRow, BalanceRow, TransferRow, unpack_account_row, unpack_balance_row,
    unpack_transfer_row,
};
use crate::shmem::{
    MAX_QUERY_ROWS, OP_CREATE_ACCOUNT, OP_CREATE_TRANSFER, OP_GET_ACCOUNT_BALANCES,
    OP_GET_ACCOUNT_TRANSFERS, OP_LOOKUP_ACCOUNT, OP_LOOKUP_TRANSFER, OP_QUERY_ACCOUNTS,
    OP_QUERY_TRANSFERS,
};
use crate::submit::submit_and_wait;

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

// Matches TB_ACCOUNT_FLAGS: LINKED(1) | DMNEC(2) | CMNED(4) | HISTORY(8) | IMPORTED(16) | CLOSED(32).
const ACCOUNT_FLAG_BITS: u32 = 0x3f;

// Matches TB_TRANSFER_FLAGS bits 0..=8.
const TRANSFER_FLAG_BITS: u32 = 0x1ff;

// Matches TB_ACCOUNT_FILTER_FLAGS: DEBITS(1) | CREDITS(2) | REVERSED(4).
const ACCOUNT_FILTER_FLAG_BITS: u32 = 0x7;

// Matches TB_QUERY_FILTER_FLAGS: REVERSED(1).
const QUERY_FILTER_FLAG_BITS: u32 = 0x1;

#[pg_extern]
fn post_account(
    id: pgrx::Uuid,
    ledger: i32,
    code: i32,
    flags: pgrx::default!(i32, 0),
) -> pgrx::Uuid {
    if ledger < 0 {
        error!("ledger must be non-negative");
    }
    if !(0..=u16::MAX as i32).contains(&code) {
        error!("code must fit in u16");
    }
    let flag_bits = flags_from_i32(flags, ACCOUNT_FLAG_BITS);

    let id_bytes = *id.as_bytes();
    match submit_and_wait(|slot| {
        slot.op = OP_CREATE_ACCOUNT;
        slot.debit_id = id_bytes;
        slot.ledger = ledger as u32;
        slot.code = code as u16;
        slot.flags = flag_bits;
    }) {
        Ok(rb) => {
            let rec = rb.record(0).expect("post_account returns one record");
            pgrx::Uuid::from_bytes(rec[..16].try_into().unwrap())
        }
        Err(msg) => error!("beetle: {}", msg),
    }
}

#[pg_extern]
fn post_transfer(
    debit: pgrx::Uuid,
    credit: pgrx::Uuid,
    amount: i64,
    ledger: i32,
    code: i32,
    flags: pgrx::default!(i32, 0),
) -> pgrx::Uuid {
    if amount < 0 {
        error!("amount must be non-negative");
    }
    if ledger < 0 {
        error!("ledger must be non-negative");
    }
    if !(0..=u16::MAX as i32).contains(&code) {
        error!("code must fit in u16");
    }
    let flag_bits = flags_from_i32(flags, TRANSFER_FLAG_BITS);

    let debit_bytes = *debit.as_bytes();
    let credit_bytes = *credit.as_bytes();
    match submit_and_wait(|slot| {
        slot.op = OP_CREATE_TRANSFER;
        slot.debit_id = debit_bytes;
        slot.credit_id = credit_bytes;
        slot.amount = amount as u128;
        slot.ledger = ledger as u32;
        slot.code = code as u16;
        slot.flags = flag_bits;
    }) {
        Ok(rb) => {
            let rec = rb.record(0).expect("post_transfer returns one record");
            pgrx::Uuid::from_bytes(rec[..16].try_into().unwrap())
        }
        Err(msg) => error!("beetle: {}", msg),
    }
}

#[pg_extern]
fn lookup_account(
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
        slot.debit_id = id_bytes;
    }) {
        Ok(rb) => match rb.record(0) {
            Some(rec) => pgrx::iter::TableIterator::once(unpack_account_row(rec)),
            None => pgrx::iter::TableIterator::empty(),
        },
        Err(msg) if msg == "not found" => pgrx::iter::TableIterator::empty(),
        Err(msg) => error!("beetle: {}", msg),
    }
}

#[pg_extern]
fn lookup_transfer(
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
        slot.debit_id = id_bytes;
    }) {
        Ok(rb) => match rb.record(0) {
            Some(rec) => pgrx::iter::TableIterator::once(unpack_transfer_row(rec)),
            None => pgrx::iter::TableIterator::empty(),
        },
        Err(msg) if msg == "not found" => pgrx::iter::TableIterator::empty(),
        Err(msg) => error!("beetle: {}", msg),
    }
}

#[pg_extern]
fn account_transfers(
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
        error!("account_transfers flags must include DEBITS(1) and/or CREDITS(2)");
    }
    let id_bytes = *account_id.as_bytes();
    let rb = match submit_and_wait(|slot| {
        slot.op = OP_GET_ACCOUNT_TRANSFERS;
        slot.debit_id = id_bytes;
        slot.limit = lim;
        slot.flags = flag_bits;
    }) {
        Ok(rb) => rb,
        Err(msg) => error!("beetle: {}", msg),
    };
    let rows: Vec<TransferRow> = (0..rb.count as usize)
        .filter_map(|i| rb.record(i).map(unpack_transfer_row))
        .collect();
    pgrx::iter::TableIterator::new(rows)
}

#[pg_extern]
fn account_balances(
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
        error!("account_balances flags must include DEBITS(1) and/or CREDITS(2)");
    }
    let id_bytes = *account_id.as_bytes();
    let rb = match submit_and_wait(|slot| {
        slot.op = OP_GET_ACCOUNT_BALANCES;
        slot.debit_id = id_bytes;
        slot.limit = lim;
        slot.flags = flag_bits;
    }) {
        Ok(rb) => rb,
        Err(msg) => error!("beetle: {}", msg),
    };
    let rows: Vec<BalanceRow> = (0..rb.count as usize)
        .filter_map(|i| rb.record(i).map(unpack_balance_row))
        .collect();
    pgrx::iter::TableIterator::new(rows)
}

#[pg_extern]
fn query_accounts(
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
        Err(msg) => error!("beetle: {}", msg),
    };
    let rows: Vec<AccountRow> = (0..rb.count as usize)
        .filter_map(|i| rb.record(i).map(unpack_account_row))
        .collect();
    pgrx::iter::TableIterator::new(rows)
}

#[pg_extern]
fn query_transfers(
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
        Err(msg) => error!("beetle: {}", msg),
    };
    let rows: Vec<TransferRow> = (0..rb.count as usize)
        .filter_map(|i| rb.record(i).map(unpack_transfer_row))
        .collect();
    pgrx::iter::TableIterator::new(rows)
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
