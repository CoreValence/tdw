use std::time::{SystemTime, UNIX_EPOCH};

use crate::shmem::RECORD_LEN;

// Packed account layout (RECORD_LEN bytes):
//   [0..16]   id              (u128 LE)
//   [16..32]  debits_posted   (u128 LE)
//   [32..48]  credits_posted  (u128 LE)
//   [48..64]  debits_pending  (u128 LE)
//   [64..80]  credits_pending (u128 LE)
//   [80..84]  ledger          (u32 LE)
//   [84..86]  code            (u16 LE)
//   [86..88]  flags           (u16 LE)
//   [88..96]  timestamp       (u64 LE)
pub(crate) fn pack_account(a: &tigerbeetle_unofficial::Account) -> [u8; RECORD_LEN] {
    let mut buf = [0u8; RECORD_LEN];
    buf[0..16].copy_from_slice(&a.id().to_le_bytes());
    buf[16..32].copy_from_slice(&a.debits_posted().to_le_bytes());
    buf[32..48].copy_from_slice(&a.credits_posted().to_le_bytes());
    buf[48..64].copy_from_slice(&a.debits_pending().to_le_bytes());
    buf[64..80].copy_from_slice(&a.credits_pending().to_le_bytes());
    buf[80..84].copy_from_slice(&a.ledger().to_le_bytes());
    buf[84..86].copy_from_slice(&a.code().to_le_bytes());
    buf[86..88].copy_from_slice(&a.flags().bits().to_le_bytes());
    buf[88..96].copy_from_slice(&systemtime_nanos(a.timestamp()).to_le_bytes());
    buf
}

// Packed transfer layout (RECORD_LEN bytes):
//   [0..16]   id                 (u128 LE)
//   [16..32]  debit_account_id   (u128 LE)
//   [32..48]  credit_account_id  (u128 LE)
//   [48..64]  amount             (u128 LE)
//   [64..68]  ledger             (u32 LE)
//   [68..70]  code               (u16 LE)
//   [70..72]  flags              (u16 LE)
//   [72..80]  timestamp          (u64 LE)
pub(crate) fn pack_transfer(t: &tigerbeetle_unofficial::Transfer) -> [u8; RECORD_LEN] {
    let mut buf = [0u8; RECORD_LEN];
    buf[0..16].copy_from_slice(&t.id().to_le_bytes());
    buf[16..32].copy_from_slice(&t.debit_account_id().to_le_bytes());
    buf[32..48].copy_from_slice(&t.credit_account_id().to_le_bytes());
    buf[48..64].copy_from_slice(&t.amount().to_le_bytes());
    buf[64..68].copy_from_slice(&t.ledger().to_le_bytes());
    buf[68..70].copy_from_slice(&t.code().to_le_bytes());
    buf[70..72].copy_from_slice(&t.flags().bits().to_le_bytes());
    buf[72..80].copy_from_slice(&systemtime_nanos(t.timestamp()).to_le_bytes());
    buf
}

// Packed balance layout (RECORD_LEN bytes):
//   [0..16]   debits_posted   (u128 LE)
//   [16..32]  credits_posted  (u128 LE)
//   [32..48]  debits_pending  (u128 LE)
//   [48..64]  credits_pending (u128 LE)
//   [64..72]  timestamp       (u64 LE)
pub(crate) fn pack_balance(b: &tigerbeetle_unofficial::account::Balance) -> [u8; RECORD_LEN] {
    let mut buf = [0u8; RECORD_LEN];
    buf[0..16].copy_from_slice(&b.debits_posted().to_le_bytes());
    buf[16..32].copy_from_slice(&b.credits_posted().to_le_bytes());
    buf[32..48].copy_from_slice(&b.debits_pending().to_le_bytes());
    buf[48..64].copy_from_slice(&b.credits_pending().to_le_bytes());
    buf[64..72].copy_from_slice(&systemtime_nanos(b.timestamp()).to_le_bytes());
    buf
}

fn systemtime_nanos(ts: SystemTime) -> u64 {
    ts.duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

pub(crate) type AccountRow = (
    pgrx::Uuid,
    i32,
    i32,
    pgrx::AnyNumeric,
    pgrx::AnyNumeric,
    pgrx::AnyNumeric,
    pgrx::AnyNumeric,
    i32,
    i64,
);

pub(crate) type TransferRow = (
    pgrx::Uuid,
    pgrx::Uuid,
    pgrx::Uuid,
    pgrx::AnyNumeric,
    i32,
    i32,
    i32,
    i64,
);

pub(crate) type BalanceRow = (
    pgrx::AnyNumeric,
    pgrx::AnyNumeric,
    pgrx::AnyNumeric,
    pgrx::AnyNumeric,
    i64,
);

pub(crate) fn u128_to_numeric(v: u128) -> pgrx::AnyNumeric {
    pgrx::AnyNumeric::try_from(v.to_string().as_str())
        .expect("u128 decimal is always a valid NUMERIC")
}

pub(crate) fn unpack_account_row(rec: &[u8]) -> AccountRow {
    let id: [u8; 16] = rec[0..16].try_into().unwrap();
    let debits_posted = u128::from_le_bytes(rec[16..32].try_into().unwrap());
    let credits_posted = u128::from_le_bytes(rec[32..48].try_into().unwrap());
    let debits_pending = u128::from_le_bytes(rec[48..64].try_into().unwrap());
    let credits_pending = u128::from_le_bytes(rec[64..80].try_into().unwrap());
    let ledger = u32::from_le_bytes(rec[80..84].try_into().unwrap()) as i32;
    let code = u16::from_le_bytes(rec[84..86].try_into().unwrap()) as i32;
    let flags = u16::from_le_bytes(rec[86..88].try_into().unwrap()) as i32;
    let timestamp = u64::from_le_bytes(rec[88..96].try_into().unwrap()) as i64;
    (
        pgrx::Uuid::from_bytes(id),
        ledger,
        code,
        u128_to_numeric(debits_posted),
        u128_to_numeric(credits_posted),
        u128_to_numeric(debits_pending),
        u128_to_numeric(credits_pending),
        flags,
        timestamp,
    )
}

pub(crate) fn unpack_transfer_row(rec: &[u8]) -> TransferRow {
    let id: [u8; 16] = rec[0..16].try_into().unwrap();
    let debit: [u8; 16] = rec[16..32].try_into().unwrap();
    let credit: [u8; 16] = rec[32..48].try_into().unwrap();
    let amount = u128::from_le_bytes(rec[48..64].try_into().unwrap());
    let ledger = u32::from_le_bytes(rec[64..68].try_into().unwrap()) as i32;
    let code = u16::from_le_bytes(rec[68..70].try_into().unwrap()) as i32;
    let flags = u16::from_le_bytes(rec[70..72].try_into().unwrap()) as i32;
    let timestamp = u64::from_le_bytes(rec[72..80].try_into().unwrap()) as i64;
    (
        pgrx::Uuid::from_bytes(id),
        pgrx::Uuid::from_bytes(debit),
        pgrx::Uuid::from_bytes(credit),
        u128_to_numeric(amount),
        ledger,
        code,
        flags,
        timestamp,
    )
}

pub(crate) fn unpack_balance_row(rec: &[u8]) -> BalanceRow {
    let debits_posted = u128::from_le_bytes(rec[0..16].try_into().unwrap());
    let credits_posted = u128::from_le_bytes(rec[16..32].try_into().unwrap());
    let debits_pending = u128::from_le_bytes(rec[32..48].try_into().unwrap());
    let credits_pending = u128::from_le_bytes(rec[48..64].try_into().unwrap());
    let timestamp = u64::from_le_bytes(rec[64..72].try_into().unwrap()) as i64;
    (
        u128_to_numeric(debits_posted),
        u128_to_numeric(credits_posted),
        u128_to_numeric(debits_pending),
        u128_to_numeric(credits_pending),
        timestamp,
    )
}
