use pgrx::{PGRXSharedMemory, PgLwLock};

// Slot (~2.3 KB with MAX_QUERY_ROWS=16) × CAPACITY lives in shmem. During
// _PG_init, pg_shmem_init! builds the Ring by value on the postmaster stack
// (twice, briefly, while wrapping into Shared<T>). 8 MB default stack bounds
// this product at ~3 MB to leave headroom for pgrx/postgres frames.
// Bursts above CAPACITY block on the session latch (see submit::submit_and_wait).
pub(crate) const CAPACITY: usize = 1024;

pub(crate) const S_EMPTY: u8 = 0;
pub(crate) const S_PENDING: u8 = 1;
pub(crate) const S_IN_FLIGHT: u8 = 2;
pub(crate) const S_DONE: u8 = 3;
pub(crate) const S_ERROR: u8 = 4;

pub(crate) const OP_NONE: u8 = 0;
pub(crate) const OP_CREATE_ACCOUNT: u8 = 1;
pub(crate) const OP_CREATE_TRANSFER: u8 = 2;
pub(crate) const OP_LOOKUP_ACCOUNT: u8 = 3;
pub(crate) const OP_LOOKUP_TRANSFER: u8 = 4;
pub(crate) const OP_GET_ACCOUNT_TRANSFERS: u8 = 5;
pub(crate) const OP_GET_ACCOUNT_BALANCES: u8 = 6;
pub(crate) const OP_QUERY_ACCOUNTS: u8 = 7;
pub(crate) const OP_QUERY_TRANSFERS: u8 = 8;
// Single-slot batch of N transfers. Worker unpacks up to MAX_BATCH_LEGS legs
// from result_buf and submits them as a single client.create_transfers call,
// so a LINKED chain stays atomic on the TB side.
pub(crate) const OP_CREATE_TRANSFER_BATCH: u8 = 9;

// All packed records are 128 bytes (RECORD_LEN) regardless of type.
// Multi-row read responses carry up to MAX_QUERY_ROWS records, so the
// per-slot result buffer is RECORD_LEN * MAX_QUERY_ROWS. Increasing
// MAX_QUERY_ROWS grows the shmem segment by CAPACITY * RECORD_LEN bytes
// per added row.
//
// Why: pg_shmem_init! constructs Ring by-value on the backend stack before
// copying into shmem. With CAPACITY=1024 slots, each additional byte of Slot
// costs 1 KB of transient stack; 64 rows × 128 B result_buf per slot pushed
// total past the 8 MB default backend stack and segfaulted at _PG_init.
pub(crate) const RECORD_LEN: usize = 128;
pub(crate) const MAX_QUERY_ROWS: u32 = 16;

pub(crate) const ERR_BUF_LEN: usize = 128;
pub(crate) const RESULT_BUF_LEN: usize = RECORD_LEN * MAX_QUERY_ROWS as usize;

// OP_CREATE_TRANSFER_BATCH packs its legs into result_buf on the way in and
// the worker overwrites it with the resulting ids on the way out (each id
// padded to RECORD_LEN so ReadBack::record() can decode them at the same
// stride as any other read). Cap is governed by the output side:
// RESULT_BUF_LEN / RECORD_LEN = MAX_QUERY_ROWS. One batch call to TB keeps a
// LINKED chain atomic.
pub(crate) const BATCH_LEG_LEN: usize = 96;
pub(crate) const MAX_BATCH_LEGS: usize = MAX_QUERY_ROWS as usize;

#[derive(Clone, Copy)]
#[repr(C)]
pub(crate) struct Slot {
    pub(crate) state: u8,
    pub(crate) op: u8,
    // Primary record id:
    //   CREATE_ACCOUNT / CREATE_TRANSFER → new record id (client-provided)
    //   LOOKUP_* / GET_ACCOUNT_* → id being looked up
    //   CREATE_TRANSFER_BATCH → unused (legs carry their own ids)
    pub(crate) id: [u8; 16],
    // CREATE_TRANSFER only.
    pub(crate) debit_id: [u8; 16],
    // CREATE_TRANSFER only.
    pub(crate) credit_id: [u8; 16],
    // POST_PENDING / VOID_PENDING (both ride OP_CREATE_TRANSFER with flags):
    //   the pending transfer this one completes.
    pub(crate) pending_id: [u8; 16],
    pub(crate) amount: u128,
    pub(crate) ledger: u32,
    pub(crate) code: u16,
    pub(crate) flags: u32,
    // For reads: row limit.
    // For CREATE_TRANSFER_BATCH: number of legs packed in result_buf.
    pub(crate) limit: u32,
    // For single-record writes: first 16 bytes hold the assigned id, result_count = 1.
    // For single-row reads: one RECORD_LEN record, result_count = 1.
    // For multi-row reads: result_count records packed back-to-back,
    //   each RECORD_LEN bytes (see pack::pack_account/pack_transfer/pack_balance).
    // For CREATE_TRANSFER_BATCH (input): `limit` legs of BATCH_LEG_LEN bytes each;
    //   (output): result_count ids of 16 bytes each packed at the start of the buf.
    pub(crate) result_count: u32,
    pub(crate) result_buf: [u8; RESULT_BUF_LEN],
    pub(crate) error_len: u16,
    pub(crate) error_msg: [u8; ERR_BUF_LEN],
    pub(crate) session_latch: usize,
}

impl Slot {
    pub(crate) const fn empty() -> Self {
        Slot {
            state: S_EMPTY,
            op: OP_NONE,
            id: [0; 16],
            debit_id: [0; 16],
            credit_id: [0; 16],
            pending_id: [0; 16],
            amount: 0,
            ledger: 0,
            code: 0,
            flags: 0,
            limit: 0,
            result_count: 0,
            result_buf: [0; RESULT_BUF_LEN],
            error_len: 0,
            error_msg: [0; ERR_BUF_LEN],
            session_latch: 0,
        }
    }
}

#[derive(Clone, Copy)]
#[repr(C)]
pub(crate) struct Ring {
    pub(crate) slots: [Slot; CAPACITY],
    pub(crate) cursor: u32,
}

impl Ring {
    pub(crate) const fn empty() -> Self {
        Ring {
            slots: [Slot::empty(); CAPACITY],
            cursor: 0,
        }
    }
}

unsafe impl PGRXSharedMemory for Ring {}

pub(crate) static RING: PgLwLock<Ring> = unsafe { PgLwLock::new(c"beetle_ring") };
pub(crate) static WORKER_LATCH: PgLwLock<usize> = unsafe { PgLwLock::new(c"beetle_worker_latch") };
