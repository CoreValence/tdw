use pgrx::{PGRXSharedMemory, PgLwLock};

// Per-slot result/leg buffer lives in ResultPool (a separate shmem segment),
// keyed by slot index. Keeping it out of Slot shrinks the per-slot metadata
// from ~2.3 KB to ~256 B, which (a) shortens the RING lwlock critical section
// on state transitions, since the 2 KB memcpy now runs under RESULTS instead
// of RING, and (b) lets CAPACITY grow without Slot-size pressure on the
// backend stack at _PG_init — the two segments initialize independently and
// pg_shmem_init!'s 2× stack tax applies to each on its own.
pub(crate) const CAPACITY: usize = 1024;

pub(crate) const S_EMPTY: u8 = 0;
// Backend has claimed the slot and written metadata, but pool payload (if any)
// is not yet written. Worker skips CLAIMED slots on drain.
pub(crate) const S_CLAIMED: u8 = 1;
pub(crate) const S_PENDING: u8 = 2;
pub(crate) const S_IN_FLIGHT: u8 = 3;
pub(crate) const S_DONE: u8 = 4;
pub(crate) const S_ERROR: u8 = 5;

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
// from the slot's pool entry and submits them as a single create_transfers
// call, so a LINKED chain stays atomic on the TB side.
pub(crate) const OP_CREATE_TRANSFER_BATCH: u8 = 9;

// All packed records are 128 bytes (RECORD_LEN) regardless of type.
// Multi-row read responses carry up to MAX_QUERY_ROWS records, so the
// per-slot pool entry is RECORD_LEN * MAX_QUERY_ROWS.
pub(crate) const RECORD_LEN: usize = 128;
pub(crate) const MAX_QUERY_ROWS: u32 = 16;

pub(crate) const ERR_BUF_LEN: usize = 128;
pub(crate) const RESULT_BUF_LEN: usize = RECORD_LEN * MAX_QUERY_ROWS as usize;

// OP_CREATE_TRANSFER_BATCH packs its legs into the slot's pool entry on the
// way in and the worker overwrites it with the resulting ids on the way out
// (each id padded to RECORD_LEN so ReadBack::record() can decode them at the
// same stride as any other read).
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
    // For CREATE_TRANSFER_BATCH: number of legs packed in the pool entry.
    pub(crate) limit: u32,
    // For single-record writes: first 16 bytes of the pool entry hold the
    //   assigned id, result_count = 1.
    // For single-row reads: one RECORD_LEN record in the pool entry, result_count = 1.
    // For multi-row reads: result_count records packed back-to-back,
    //   each RECORD_LEN bytes (see pack::pack_account/pack_transfer/pack_balance).
    // For CREATE_TRANSFER_BATCH (input): `limit` legs of BATCH_LEG_LEN bytes each;
    //   (output): result_count ids of 16 bytes each packed at the start of the buf.
    pub(crate) result_count: u32,
    pub(crate) error_len: u16,
    pub(crate) error_msg: [u8; ERR_BUF_LEN],
    pub(crate) session_latch: usize,
    // CLOCK_MONOTONIC ns at the moment the backend promoted the slot to
    // S_PENDING. Worker computes pending_wait = now - pending_ns at drain
    // time. Zero when the slot is not on the wire.
    pub(crate) pending_ns: u64,
    // TB pagination cursor for multi-row reads (QUERY_*, GET_ACCOUNT_*):
    // inclusive lower bound on TB timestamps. Zero means "no lower bound"
    // (first page). On subsequent pages the backend sets it to
    // last_returned_ts + 1, so a result set larger than MAX_QUERY_ROWS can
    // be drained in multiple round-trips without exposing the per-slot cap.
    // Unused by lookups and writes.
    pub(crate) timestamp_min: u64,
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
            error_len: 0,
            error_msg: [0; ERR_BUF_LEN],
            session_latch: 0,
            pending_ns: 0,
            timestamp_min: 0,
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

// Per-slot payload pool. bufs[i] is owned by slot i for the duration of its
// op. Splits the big memcpy out of the RING critical section so backends
// claiming new slots don't wait behind the worker writing a 2 KB result.
#[derive(Clone, Copy)]
#[repr(C)]
pub(crate) struct ResultPool {
    pub(crate) bufs: [[u8; RESULT_BUF_LEN]; CAPACITY],
}

impl ResultPool {
    pub(crate) const fn empty() -> Self {
        ResultPool {
            bufs: [[0; RESULT_BUF_LEN]; CAPACITY],
        }
    }
}

unsafe impl PGRXSharedMemory for Ring {}
unsafe impl PGRXSharedMemory for ResultPool {}

pub(crate) static RING: PgLwLock<Ring> = unsafe { PgLwLock::new(c"beetle_ring") };
pub(crate) static RESULTS: PgLwLock<ResultPool> = unsafe { PgLwLock::new(c"beetle_results") };
pub(crate) static WORKER_LATCH: PgLwLock<usize> = unsafe { PgLwLock::new(c"beetle_worker_latch") };
