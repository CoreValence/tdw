use pgrx::prelude::*;

use crate::shmem::{
    CAPACITY, RECORD_LEN, RESULT_BUF_LEN, RESULTS, RING, S_CLAIMED, S_DONE, S_EMPTY, S_ERROR,
    S_PENDING, Slot, WORKER_LATCH,
};

pub(crate) struct ReadBack {
    pub(crate) count: u32,
    pub(crate) bytes: Vec<u8>,
}

impl ReadBack {
    pub(crate) fn record(&self, i: usize) -> Option<&[u8]> {
        let start = i * RECORD_LEN;
        let end = start + RECORD_LEN;
        if i < self.count as usize && end <= self.bytes.len() {
            Some(&self.bytes[start..end])
        } else {
            None
        }
    }
}

pub(crate) fn submit_and_wait(fill: impl FnOnce(&mut Slot)) -> Result<ReadBack, String> {
    submit_and_wait_inner(fill, None::<fn(&mut [u8; RESULT_BUF_LEN])>)
}

// For ops that write a payload into the slot's pool entry before the worker
// picks them up (currently OP_CREATE_TRANSFER_BATCH, which packs legs). The
// pool is filled after the metadata is in place and the slot is in S_CLAIMED,
// and only then is the slot promoted to S_PENDING so the worker will see it.
pub(crate) fn submit_and_wait_with_legs(
    fill_meta: impl FnOnce(&mut Slot),
    fill_pool: impl FnOnce(&mut [u8; RESULT_BUF_LEN]),
) -> Result<ReadBack, String> {
    submit_and_wait_inner(fill_meta, Some(fill_pool))
}

fn submit_and_wait_inner<M, P>(fill_meta: M, fill_pool: Option<P>) -> Result<ReadBack, String>
where
    M: FnOnce(&mut Slot),
    P: FnOnce(&mut [u8; RESULT_BUF_LEN]),
{
    let worker_latch = *WORKER_LATCH.share();
    if worker_latch == 0 {
        return Err("tdw background worker not running".into());
    }

    let my_latch = unsafe { pg_sys::MyLatch };

    // Ring-full blocks rather than errors: under backend counts > CAPACITY
    // the worker drains slots within ~ms, so briefly yielding the latch and
    // rescanning is cheaper (and kinder to pgbench) than bouncing the client.
    // fill_meta is FnOnce; Option<...>.take() moves it out on the one iteration
    // that acquires a slot.
    let mut fill_meta_opt = Some(fill_meta);
    let slot_idx = loop {
        let chosen = {
            let mut guard = RING.exclusive();
            let cursor = guard.cursor as usize;
            let mut chosen: Option<usize> = None;
            for offset in 0..CAPACITY {
                let i = (cursor + offset) % CAPACITY;
                if guard.slots[i].state == S_EMPTY {
                    chosen = Some(i);
                    break;
                }
            }
            if let Some(i) = chosen {
                guard.cursor = ((i + 1) % CAPACITY) as u32;
                guard.slots[i] = Slot::empty();
                guard.slots[i].session_latch = my_latch as usize;
                guard.slots[i].owner_pid = unsafe { pg_sys::MyProcPid };
                (fill_meta_opt
                    .take()
                    .expect("fill_meta consumed exactly once"))(&mut guard.slots[i]);
                // CLAIMED parks the slot out of EMPTY (so no other backend
                // grabs it) without yet exposing it to the worker. Promoted
                // to PENDING below, after the pool has been written.
                guard.slots[i].state = S_CLAIMED;
            }
            chosen
        };
        if let Some(i) = chosen {
            break i;
        }

        check_for_interrupts!();
        unsafe {
            let wl = pg_sys::WaitLatch(
                my_latch,
                (pg_sys::WL_LATCH_SET | pg_sys::WL_TIMEOUT | pg_sys::WL_POSTMASTER_DEATH) as i32,
                1,
                pg_sys::PG_WAIT_EXTENSION,
            );
            pg_sys::ResetLatch(my_latch);
            if (wl & pg_sys::WL_POSTMASTER_DEATH as i32) != 0 {
                return Err("postmaster died".into());
            }
        }
    };

    // Pool write (if any) happens without the RING lock held.
    if let Some(fill_pool) = fill_pool {
        let mut pool = RESULTS.exclusive();
        fill_pool(&mut pool.bufs[slot_idx]);
    }

    // Publish the slot to the worker.
    {
        let mut guard = RING.exclusive();
        guard.slots[slot_idx].state = S_PENDING;
    }

    unsafe {
        pg_sys::ResetLatch(my_latch);
        pg_sys::SetLatch(worker_latch as *mut pg_sys::Latch);
    }

    loop {
        check_for_interrupts!();

        // Peek state. Once the slot is DONE/ERROR, this backend owns it
        // exclusively until it resets — the worker won't touch it again.
        let snapshot = {
            let guard = RING.share();
            let s = &guard.slots[slot_idx];
            match s.state {
                S_DONE => Some(Ok(s.result_count)),
                S_ERROR => {
                    let n = s.error_len as usize;
                    Some(Err(String::from_utf8_lossy(&s.error_msg[..n]).to_string()))
                }
                _ => None,
            }
        };

        match snapshot {
            Some(Ok(count)) => {
                let n = (count as usize * RECORD_LEN).min(RESULT_BUF_LEN);
                let bytes = {
                    let pool = RESULTS.share();
                    pool.bufs[slot_idx][..n].to_vec()
                };
                {
                    let mut guard = RING.exclusive();
                    guard.slots[slot_idx] = Slot::empty();
                }
                return Ok(ReadBack { count, bytes });
            }
            Some(Err(msg)) => {
                {
                    let mut guard = RING.exclusive();
                    guard.slots[slot_idx] = Slot::empty();
                }
                return Err(msg);
            }
            None => {}
        }

        unsafe {
            let wl = pg_sys::WaitLatch(
                my_latch,
                (pg_sys::WL_LATCH_SET | pg_sys::WL_TIMEOUT | pg_sys::WL_POSTMASTER_DEATH) as i32,
                500,
                pg_sys::PG_WAIT_EXTENSION,
            );
            pg_sys::ResetLatch(my_latch);
            if (wl & pg_sys::WL_POSTMASTER_DEATH as i32) != 0 {
                return Err("postmaster died".into());
            }
        }
    }
}
