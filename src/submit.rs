use pgrx::prelude::*;

use crate::shmem::{
    CAPACITY, RECORD_LEN, RESULT_BUF_LEN, RING, S_DONE, S_EMPTY, S_ERROR, S_PENDING, Slot,
    WORKER_LATCH,
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
    let worker_latch = *WORKER_LATCH.share();
    if worker_latch == 0 {
        return Err("beetle background worker not running".into());
    }

    let my_latch = unsafe { pg_sys::MyLatch };

    // Ring-full blocks rather than errors: under backend counts > CAPACITY
    // the worker drains slots within ~ms, so briefly yielding the latch and
    // rescanning is cheaper (and kinder to pgbench) than bouncing the client.
    // fill is FnOnce; Option<...>.take() moves it out on the one iteration
    // that acquires a slot.
    let mut fill_opt = Some(fill);
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
                (fill_opt.take().expect("fill consumed exactly once"))(&mut guard.slots[i]);
                guard.slots[i].state = S_PENDING;
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

    unsafe {
        pg_sys::ResetLatch(my_latch);
        pg_sys::SetLatch(worker_latch as *mut pg_sys::Latch);
    }

    loop {
        check_for_interrupts!();

        let outcome = {
            let mut guard = RING.exclusive();
            let s = &mut guard.slots[slot_idx];
            match s.state {
                S_DONE => {
                    let count = s.result_count;
                    let n = (count as usize * RECORD_LEN).min(RESULT_BUF_LEN);
                    let bytes = s.result_buf[..n].to_vec();
                    *s = Slot::empty();
                    Some(Ok(ReadBack { count, bytes }))
                }
                S_ERROR => {
                    let n = s.error_len as usize;
                    let msg = String::from_utf8_lossy(&s.error_msg[..n]).to_string();
                    *s = Slot::empty();
                    Some(Err(msg))
                }
                _ => None,
            }
        };

        if let Some(r) = outcome {
            return r;
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
