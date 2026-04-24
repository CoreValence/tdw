use pgrx::bgworkers::{BackgroundWorkerBuilder, BgWorkerStartTime};
use pgrx::pg_shmem_init;
use pgrx::prelude::*;
use std::time::Duration;

mod api;
mod fdw;
mod guc;
mod pack;
mod shmem;
mod submit;
mod worker;

use shmem::{RESULTS, RING, ResultPool, Ring, WORKER_LATCH};

::pgrx::pg_module_magic!(name, version);

#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    guc::register();

    pg_shmem_init!(RING = Ring::empty());
    pg_shmem_init!(RESULTS = ResultPool::empty());
    pg_shmem_init!(WORKER_LATCH = 0usize);

    BackgroundWorkerBuilder::new("beetle worker")
        .set_function("beetle_worker_main")
        .set_library("beetle")
        .set_start_time(BgWorkerStartTime::PostmasterStart)
        .set_restart_time(Some(Duration::from_secs(5)))
        .enable_shmem_access(None)
        .load();
}
