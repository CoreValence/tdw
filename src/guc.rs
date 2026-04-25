use pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting};
use std::ffi::CString;

pub(crate) static TB_ADDR: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(Some(c"3000"));

pub(crate) static TB_CLUSTER_ID: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(Some(c"0"));

pub(crate) static BATCH_WAIT_MS: GucSetting<i32> = GucSetting::<i32>::new(1);

pub(crate) static BATCH_MAX: GucSetting<i32> = GucSetting::<i32>::new(8189);

pub(crate) fn register() {
    GucRegistry::define_string_guc(
        c"tdw.tb_addr",
        c"TigerBeetle replica address(es).",
        c"Comma-separated list of `port` or `ip:port` / `host:port` entries. Hostnames are resolved once at worker start.",
        &TB_ADDR,
        GucContext::Postmaster,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"tdw.tb_cluster_id",
        c"TigerBeetle cluster id (u128, decimal).",
        c"Parsed as u128. Must match the cluster the replica was formatted with.",
        &TB_CLUSTER_ID,
        GucContext::Postmaster,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"tdw.batch_wait_ms",
        c"Worker idle wait between batch drains, in ms.",
        c"Lower is lower-latency but more wakeups; higher batches more aggressively.",
        &BATCH_WAIT_MS,
        1,
        1000,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"tdw.batch_max",
        c"Maximum slots drained into a single TigerBeetle request.",
        c"Capped by TigerBeetle's per-message limit (8189 for transfers/accounts at 128 B).",
        &BATCH_MAX,
        1,
        8189,
        GucContext::Sighup,
        GucFlags::default(),
    );
}
