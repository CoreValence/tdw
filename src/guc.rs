use pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting};
use std::ffi::CString;

pub(crate) static TB_ADDR: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(Some(c"3000"));

pub(crate) static TB_CLUSTER_ID: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(Some(c"0"));

pub(crate) static BATCH_WAIT_MS: GucSetting<i32> = GucSetting::<i32>::new(1);

pub(crate) static BATCH_MAX: GucSetting<i32> = GucSetting::<i32>::new(8189);

pub(crate) static OTLP_ENDPOINT: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(None);

pub(crate) static OTLP_SERVICE_NAME: GucSetting<Option<CString>> =
    GucSetting::<Option<CString>>::new(Some(c"beetle"));

pub(crate) static METRICS_INTERVAL_MS: GucSetting<i32> = GucSetting::<i32>::new(1000);

// Row ceilings for the FDW's paginated multi-row scans. See drive_pagination
// in fdw.rs: the first bounds an unlimited scan (no user LIMIT); the second
// bounds even a user-supplied LIMIT. Both exist to keep an unfiltered scan
// of a large TB cluster from draining unbounded rows into backend memory.
pub(crate) static FDW_DEFAULT_SCAN_LIMIT: GucSetting<i32> = GucSetting::<i32>::new(1_000);
pub(crate) static FDW_PAGINATION_CAP: GucSetting<i32> = GucSetting::<i32>::new(10_000);

pub(crate) fn register() {
    GucRegistry::define_string_guc(
        c"beetle.tb_addr",
        c"TigerBeetle replica address(es).",
        c"Comma-separated list of `port` or `ip:port` / `host:port` entries. Hostnames are resolved once at worker start.",
        &TB_ADDR,
        GucContext::Postmaster,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"beetle.tb_cluster_id",
        c"TigerBeetle cluster id (u128, decimal).",
        c"Parsed as u128. Must match the cluster the replica was formatted with.",
        &TB_CLUSTER_ID,
        GucContext::Postmaster,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"beetle.batch_wait_ms",
        c"Worker idle wait between batch drains, in ms.",
        c"Lower is lower-latency but more wakeups; higher batches more aggressively.",
        &BATCH_WAIT_MS,
        1,
        1000,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"beetle.batch_max",
        c"Maximum slots drained into a single TigerBeetle request.",
        c"Capped by TigerBeetle's per-message limit (8189 for transfers/accounts at 128 B).",
        &BATCH_MAX,
        1,
        8189,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"beetle.otlp_endpoint",
        c"OTLP gRPC endpoint for traces/metrics (e.g. http://localhost:4317). Empty disables.",
        c"Endpoint is read at worker start; changes require a worker restart.",
        &OTLP_ENDPOINT,
        GucContext::Postmaster,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"beetle.otlp_service_name",
        c"service.name resource attribute on exported telemetry.",
        c"Shown as the service in Tempo/Jaeger/Grafana.",
        &OTLP_SERVICE_NAME,
        GucContext::Postmaster,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"beetle.metrics_interval_ms",
        c"Metric export interval (ms).",
        c"Worker pushes accumulated counters/histograms to the collector on this cadence.",
        &METRICS_INTERVAL_MS,
        100,
        60_000,
        GucContext::Postmaster,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"beetle.fdw_default_scan_limit",
        c"Row ceiling for FDW scans that lack an explicit LIMIT clause.",
        c"Applies to SELECT * FROM tb_* without LIMIT. Prevents an unfiltered scan on a large TB cluster from draining unbounded rows into backend memory. Raise to enumerate larger result sets; cap with beetle.fdw_pagination_cap. Per-session SET is allowed.",
        &FDW_DEFAULT_SCAN_LIMIT,
        1,
        10_000_000,
        GucContext::Userset,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"beetle.fdw_pagination_cap",
        c"Hard ceiling on rows any single FDW scan will drain, even with an explicit LIMIT.",
        c"User-supplied LIMIT is clamped to this value. Protects against typos like LIMIT 1000000 on a ledger with millions of rows. Per-session SET is allowed.",
        &FDW_PAGINATION_CAP,
        1,
        10_000_000,
        GucContext::Userset,
        GucFlags::default(),
    );
}
