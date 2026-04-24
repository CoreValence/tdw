use opentelemetry::KeyValue;
use opentelemetry::global;
use opentelemetry::metrics::{Counter, Histogram, MeterProvider as _};
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::{MetricExporter, SpanExporter, WithExportConfig};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::metrics::periodic_reader_with_async_runtime::PeriodicReader;
use opentelemetry_sdk::runtime::TokioCurrentThread;
use opentelemetry_sdk::trace::SdkTracerProvider;
use opentelemetry_sdk::trace::span_processor_with_async_runtime::BatchSpanProcessor;
use pgrx::log;
use pgrx::{PGRXSharedMemory, PgAtomic};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::guc::{METRICS_INTERVAL_MS, OTLP_ENDPOINT, OTLP_SERVICE_NAME};
use crate::shmem::{
    OP_CREATE_ACCOUNT, OP_CREATE_TRANSFER, OP_CREATE_TRANSFER_BATCH, OP_GET_ACCOUNT_BALANCES,
    OP_GET_ACCOUNT_TRANSFERS, OP_LOOKUP_ACCOUNT, OP_LOOKUP_TRANSFER, OP_QUERY_ACCOUNTS,
    OP_QUERY_TRANSFERS,
};

// Shmem counters written by backends. Only fields that cross the process
// boundary live here; worker-only values sit in process-local statics below.
#[repr(C)]
pub(crate) struct Stats {
    pub(crate) ring_full_blocks: AtomicU64,
}

impl Stats {
    pub(crate) const fn new() -> Self {
        Stats {
            ring_full_blocks: AtomicU64::new(0),
        }
    }
}

unsafe impl PGRXSharedMemory for Stats {}

pub(crate) static STATS: PgAtomic<Stats> = unsafe { PgAtomic::new(c"beetle_stats") };

// Worker-local gauges. The worker writes these at the top of each drain loop;
// the observable-gauge callback reads them without touching shmem locks.
static LAST_OCC_PENDING: AtomicU64 = AtomicU64::new(0);
static LAST_OCC_IN_FLIGHT: AtomicU64 = AtomicU64::new(0);

pub(crate) fn set_occupancy(pending: u64, in_flight: u64) {
    LAST_OCC_PENDING.store(pending, Ordering::Relaxed);
    LAST_OCC_IN_FLIGHT.store(in_flight, Ordering::Relaxed);
}

// CLOCK_MONOTONIC reads the kernel-boot clock — comparable across processes
// on the same host, so backends and the worker can diff timestamps written
// into slot fields.
pub(crate) fn now_monotonic_ns() -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    unsafe {
        libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts);
    }
    (ts.tv_sec as u64)
        .wrapping_mul(1_000_000_000)
        .wrapping_add(ts.tv_nsec as u64)
}

pub(crate) fn op_label(op: u8) -> &'static str {
    match op {
        OP_CREATE_ACCOUNT => "create_account",
        OP_CREATE_TRANSFER => "create_transfer",
        OP_CREATE_TRANSFER_BATCH => "create_transfer_batch",
        OP_LOOKUP_ACCOUNT => "lookup_account",
        OP_LOOKUP_TRANSFER => "lookup_transfer",
        OP_GET_ACCOUNT_TRANSFERS => "get_account_transfers",
        OP_GET_ACCOUNT_BALANCES => "get_account_balances",
        OP_QUERY_ACCOUNTS => "query_accounts",
        OP_QUERY_TRANSFERS => "query_transfers",
        _ => "unknown",
    }
}

pub(crate) struct Instruments {
    pub tracer: opentelemetry_sdk::trace::Tracer,
    pub submit_count: Counter<u64>,
    pub submit_errors: Counter<u64>,
    pub pending_wait_ms: Histogram<f64>,
    pub drain_size: Histogram<u64>,
    pub tb_call_ms: Histogram<f64>,
    pub publish_ms: Histogram<f64>,
    // Held so the providers and their export threads stay alive for the
    // worker's lifetime. Dropping either stops exports.
    _tracer_provider: SdkTracerProvider,
    _meter_provider: SdkMeterProvider,
}

static INSTRUMENTS: OnceLock<Instruments> = OnceLock::new();

pub(crate) fn instruments() -> Option<&'static Instruments> {
    INSTRUMENTS.get()
}

pub(crate) fn init(rt: &tokio::runtime::Runtime) {
    let endpoint = OTLP_ENDPOINT
        .get()
        .and_then(|s| s.into_string().ok())
        .unwrap_or_default();
    if endpoint.trim().is_empty() {
        log!("beetle: OTLP disabled (beetle.otlp_endpoint not set)");
        return;
    }
    let service_name = OTLP_SERVICE_NAME
        .get()
        .and_then(|s| s.into_string().ok())
        .unwrap_or_else(|| "beetle".into());
    let interval = Duration::from_millis(METRICS_INTERVAL_MS.get() as u64);

    let resource = Resource::builder().with_service_name(service_name).build();

    // Exporter/channel construction needs a tokio reactor (tonic); do it
    // inside the worker runtime. The async batch processors spawn their own
    // std::threads with private current-thread runtimes, so once built they
    // are independent of this runtime.
    let (tracer_provider, meter_provider) = rt.block_on(async {
        let span_exporter = SpanExporter::builder()
            .with_tonic()
            .with_endpoint(&endpoint)
            .build()
            .expect("beetle: otlp span exporter");
        let span_processor = BatchSpanProcessor::builder(span_exporter, TokioCurrentThread).build();
        let tracer_provider = SdkTracerProvider::builder()
            .with_resource(resource.clone())
            .with_span_processor(span_processor)
            .build();

        let metric_exporter = MetricExporter::builder()
            .with_tonic()
            .with_endpoint(&endpoint)
            .build()
            .expect("beetle: otlp metric exporter");
        let reader = PeriodicReader::builder(metric_exporter, TokioCurrentThread)
            .with_interval(interval)
            .build();
        let meter_provider = SdkMeterProvider::builder()
            .with_resource(resource)
            .with_reader(reader)
            .build();

        (tracer_provider, meter_provider)
    });

    global::set_tracer_provider(tracer_provider.clone());
    global::set_meter_provider(meter_provider.clone());

    let tracer = tracer_provider.tracer("beetle");
    let meter = meter_provider.meter("beetle");

    let submit_count = meter.u64_counter("beetle.submit.count").build();
    let submit_errors = meter.u64_counter("beetle.submit.errors").build();
    let pending_wait_ms = meter
        .f64_histogram("beetle.worker.pending_wait.ms")
        .with_unit("ms")
        .build();
    let drain_size = meter.u64_histogram("beetle.worker.drain.size").build();
    let tb_call_ms = meter
        .f64_histogram("beetle.worker.tb_call.ms")
        .with_unit("ms")
        .build();
    let publish_ms = meter
        .f64_histogram("beetle.worker.publish.ms")
        .with_unit("ms")
        .build();

    // Observable instruments: callbacks fire on the export cadence and read
    // live values from shmem / worker-local statics. Not retained — the meter
    // provider owns them.
    let _ = meter
        .u64_observable_counter("beetle.ring.full_blocks")
        .with_callback(|obs| {
            obs.observe(STATS.get().ring_full_blocks.load(Ordering::Relaxed), &[]);
        })
        .build();
    let _ = meter
        .u64_observable_gauge("beetle.ring.occupancy")
        .with_callback(|obs| {
            obs.observe(
                LAST_OCC_PENDING.load(Ordering::Relaxed),
                &[KeyValue::new("state", "pending")],
            );
            obs.observe(
                LAST_OCC_IN_FLIGHT.load(Ordering::Relaxed),
                &[KeyValue::new("state", "in_flight")],
            );
        })
        .build();

    let _ = INSTRUMENTS.set(Instruments {
        tracer,
        submit_count,
        submit_errors,
        pending_wait_ms,
        drain_size,
        tb_call_ms,
        publish_ms,
        _tracer_provider: tracer_provider,
        _meter_provider: meter_provider,
    });

    log!(
        "beetle: OTLP telemetry initialized (endpoint={}, interval={}ms)",
        endpoint,
        METRICS_INTERVAL_MS.get()
    );
}
