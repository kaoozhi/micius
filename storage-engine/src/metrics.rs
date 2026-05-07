use std::sync::OnceLock;

use prometheus::{
    Encoder, Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge, Opts,
    register_histogram, register_histogram_vec, register_int_counter, register_int_counter_vec,
    register_int_gauge,
};

// ── WAL ───────────────────────────────────────────────────────────────────────

pub fn wal_append_duration() -> &'static HistogramVec {
    static M: OnceLock<HistogramVec> = OnceLock::new();
    M.get_or_init(|| {
        register_histogram_vec!(
            HistogramOpts::new(
                "micius_wal_append_duration_seconds",
                "WAL append + fsync duration in seconds"
            )
            .buckets(vec![0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1]),
            &["result"]
        )
        .expect("metric registration failed")
    })
}

pub fn wal_entries_total() -> &'static IntCounterVec {
    static M: OnceLock<IntCounterVec> = OnceLock::new();
    M.get_or_init(|| {
        register_int_counter_vec!(
            Opts::new("micius_wal_entries_total", "Total WAL batches written"),
            &["result"]
        )
        .expect("metric registration failed")
    })
}

// ── Memtable ──────────────────────────────────────────────────────────────────

pub fn memtable_size_bytes() -> &'static IntGauge {
    static M: OnceLock<IntGauge> = OnceLock::new();
    M.get_or_init(|| {
        register_int_gauge!(Opts::new(
            "micius_memtable_size_bytes",
            "Current memtable size in bytes"
        ))
        .expect("metric registration failed")
    })
}

pub fn memtable_flush_total() -> &'static IntCounterVec {
    static M: OnceLock<IntCounterVec> = OnceLock::new();
    M.get_or_init(|| {
        register_int_counter_vec!(
            Opts::new("micius_memtable_flush_total", "Total memtable flushes"),
            &["result"]
        )
        .expect("metric registration failed")
    })
}

pub fn memtable_flush_duration_seconds() -> &'static Histogram {
    static M: OnceLock<Histogram> = OnceLock::new();
    M.get_or_init(|| {
        register_histogram!(
            HistogramOpts::new(
                "micius_memtable_flush_duration_seconds",
                "Time to flush memtable to a chunk file"
            )
            .buckets(vec![0.01, 0.05, 0.1, 0.5, 1.0, 5.0])
        )
        .expect("metric registration failed")
    })
}

// ── Chunk store ───────────────────────────────────────────────────────────────

pub fn chunk_files_total() -> &'static IntGauge {
    static M: OnceLock<IntGauge> = OnceLock::new();
    M.get_or_init(|| {
        register_int_gauge!(Opts::new(
            "micius_chunk_files_total",
            "Total chunk files on disk"
        ))
        .expect("metric registration failed")
    })
}

pub fn chunk_bytes_written_total() -> &'static IntCounter {
    static M: OnceLock<IntCounter> = OnceLock::new();
    M.get_or_init(|| {
        register_int_counter!(Opts::new(
            "micius_chunk_bytes_written_total",
            "Total bytes written to chunk files"
        ))
        .expect("metric registration failed")
    })
}

// ── Query ─────────────────────────────────────────────────────────────────────

pub fn query_duration_seconds() -> &'static Histogram {
    static M: OnceLock<Histogram> = OnceLock::new();
    M.get_or_init(|| {
        register_histogram!(
            HistogramOpts::new(
                "micius_query_duration_seconds",
                "End-to-end query latency in seconds"
            )
            .buckets(vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0])
        )
        .expect("metric registration failed")
    })
}

/// Track how many chunks survived each pruning stage.
/// Labels: "total" (before pruning) | "after_pruning" (after time+stats filter).
/// The gap between "total" and "after_pruning" is the optimisation story:
/// chunks eliminated in-memory before any disk I/O.
pub fn query_chunks_scanned() -> &'static HistogramVec {
    static M: OnceLock<HistogramVec> = OnceLock::new();
    M.get_or_init(|| {
        register_histogram_vec!(
            HistogramOpts::new(
                "micius_query_chunks_scanned",
                "Chunk files evaluated at each query pruning stage"
            )
            .buckets(vec![0.0, 1.0, 5.0, 10.0, 50.0, 100.0, 500.0]),
            &["stage"]
        )
        .expect("metric registration failed")
    })
}

// ── Index ─────────────────────────────────────────────────────────────────────

pub fn index_series_count() -> &'static IntGauge {
    static M: OnceLock<IntGauge> = OnceLock::new();
    M.get_or_init(|| {
        register_int_gauge!(Opts::new(
            "micius_index_series_count",
            "Number of distinct time series in the chunk index"
        ))
        .expect("metric registration failed")
    })
}

// ── Compaction ────────────────────────────────────────────────────────────────

pub fn compaction_runs_total() -> &'static IntCounterVec {
    static M: OnceLock<IntCounterVec> = OnceLock::new();
    M.get_or_init(|| {
        register_int_counter_vec!(
            Opts::new("micius_compaction_runs_total", "Total compaction cycles"),
            &["result"]
        )
        .expect("metric registration failed")
    })
}

pub fn compaction_chunks_merged_total() -> &'static IntCounter {
    static M: OnceLock<IntCounter> = OnceLock::new();
    M.get_or_init(|| {
        register_int_counter!(Opts::new(
            "micius_compaction_chunks_merged_total",
            "Total chunk files consumed by compaction"
        ))
        .expect("metric registration failed")
    })
}

// ── HTTP metrics server ───────────────────────────────────────────────────────

async fn metrics_handler() -> impl axum::response::IntoResponse {
    let encoder = prometheus::TextEncoder::new();
    let metric_families = prometheus::gather();
    let mut body = Vec::new();
    let _ = encoder.encode(&metric_families, &mut body);
    (
        [(
            axum::http::header::CONTENT_TYPE,
            encoder.format_type().to_owned(),
        )],
        body,
    )
}

/// Serve Prometheus metrics on `addr` at `/metrics`.
/// Spawn this as a background task — it runs forever.
pub async fn serve(addr: std::net::SocketAddr) -> anyhow::Result<()> {
    let app = axum::Router::new().route("/metrics", axum::routing::get(metrics_handler));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, "Prometheus metrics server listening");
    axum::serve(listener, app).await?;
    Ok(())
}
