#![deny(
    unsafe_code,
    missing_docs,
    bad_style,
    dead_code,
    non_shorthand_field_patterns,
    no_mangle_generic_items,
    overflowing_literals,
    path_statements,
    patterns_in_fns_without_body,
    unconditional_recursion,
    unused_allocation,
    unused_comparisons,
    unused_parens,
    while_true,
    missing_debug_implementations,
    trivial_casts,
    trivial_numeric_casts,
    unused,
    unused_extern_crates,
    unused_import_braces,
    unused_qualifications,
    let_underscore_drop,
    unreachable_pub,
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::panic
)]

//! Micius storage engine — a time-series database kernel built from scratch in Rust.
//!
//! This library provides a durable, high-performance storage engine with:
//! - N-shard write-ahead log (WAL) with group commit (one fsync per batch)
//! - N-shard BTreeMap memtable with periodic flush
//! - Columnar chunk files with delta-encoding and lz4 compression
//! - Inverted tag index for multi-tag query acceleration
//! - Size-tiered compaction for controlled read amplification
//! - gRPC server exposing Append, Query, and Compact RPCs

/// Chunk file storage — columnar layout, delta-encoding, lz4, bloom filters.
pub mod chunk;
/// Background compaction worker — size-tiered merge strategy.
pub mod compaction;
/// Configuration — loads from environment variables.
pub mod config;
/// ChunkIndex — in-memory inverted tag index, time-range pruning, persistence.
pub mod index;
/// Memtable — sharded BTreeMap, flush threshold, watermark tracking.
pub mod memtable;
/// Prometheus metrics — OnceLock counters, axum /metrics endpoint.
pub mod metrics;
/// gRPC server — Append, Query, Compact, Snapshot RPCs with background tasks.
pub mod server;
/// Core types — DataPoint, SeriesKey, SeriesId, ChunkId.
pub mod types;
/// Write-ahead log — segment rotation, group commit, per-shard recovery and GC.
pub mod wal;
/// Protobuf definitions — generated from proto/storage/v1/storage.proto.
pub mod proto {
    /// Storage service protobuf package.
    pub mod storage {
        /// Storage v1 API.
        pub mod v1 {
            #![allow(missing_docs, unused_qualifications, clippy::unwrap_used)]
            tonic::include_proto!("storage.v1");
        }
    }
}
