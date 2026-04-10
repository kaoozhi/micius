# Micius — Phase 1: Rust Storage Engine

> **Goal**: A working Rust storage engine that accepts write batches via gRPC, durably
> stores them using a WAL + memtable + chunk file pipeline, serves range queries with
> a three-stage pruning strategy, and recovers correctly after a simulated crash.
>
> **Estimated duration**: 6–8 weeks  
> **Language**: Rust + Tokio async runtime  
> **Output**: `storage-engine/` crate exposing a tonic gRPC server

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Project Setup](#2-project-setup)
3. [Core Types](#3-core-types)
4. [Error Handling Strategy](#4-error-handling-strategy)
5. [WAL — Write-Ahead Log](#5-wal--write-ahead-log)
6. [Memtable](#6-memtable)
7. [Chunk File Format](#7-chunk-file-format)
8. [Chunk Writer](#8-chunk-writer)
9. [Chunk Reader](#9-chunk-reader)
10. [Chunk Index and Tag Inverted Index](#10-chunk-index-and-tag-inverted-index)
11. [Index Persistence and Startup Recovery](#11-index-persistence-and-startup-recovery)
12. [Write Path — End to End](#12-write-path--end-to-end)
13. [Read Path — End to End](#13-read-path--end-to-end)
14. [Compaction Worker](#14-compaction-worker)
15. [Prometheus Metrics](#15-prometheus-metrics)
16. [gRPC Server](#16-grpc-server)
17. [main.rs — Wiring Everything Together](#17-mainrs--wiring-everything-together)
18. [Test Plan](#18-test-plan)

---

## 1. Architecture Overview

### The write path

```
gRPC Append RPC
      │
      ▼
  WalWriter
  (fsync per batch)
      │
      ▼
  Memtable
  (in-memory BTreeMap, sorted by series+time)
      │  when size threshold reached
      ▼
  ChunkWriter
  (flush memtable → immutable .mcz file on disk)
      │
      ▼
  ChunkIndex
  (register new chunk → update time index + tag index + stats)
```

### The read path

```
gRPC Query RPC
      │
      ▼
  ChunkIndex.resolve_series()       ← tag inverted index intersection
      │
      ▼
  ChunkIndex.prune_chunks()         ← stage 1: time range pruning
      │                             ← stage 2: min/max stats pruning
      ▼
  ChunkReader.check_bloom()         ← stage 3: bloom filter check (per file)
      │
      ▼
  ChunkReader.read_series()         ← decompress + decode columns
      │
      ▼
  stream QueryResponse back to caller
```

### Concurrency model

The storage engine runs inside a single Tokio runtime. The key shared state is
protected by two different primitives for different reasons:

- `Arc<Mutex<WalWriter>>` — the WAL writer is protected by a Mutex because
  WAL appends must be serialized. Two concurrent appends interleaving their bytes
  would corrupt the WAL file. Mutex is correct here — there is no read path on
  the WAL at runtime (only during recovery).

- `Arc<RwLock<ChunkIndex>>` — the chunk index is protected by RwLock because
  reads (queries) are far more frequent than writes (memtable flushes,
  compaction). Multiple concurrent queries can hold the read lock simultaneously.
  Only a memtable flush or compaction acquires the write lock.

- `Arc<Mutex<Memtable>>` — the memtable is protected by Mutex because both
  inserts (from the gRPC append handler) and drains (triggering a flush) must
  be atomic. The critical path is: insert point → check should_flush → if true,
  drain atomically and spawn a flush task. Without the Mutex, two concurrent
  appends could both observe should_flush = true and trigger duplicate flushes.

### Why the flush is async and non-blocking

When the memtable reaches its flush threshold, the gRPC handler drains the
memtable synchronously (holding the lock for microseconds) then spawns a
`tokio::spawn` task for the actual disk write. This means:

- The gRPC Append RPC returns to the caller immediately after the WAL fsync
  and memtable insert — it does not wait for the chunk flush to complete.
- The flush happens concurrently with new incoming writes.
- New writes during a flush go into a fresh memtable while the old one is
  being written to disk.

The tradeoff: if the process crashes after the memtable drain but before the
chunk flush completes, the WAL is used to recover those points. This is why
the WAL must be fsynced before the memtable insert — the WAL is the source
of truth for un-flushed data.

---

## 2. Project Setup

### Directory structure

```
storage-engine/
├── build.rs
├── Cargo.toml
├── src/
│   ├── main.rs
│   ├── error.rs          # unified error type
│   ├── types.rs          # DataPoint, SeriesKey, ChunkMeta, ChunkStats
│   ├── config.rs         # StorageConfig (loaded from env vars)
│   ├── wal/
│   │   ├── mod.rs
│   │   ├── writer.rs     # WalWriter — append + fsync + segment rotation
│   │   └── recovery.rs   # recover() — replay WAL on startup
│   ├── memtable/
│   │   └── mod.rs        # Memtable — in-memory buffer
│   ├── chunk/
│   │   ├── mod.rs
│   │   ├── format.rs     # binary layout constants and helpers
│   │   ├── writer.rs     # ChunkWriter — flush memtable to .mcs file
│   │   └── reader.rs     # ChunkReader — decompress and decode columns
│   ├── index/
│   │   ├── mod.rs
│   │   ├── chunk_index.rs  # ChunkIndex — time index + tag index + stats
│   │   └── persistence.rs  # save/load index to/from disk
│   ├── compaction/
│   │   └── mod.rs          # CompactionWorker — background merge task
│   ├── metrics.rs          # Prometheus metrics registry
│   └── server/
│       └── mod.rs          # tonic gRPC server implementation
└── tests/
    ├── wal_test.rs
    ├── memtable_test.rs
    ├── chunk_test.rs
    ├── index_test.rs
    ├── compaction_test.rs
    └── integration_test.rs
```

### Cargo.toml

```toml
[package]
name = "micius-storage"
version = "0.1.0"
edition = "2021"

[dependencies]
# Async runtime
tokio          = { version = "1", features = ["full"] }

# gRPC
tonic          = "0.11"
prost          = "0.12"

# Compression
lz4_flex       = { version = "0.11", features = ["frame"] }

# Checksum
crc32fast      = "1"

# Bloom filter
bloomfilter    = "1"

# Error handling
anyhow         = "1"
thiserror      = "1"

# Serialization (index persistence)
serde          = { version = "1", features = ["derive"] }
serde_json     = "1"

# Observability
tracing              = "0.1"
tracing-subscriber   = { version = "0.3", features = ["env-filter"] }
prometheus           = { version = "0.13", features = ["process"] }

# Utilities
uuid           = { version = "1", features = ["v4"] }
bytes          = "1"

[build-dependencies]
tonic-build = "0.11"
```

### build.rs

```rust
fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(true)
        .build_client(false)
        .out_dir("src/generated")
        .compile(
            &["../proto/storage/v1/storage.proto"],
            &["../proto"],
        )?;
    Ok(())
}
```

### config.rs

```rust
// src/config.rs
//
// All configuration is read from environment variables at startup.
// This makes the storage engine configurable via Docker Compose
// without rebuilding the binary.

pub struct StorageConfig {
    /// Directory for WAL segment files
    pub wal_dir: PathBuf,

    /// Directory for chunk (.mcs) files
    pub chunk_dir: PathBuf,

    /// Path for the persisted chunk index snapshot
    pub index_path: PathBuf,

    /// WAL segment rotates after this many bytes (default 64 MB)
    /// Smaller segments = faster recovery scan but more files
    pub wal_max_segment_bytes: u64,

    /// Memtable flushes to disk after this many bytes (default 32 MB)
    /// Larger threshold = fewer chunk files but more memory usage
    pub memtable_flush_threshold_bytes: usize,

    /// Compaction runs every this many seconds (default 300)
    pub compaction_interval_secs: u64,

    /// Minimum number of same-series chunks to trigger size-tiered compaction
    pub compaction_min_threshold: usize,

    /// Chunks within this size ratio are candidates for merging
    pub compaction_size_ratio: f64,

    /// gRPC server listen address
    pub grpc_addr: String,

    /// Prometheus metrics listen address
    pub metrics_addr: String,
}

impl StorageConfig {
    pub fn from_env() -> Result<Self> {
        Ok(Self {
            wal_dir: env_path("MICIUS_WAL_DIR", "/var/micius/wal"),
            chunk_dir: env_path("MICIUS_CHUNK_DIR", "/var/micius/chunks"),
            index_path: env_path("MICIUS_INDEX_PATH", "/var/micius/index.json"),
            wal_max_segment_bytes: env_u64("MICIUS_WAL_MAX_SEGMENT_MB", 64) * 1024 * 1024,
            memtable_flush_threshold_bytes: env_usize("MICIUS_MEMTABLE_FLUSH_MB", 32) * 1024 * 1024,
            compaction_interval_secs: env_u64("MICIUS_COMPACTION_INTERVAL_SECS", 300),
            compaction_min_threshold: env_usize("MICIUS_COMPACTION_MIN_THRESHOLD", 4),
            compaction_size_ratio: env_f64("MICIUS_COMPACTION_SIZE_RATIO", 1.5),
            grpc_addr: env_string("MICIUS_GRPC_ADDR", "0.0.0.0:50051"),
            metrics_addr: env_string("MICIUS_METRICS_ADDR", "0.0.0.0:9091"),
        })
    }
}
```

---

## 3. Core Types

```rust
// src/types.rs
use std::collections::BTreeMap;
use std::path::PathBuf;

/// A single measurement at a point in time
#[derive(Debug, Clone, PartialEq)]
pub struct DataPoint {
    pub metric_name:  String,
    pub tags:         BTreeMap<String, String>,
    pub timestamp_ns: i64,     // unix nanoseconds
    pub value:        f64,
}

/// Uniquely identifies a time series — the combination of metric name
/// and its complete tag set. Two series with the same metric name but
/// different tags are entirely independent series.
///
/// BTreeMap is used for tags rather than HashMap to guarantee a
/// deterministic byte representation for hashing and bloom filters.
/// HashMap iteration order is not stable across runs.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct SeriesKey {
    pub metric_name: String,
    pub tags:        BTreeMap<String, String>,
}

impl SeriesKey {
    /// Canonical byte representation. Used as the bloom filter key
    /// and as the series directory key inside chunk files.
    ///
    /// Format: "metric_name,tag1=val1,tag2=val2" (tags in sorted order,
    /// guaranteed by BTreeMap).
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut s = self.metric_name.clone();
        for (k, v) in &self.tags {
            s.push(',');
            s.push_str(k);
            s.push('=');
            s.push_str(v);
        }
        s.into_bytes()
    }
}

/// Opaque stable identifier assigned to a SeriesKey on first registration.
/// Used internally to avoid storing full SeriesKey strings in every index
/// data structure.
pub type SeriesId = u64;

/// Opaque identifier for a chunk file. Derived from a timestamp at
/// flush time so chunk IDs sort chronologically.
pub type ChunkId = u64;

/// WAL sequence number — monotonically increasing per batch.
pub type Sequence = u64;

/// Metadata about a chunk file stored in the ChunkIndex.
/// Does not contain the chunk data itself — only enough information
/// to locate and evaluate the chunk during query planning.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ChunkMeta {
    pub chunk_id:      ChunkId,
    pub series_id:     SeriesId,
    pub time_start_ns: i64,
    pub time_end_ns:   i64,
    pub file_path:     PathBuf,
    pub size_bytes:    u64,
    pub entry_count:   u32,
}

/// Per-chunk value statistics used for predicate pushdown.
/// Stored alongside ChunkMeta in the ChunkIndex.
/// Computed during the chunk write from the raw value column.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ChunkStats {
    pub min_value: f64,
    pub max_value: f64,
    pub null_count: u32,     // reserved for future nullable value support
}

impl ChunkStats {
    pub fn from_values(values: &[f64]) -> Self {
        let min = values.iter().cloned().fold(f64::INFINITY, f64::min);
        let max = values.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        Self { min_value: min, max_value: max, null_count: 0 }
    }
}
```

---

## 4. Error Handling Strategy

```rust
// src/error.rs
//
// Use thiserror for the library-facing error type.
// Use anyhow for internal application code where error context
// is more important than matching on specific variants.
//
// Rule: public API (gRPC handlers, ChunkIndex methods) returns
//       Result<T, StorageError>.
//       Internal helpers use anyhow::Result<T>.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("WAL write failed: {0}")]
    WalWriteError(#[from] std::io::Error),

    #[error("WAL entry corrupt at sequence {sequence}: {reason}")]
    WalCorrupt { sequence: u64, reason: String },

    #[error("Chunk write failed for chunk {chunk_id}: {reason}")]
    ChunkWriteError { chunk_id: u64, reason: String },

    #[error("Chunk read failed: {0}")]
    ChunkReadError(String),

    #[error("Decompression failed: {0}")]
    DecompressionFailed(String),

    #[error("Protobuf encode/decode error: {0}")]
    ProtoError(#[from] prost::EncodeError),

    #[error("Series not found: {metric_name}")]
    SeriesNotFound { metric_name: String },

    #[error("Index persistence error: {0}")]
    IndexPersistenceError(String),
}

// Conversion to tonic Status for gRPC error propagation
impl From<StorageError> for tonic::Status {
    fn from(e: StorageError) -> Self {
        match e {
            StorageError::SeriesNotFound { .. } =>
                tonic::Status::not_found(e.to_string()),
            StorageError::WalCorrupt { .. } =>
                tonic::Status::data_loss(e.to_string()),
            _ =>
                tonic::Status::internal(e.to_string()),
        }
    }
}
```

---

## 5. WAL — Write-Ahead Log

### Why a WAL exists and what it guarantees

The WAL provides the durability guarantee: once `append()` returns `Ok`, the
data survives a process crash. Without a WAL, data in the memtable is lost
if the process dies before the memtable flushes to disk.

The WAL is an append-only file. Each call to `append()` adds one entry to
the end of the current segment file and calls `fsync` before returning. The
`fsync` syscall forces the OS to flush its write buffer to the physical
storage medium — without it, the OS may buffer the write in memory and the
"written" data could still be lost on a power failure.

### WAL entry binary format

```
┌─────────────────────────────────────────┐
│ length   : u32 (4 bytes, little-endian) │  length of the payload in bytes
│ checksum : u32 (4 bytes, little-endian) │  CRC32 of the payload bytes
│ payload  : [u8; length]                 │  prost-encoded WalEntry proto
└─────────────────────────────────────────┘
```

The length prefix serves two purposes: it tells the recovery reader how many
bytes to read for the payload, and it detects a torn write — if the file
ends in the middle of a payload (cursor + length > buf.len()), we know the
process died mid-write and we stop recovery at that boundary.

The checksum detects bit-flip corruption — a physical storage error that
silently changes bytes without truncating the file.

### WAL segment rotation

A WAL segment file has a maximum size (`wal_max_segment_bytes`). When the
current segment exceeds this size, `append()` closes the current file and
opens a new segment with an incremented sequence number:
`wal-000001.log`, `wal-000002.log`, etc.

Why rotate? A single ever-growing WAL file makes recovery slow — it must be
read from the beginning every time. With rotation, recovery only needs to
read segments that contain entries newer than the last memtable flush.
After a successful flush, old WAL segments can be deleted (Phase 4).

### WAL proto definition (internal, not the gRPC proto)

```protobuf
// Used only for WAL serialization — not exposed via gRPC
message WalEntry {
  uint64 sequence = 1;
  repeated WalDataPoint points = 2;
}

message WalDataPoint {
  string metric_name  = 1;
  map<string, string> tags = 2;
  int64 timestamp_ns  = 3;
  double value        = 4;
}
```

### WalWriter implementation

```rust
// src/wal/writer.rs
use anyhow::{Context, Result};
use crc32fast::Hasher;
use prost::Message;
use std::path::{Path, PathBuf};
use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncWriteExt;
use crate::types::Sequence;

pub struct WalWriter {
    file:              File,
    current_seq:       Sequence,
    current_size:      u64,
    current_segment:   u32,          // monotonically increasing segment number
    wal_dir:           PathBuf,
    max_segment_bytes: u64,
}

impl WalWriter {
    /// Opens the most recent WAL segment for appending, or creates
    /// the first segment if none exists.
    pub async fn open(wal_dir: &Path, max_segment_bytes: u64) -> Result<Self> {
        tokio::fs::create_dir_all(wal_dir).await?;

        // Find the highest existing segment number
        let segment_number = highest_segment_number(wal_dir).await?.unwrap_or(1);
        let path = segment_path(wal_dir, segment_number);

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await
            .with_context(|| format!("Failed to open WAL segment {:?}", path))?;

        let current_size = file.metadata().await?.len();

        Ok(Self {
            file,
            current_seq: 0,       // will be updated during recovery
            current_size,
            current_segment: segment_number,
            wal_dir: wal_dir.to_path_buf(),
            max_segment_bytes,
        })
    }

    /// Appends a batch of data points to the WAL.
    ///
    /// Steps:
    /// 1. Serialize points to protobuf
    /// 2. Compute CRC32 checksum over the serialized bytes
    /// 3. Write [length][checksum][payload] to the segment file
    /// 4. fsync — blocks until the OS confirms data is on disk
    /// 5. Rotate segment if size threshold exceeded
    ///
    /// Returns the sequence number assigned to this batch.
    pub async fn append(&mut self, points: &[crate::types::DataPoint]) -> Result<Sequence> {
        self.current_seq += 1;

        let entry = crate::wal::proto::WalEntry {
            sequence: self.current_seq,
            points: points.iter().map(encode_point).collect(),
        };

        let payload = entry.encode_to_vec();

        // CRC32 computed over the payload bytes only
        let mut hasher = Hasher::new();
        hasher.update(&payload);
        let checksum = hasher.finalize();

        // Assemble the framed entry: [length u32][checksum u32][payload]
        let payload_len = payload.len() as u32;
        let mut frame = Vec::with_capacity(8 + payload.len());
        frame.extend_from_slice(&payload_len.to_le_bytes());
        frame.extend_from_slice(&checksum.to_le_bytes());
        frame.extend_from_slice(&payload);

        self.file.write_all(&frame).await?;

        // fsync — this is the durability guarantee.
        // sync_data() flushes file data but not metadata (mtime etc.)
        // which is faster than sync_all() and sufficient for WAL integrity.
        self.file.sync_data().await
            .with_context(|| "WAL fsync failed")?;

        self.current_size += frame.len() as u64;

        // Rotate to a new segment if current segment exceeds threshold
        if self.current_size >= self.max_segment_bytes {
            self.rotate().await?;
        }

        Ok(self.current_seq)
    }

    /// Closes the current segment and opens a new one.
    async fn rotate(&mut self) -> Result<()> {
        self.current_segment += 1;
        let path = segment_path(&self.wal_dir, self.current_segment);

        tracing::info!(
            segment = self.current_segment,
            "Rotating WAL segment"
        );

        self.file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await?;

        self.current_size = 0;
        Ok(())
    }

    /// Returns the current WAL sequence number.
    /// Used by the replication layer to track what has been shipped
    /// to follower nodes.
    pub fn current_sequence(&self) -> Sequence {
        self.current_seq
    }

    /// Returns the path of the current WAL segment.
    pub fn current_segment_path(&self) -> PathBuf {
        segment_path(&self.wal_dir, self.current_segment)
    }
}

fn segment_path(dir: &Path, number: u32) -> PathBuf {
    dir.join(format!("wal-{:06}.log", number))
}

async fn highest_segment_number(dir: &Path) -> Result<Option<u32>> {
    let mut entries = tokio::fs::read_dir(dir).await?;
    let mut max: Option<u32> = None;
    while let Some(entry) = entries.next_entry().await? {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with("wal-") && name.ends_with(".log") {
            if let Ok(n) = name[4..10].parse::<u32>() {
                max = Some(max.map_or(n, |m: u32| m.max(n)));
            }
        }
    }
    Ok(max)
}
```

### WAL recovery

```rust
// src/wal/recovery.rs
//
// Recovery is called once at startup before the gRPC server starts
// accepting connections. It replays all WAL entries whose data points
// are not yet reflected in the chunk files on disk.
//
// Recovery stops at:
// - End of the last segment file
// - A torn write (entry length extends beyond file boundary)
// - A checksum mismatch (corrupted entry)
//
// The "last applied sequence" problem:
// After a clean flush, the chunk index records which WAL sequence was
// current at flush time. On recovery we can skip entries with sequence
// <= last_flushed_sequence. In Phase 1 we replay everything (simpler)
// and in Phase 4 we add the optimization.

use anyhow::Result;
use crc32fast::Hasher;
use prost::Message;
use std::path::Path;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use crate::types::DataPoint;

pub struct RecoveryResult {
    pub points: Vec<DataPoint>,
    pub last_sequence: u64,
    pub segments_replayed: u32,
    pub entries_replayed: u64,
    pub torn_write_detected: bool,
}

pub async fn recover(wal_dir: &Path) -> Result<RecoveryResult> {
    let mut result = RecoveryResult {
        points: Vec::new(),
        last_sequence: 0,
        segments_replayed: 0,
        entries_replayed: 0,
        torn_write_detected: false,
    };

    // Collect all WAL segment files sorted by segment number
    let mut segments = collect_segments(wal_dir).await?;
    segments.sort_by_key(|(num, _)| *num);

    for (_, segment_path) in &segments {
        tracing::info!(path = ?segment_path, "Replaying WAL segment");

        let mut file = File::open(segment_path).await?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf).await?;

        let mut cursor = 0usize;

        loop {
            // Need at least 8 bytes for the frame header
            if cursor + 8 > buf.len() {
                break;
            }

            // Read frame header
            let length = u32::from_le_bytes(
                buf[cursor..cursor + 4].try_into().unwrap()
            ) as usize;
            let stored_checksum = u32::from_le_bytes(
                buf[cursor + 4..cursor + 8].try_into().unwrap()
            );
            cursor += 8;

            // Detect torn write — payload extends beyond file end
            if cursor + length > buf.len() {
                tracing::warn!(
                    path = ?segment_path,
                    offset = cursor - 8,
                    "Torn WAL write detected — stopping recovery at this boundary"
                );
                result.torn_write_detected = true;
                break;
            }

            let payload = &buf[cursor..cursor + length];

            // Verify checksum
            let mut hasher = Hasher::new();
            hasher.update(payload);
            let computed_checksum = hasher.finalize();

            if computed_checksum != stored_checksum {
                tracing::warn!(
                    path = ?segment_path,
                    offset = cursor - 8,
                    stored = stored_checksum,
                    computed = computed_checksum,
                    "WAL checksum mismatch — stopping recovery at this boundary"
                );
                result.torn_write_detected = true;
                break;
            }

            // Decode and collect points
            let entry = crate::wal::proto::WalEntry::decode(payload)?;
            result.last_sequence = result.last_sequence.max(entry.sequence);

            for proto_point in entry.points {
                result.points.push(decode_point(proto_point)?);
            }

            result.entries_replayed += 1;
            cursor += length;
        }

        result.segments_replayed += 1;
    }

    tracing::info!(
        segments = result.segments_replayed,
        entries = result.entries_replayed,
        points = result.points.len(),
        last_sequence = result.last_sequence,
        torn_write = result.torn_write_detected,
        "WAL recovery complete"
    );

    Ok(result)
}
```

---

## 6. Memtable

### What the memtable is

The memtable is the in-memory write buffer that sits between the WAL and
the chunk files on disk. Every data point appended to the WAL is also
inserted into the memtable. The memtable accumulates writes until it
reaches its configured size threshold, then flushes — writing all
its contents to a new immutable chunk file and clearing itself.

### Why BTreeMap for internal storage

The memtable uses `BTreeMap<SeriesKey, Vec<(i64, f64)>>` rather than
HashMap for two reasons:

1. **Sorted iteration during flush**: When writing a chunk file, data
   must be written in series order (all points for series A, then all
   points for series B, etc.). BTreeMap provides this for free. HashMap
   would require an explicit sort step.

2. **Binary search for out-of-order inserts**: Each series' point vec
   is maintained in timestamp order. The `binary_search_by_key` call
   handles the rare case of out-of-order points (e.g., late-arriving
   data) without a full sort.

### Double-buffering for concurrent writes during flush

A naive implementation holds the Memtable lock for the entire duration
of the flush (the disk write), which blocks all incoming writes.
The correct approach is double-buffering:

1. Acquire lock, drain the active memtable into a local variable, swap
   in a fresh empty memtable, release lock — all in microseconds.
2. Write the drained data to disk outside the lock — takes milliseconds.

New writes during the flush go into the fresh memtable. The WAL ensures
these writes survive a crash even before they reach a chunk file.

```rust
// src/memtable/mod.rs
use std::collections::BTreeMap;
use crate::types::{DataPoint, SeriesKey};

pub struct Memtable {
    entries:         BTreeMap<SeriesKey, Vec<(i64, f64)>>,
    size_bytes:      usize,
    flush_threshold: usize,
    entry_count:     u64,
}

impl Memtable {
    pub fn new(flush_threshold: usize) -> Self {
        Self {
            entries: BTreeMap::new(),
            size_bytes: 0,
            flush_threshold,
            entry_count: 0,
        }
    }

    /// Insert a data point into the memtable.
    ///
    /// Most inserts are in chronological order — binary_search locates
    /// the insertion position in O(log n) and insert at the end is O(1)
    /// amortized (vec push). Out-of-order inserts are O(n) due to vec
    /// element shifting, but rare in practice.
    pub fn insert(&mut self, point: DataPoint) {
        let key = SeriesKey {
            metric_name: point.metric_name,
            tags: point.tags,
        };

        let vec = self.entries.entry(key).or_default();

        match vec.binary_search_by_key(&point.timestamp_ns, |&(ts, _)| ts) {
            // Duplicate timestamp: overwrite the value.
            // This handles the case where a client retries an append
            // and the same point arrives twice.
            Ok(pos) => {
                vec[pos].1 = point.value;
                // No size increment — replacing not adding
            }
            Err(pos) => {
                vec.insert(pos, (point.timestamp_ns, point.value));
                // Approximate memory cost:
                // (i64 + f64) = 16 bytes per point +
                // amortized overhead for Vec growth and BTreeMap node
                self.size_bytes += 32;
                self.entry_count += 1;
            }
        }
    }

    pub fn should_flush(&self) -> bool {
        self.size_bytes >= self.flush_threshold
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn entry_count(&self) -> u64 {
        self.entry_count
    }

    pub fn size_bytes(&self) -> usize {
        self.size_bytes
    }

    /// Atomically drain the memtable contents for flushing.
    /// Returns the data and resets internal state.
    /// This operation is intentionally cheap — only a pointer swap
    /// and counter reset. The expensive work (disk write) happens
    /// after the lock is released.
    pub fn drain(&mut self) -> BTreeMap<SeriesKey, Vec<(i64, f64)>> {
        self.size_bytes = 0;
        self.entry_count = 0;
        std::mem::take(&mut self.entries)
    }
}
```

---

## 7. Chunk File Format

### Design principles

A chunk file is an immutable, self-describing binary file. "Self-describing"
means the file header and series directory contain all the information needed
to read and validate the file without consulting any external index. This
matters for crash recovery — if the in-memory index is lost, the engine can
rebuild it by scanning the chunk directory.

"Immutable" means a chunk file is never modified after it is written.
Compaction creates new chunk files and deletes old ones — it never updates
an existing file in place. This property makes concurrent reads safe without
any locking on the file itself.

### Full binary layout

```
┌─────────────────────────────────────────────────────┐
│ FILE HEADER — 40 bytes                              │
│                                                     │
│  magic         : u32   = 0x48454C58  ("HELX")       │
│  version       : u8    = 1                          │
│  _padding      : [u8;3]                             │
│  chunk_id      : u64                                │
│  time_start_ns : i64   earliest timestamp in file  │
│  time_end_ns   : i64   latest timestamp in file    │
│  series_count  : u32   number of series in file    │
│  total_entries : u32   total data points across    │
│                        all series                   │
├─────────────────────────────────────────────────────┤
│ SERIES DIRECTORY — series_count entries             │
│                                                     │
│  Per entry:                                         │
│    key_len         : u32  byte length of series key │
│    series_key      : [u8; key_len]  canonical bytes │
│    ts_col_offset   : u64  byte offset from file     │
│                          start to timestamp column  │
│    val_col_offset  : u64  byte offset to value col  │
│    entry_count     : u32  number of points          │
│    min_value       : f64  for predicate pushdown    │
│    max_value       : f64  for predicate pushdown    │
├─────────────────────────────────────────────────────┤
│ COLUMN DATA — interleaved per series                │
│                                                     │
│  For each series (in directory order):              │
│                                                     │
│    TIMESTAMP COLUMN:                                │
│      compressed_len : u32  byte length after lz4   │
│      data           : [u8; compressed_len]          │
│        → lz4-block-compressed delta-encoded i64s   │
│                                                     │
│    VALUE COLUMN:                                    │
│      compressed_len : u32                           │
│      data           : [u8; compressed_len]          │
│        → lz4-block-compressed little-endian f64s   │
│                                                     │
├─────────────────────────────────────────────────────┤
│ FOOTER                                              │
│                                                     │
│  bloom_len    : u32                                 │
│  bloom_data   : [u8; bloom_len]                     │
│    → bloom filter over all series_key bytes in     │
│      this chunk. Used to skip chunks that           │
│      definitely do not contain a queried series.    │
│                                                     │
│  file_checksum : u32                                │
│    → CRC32 of all bytes from start of file header  │
│      to end of bloom_data (exclusive of this field)│
│      Detects chunk file corruption on disk.         │
└─────────────────────────────────────────────────────┘
```

### format.rs — constants and helpers

```rust
// src/chunk/format.rs

pub const MAGIC: u32   = 0x48454C58;    // "HELX" in ASCII
pub const VERSION: u8  = 1;
pub const HEADER_SIZE: usize = 40;

/// Generate a chunk ID from the current timestamp.
/// IDs are monotonically increasing and sort chronologically.
pub fn new_chunk_id() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}

/// Encode a slice of i64 timestamps using delta encoding.
///
/// Delta encoding exploits the fact that consecutive timestamps in a
/// time-series are close together. Instead of storing full nanosecond
/// timestamps (8 bytes each), we store:
///   [first_timestamp, delta1, delta2, ...]
///
/// For 1-second interval data, deltas are ~1_000_000_000 (1 billion).
/// For 1-millisecond data, deltas are ~1_000_000 (1 million).
/// These small values compress dramatically better under lz4 than
/// raw nanosecond timestamps which all start with ~1700000000000000000.
pub fn delta_encode(timestamps: &[i64]) -> Vec<i64> {
    if timestamps.is_empty() { return vec![]; }
    let mut out = Vec::with_capacity(timestamps.len());
    out.push(timestamps[0]);
    for i in 1..timestamps.len() {
        out.push(timestamps[i] - timestamps[i - 1]);
    }
    out
}

pub fn delta_decode(deltas: &[i64]) -> Vec<i64> {
    if deltas.is_empty() { return vec![]; }
    let mut out = Vec::with_capacity(deltas.len());
    out.push(deltas[0]);
    for i in 1..deltas.len() {
        out.push(out[i - 1] + deltas[i]);
    }
    out
}

/// Serialize a slice of i64 to little-endian bytes
pub fn i64_slice_to_bytes(values: &[i64]) -> Vec<u8> {
    values.iter().flat_map(|v| v.to_le_bytes()).collect()
}

/// Serialize a slice of f64 to little-endian bytes
pub fn f64_slice_to_bytes(values: &[f64]) -> Vec<u8> {
    values.iter().flat_map(|v| v.to_le_bytes()).collect()
}

pub fn bytes_to_i64_slice(bytes: &[u8]) -> Vec<i64> {
    bytes.chunks_exact(8)
        .map(|b| i64::from_le_bytes(b.try_into().unwrap()))
        .collect()
}

pub fn bytes_to_f64_slice(bytes: &[u8]) -> Vec<f64> {
    bytes.chunks_exact(8)
        .map(|b| f64::from_le_bytes(b.try_into().unwrap()))
        .collect()
}
```

---

## 8. Chunk Writer

### Responsibility

The ChunkWriter takes a `BTreeMap<SeriesKey, Vec<(i64, f64)>>` — the
drained memtable contents — and writes it to a single immutable `.mcs`
file on disk. It also computes the ChunkMeta and ChunkStats for each
series so the ChunkIndex can be updated after the write.

### Two-pass approach

The chunk writer uses two passes over the data because column offsets in
the series directory must point to the actual byte positions of each column
in the file, but those positions are not known until after the directory
itself has been written. The two-pass approach:

**Pass 1** (in memory): encode and compress all columns. Compute sizes.
Calculate the byte offset for each column relative to the file start.

**Pass 2** (write): assemble the complete buffer — header, directory
with correct offsets, column data, footer.

```rust
// src/chunk/writer.rs
use anyhow::Result;
use bloomfilter::Bloom;
use crc32fast::Hasher as CrcHasher;
use lz4_flex::block::{compress_prepend_size};
use std::collections::BTreeMap;
use std::path::PathBuf;
use crate::chunk::format::*;
use crate::types::{ChunkId, ChunkMeta, ChunkStats, SeriesKey};

pub struct ChunkWriter {
    chunk_dir: PathBuf,
}

/// Result of writing a chunk — one entry per series
pub struct ChunkWriteResult {
    pub chunk_id:  ChunkId,
    pub file_path: PathBuf,
    pub file_size: u64,
    /// Per-series metadata for registering in the ChunkIndex
    pub series_results: Vec<SeriesWriteResult>,
}

pub struct SeriesWriteResult {
    pub series_key: SeriesKey,
    pub meta:       ChunkMeta,
    pub stats:      ChunkStats,
}

impl ChunkWriter {
    pub fn new(chunk_dir: PathBuf) -> Self {
        Self { chunk_dir }
    }

    pub async fn write(
        &self,
        series_data: BTreeMap<SeriesKey, Vec<(i64, f64)>>,
    ) -> Result<ChunkWriteResult> {
        let chunk_id = new_chunk_id();
        let file_path = self.chunk_dir.join(format!("chunk-{:016x}.mcs", chunk_id));

        // ── Pass 1: encode and compress all columns ──────────────────

        struct EncodedSeries {
            key:                SeriesKey,
            entry_count:        u32,
            min_value:          f64,
            max_value:          f64,
            ts_compressed:      Vec<u8>,   // lz4-compressed delta-encoded timestamps
            val_compressed:     Vec<u8>,   // lz4-compressed f64 values
        }

        let mut encoded: Vec<EncodedSeries> = Vec::new();
        let mut bloom = Bloom::new_for_fp_rate(series_data.len().max(1), 0.01);
        let mut global_min_ts = i64::MAX;
        let mut global_max_ts = i64::MIN;
        let mut total_entries: u32 = 0;

        for (key, points) in &series_data {
            bloom.set(&key.to_bytes());

            let (timestamps, values): (Vec<i64>, Vec<f64>) =
                points.iter().copied().unzip();

            let stats = ChunkStats::from_values(&values);

            global_min_ts = global_min_ts.min(*timestamps.first().unwrap_or(&0));
            global_max_ts = global_max_ts.max(*timestamps.last().unwrap_or(&0));
            total_entries += timestamps.len() as u32;

            // Delta-encode timestamps, then serialize to bytes, then compress
            let deltas = delta_encode(&timestamps);
            let ts_bytes = i64_slice_to_bytes(&deltas);
            let ts_compressed = compress_prepend_size(&ts_bytes);

            // Serialize values to bytes, then compress
            let val_bytes = f64_slice_to_bytes(&values);
            let val_compressed = compress_prepend_size(&val_bytes);

            encoded.push(EncodedSeries {
                key: key.clone(),
                entry_count: timestamps.len() as u32,
                min_value: stats.min_value,
                max_value: stats.max_value,
                ts_compressed,
                val_compressed,
            });
        }

        // ── Pass 2: compute byte offsets ──────────────────────────────
        //
        // Calculate the byte offset where each series' columns will be
        // written so we can fill the series directory correctly.
        //
        // Layout:
        //   HEADER_SIZE
        //   + series directory bytes (computed below)
        //   + column data for series 0 (ts then val)
        //   + column data for series 1
        //   + ...

        // Compute series directory size
        // Per entry: u32(key_len) + key_bytes + u64(ts_offset) + u64(val_offset)
        //            + u32(entry_count) + f64(min) + f64(max)
        //          = 4 + key_len + 8 + 8 + 4 + 8 + 8
        //          = 40 + key_len
        let dir_size: usize = encoded.iter()
            .map(|s| 40 + s.key.to_bytes().len())
            .sum();

        let mut current_offset = HEADER_SIZE + dir_size;
        let mut offsets: Vec<(u64, u64)> = Vec::new(); // (ts_offset, val_offset)

        for s in &encoded {
            let ts_offset = current_offset as u64;
            current_offset += 4 + s.ts_compressed.len();  // u32 len prefix + data
            let val_offset = current_offset as u64;
            current_offset += 4 + s.val_compressed.len();
            offsets.push((ts_offset, val_offset));
        }

        // ── Assemble the complete file buffer ─────────────────────────

        let mut buf: Vec<u8> = Vec::new();
        let mut crc = CrcHasher::new();

        // Header
        let series_count = encoded.len() as u32;
        let header_bytes = build_header(
            chunk_id, global_min_ts, global_max_ts, series_count, total_entries
        );
        buf.extend_from_slice(&header_bytes);

        // Series directory
        for (i, s) in encoded.iter().enumerate() {
            let key_bytes = s.key.to_bytes();
            let (ts_off, val_off) = offsets[i];

            buf.extend_from_slice(&(key_bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(&key_bytes);
            buf.extend_from_slice(&ts_off.to_le_bytes());
            buf.extend_from_slice(&val_off.to_le_bytes());
            buf.extend_from_slice(&s.entry_count.to_le_bytes());
            buf.extend_from_slice(&s.min_value.to_le_bytes());
            buf.extend_from_slice(&s.max_value.to_le_bytes());
        }

        // Column data
        for s in &encoded {
            buf.extend_from_slice(&(s.ts_compressed.len() as u32).to_le_bytes());
            buf.extend_from_slice(&s.ts_compressed);
            buf.extend_from_slice(&(s.val_compressed.len() as u32).to_le_bytes());
            buf.extend_from_slice(&s.val_compressed);
        }

        // Footer — bloom filter
        let bloom_bytes = bloom.bitmap();
        buf.extend_from_slice(&(bloom_bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(bloom_bytes);

        // Footer — CRC32 over everything written so far
        crc.update(&buf);
        let checksum = crc.finalize();
        buf.extend_from_slice(&checksum.to_le_bytes());

        // ── Write to disk ─────────────────────────────────────────────
        tokio::fs::write(&file_path, &buf).await?;

        let file_size = buf.len() as u64;

        // ── Build results for ChunkIndex registration ─────────────────
        let series_results = encoded.into_iter().map(|s| {
            let stats = ChunkStats {
                min_value: s.min_value,
                max_value: s.max_value,
                null_count: 0,
            };
            let meta = ChunkMeta {
                chunk_id,
                series_id: 0,   // assigned by ChunkIndex.register()
                time_start_ns: global_min_ts,
                time_end_ns: global_max_ts,
                file_path: file_path.clone(),
                size_bytes: file_size,
                entry_count: s.entry_count,
            };
            SeriesWriteResult { series_key: s.key, meta, stats }
        }).collect();

        Ok(ChunkWriteResult {
            chunk_id,
            file_path,
            file_size,
            series_results,
        })
    }
}
```

---

## 9. Chunk Reader

### Responsibility

The ChunkReader opens a `.mcs` file and extracts data points for a
specific series within a time range. It is the only component that reads
chunk files from disk during query execution.

### Three-stage read optimization

Before decompressing any column data, the reader applies two fast checks:

1. **Bloom filter check** — reads only the file footer to check if the
   queried series key is in the bloom filter. If the bloom filter returns
   false (definitively absent), the reader returns immediately with zero
   disk I/O beyond the footer read.

2. **Series directory scan** — scans the series directory to find the
   queried series. If the series key is not in the directory (bloom false
   positive), returns immediately.

3. **Column read** — only if both checks pass, seeks to the timestamp and
   value column offsets and decompresses the data.

```rust
// src/chunk/reader.rs
use anyhow::{bail, Result};
use bloomfilter::Bloom;
use lz4_flex::block::decompress_size_prepended;
use std::path::Path;
use crate::chunk::format::*;
use crate::types::{DataPoint, SeriesKey};

pub struct ChunkReader;

impl ChunkReader {
    pub fn new() -> Self { Self }

    /// Check the bloom filter without reading any column data.
    /// Returns false if the series is DEFINITELY absent from this chunk.
    /// Returns true if the series MAY be present (requires full read to confirm).
    pub async fn check_bloom(
        &self,
        chunk_path: &Path,
        series_key: &SeriesKey,
    ) -> Result<bool> {
        let buf = tokio::fs::read(chunk_path).await?;
        let bloom = read_bloom_from_footer(&buf)?;
        Ok(bloom.check(&series_key.to_bytes()))
    }

    /// Read all data points for a specific series within a time range.
    ///
    /// Returns an empty vec if the series is not present in this chunk
    /// (bloom false positive). Never returns an error in this case —
    /// absence is not an error.
    pub async fn read_series(
        &self,
        chunk_path: &Path,
        series_key: &SeriesKey,
        time_start_ns: i64,
        time_end_ns: i64,
    ) -> Result<Vec<DataPoint>> {
        let buf = tokio::fs::read(chunk_path).await?;

        // Validate magic bytes
        let magic = u32::from_le_bytes(buf[0..4].try_into()?);
        if magic != MAGIC {
            bail!("Invalid chunk magic: expected 0x{:08X}, got 0x{:08X}", MAGIC, magic);
        }

        // Validate file checksum
        validate_checksum(&buf)?;

        // Find the series in the directory
        let Some(dir_entry) = find_series_in_directory(&buf, series_key)? else {
            return Ok(vec![]);   // bloom false positive — series not in chunk
        };

        // Read and decompress timestamp column
        let ts_offset = dir_entry.ts_col_offset as usize;
        let ts_len = u32::from_le_bytes(buf[ts_offset..ts_offset+4].try_into()?) as usize;
        let ts_compressed = &buf[ts_offset + 4..ts_offset + 4 + ts_len];
        let ts_bytes = decompress_size_prepended(ts_compressed)
            .map_err(|e| anyhow::anyhow!("Timestamp decompression failed: {}", e))?;
        let deltas = bytes_to_i64_slice(&ts_bytes);
        let timestamps = delta_decode(&deltas);

        // Read and decompress value column
        let val_offset = dir_entry.val_col_offset as usize;
        let val_len = u32::from_le_bytes(buf[val_offset..val_offset+4].try_into()?) as usize;
        let val_compressed = &buf[val_offset + 4..val_offset + 4 + val_len];
        let val_bytes = decompress_size_prepended(val_compressed)
            .map_err(|e| anyhow::anyhow!("Value decompression failed: {}", e))?;
        let values = bytes_to_f64_slice(&val_bytes);

        // Apply time range filter and reconstruct DataPoints
        let points: Vec<DataPoint> = timestamps.into_iter()
            .zip(values.into_iter())
            .filter(|(ts, _)| *ts >= time_start_ns && *ts <= time_end_ns)
            .map(|(ts, val)| DataPoint {
                metric_name: series_key.metric_name.clone(),
                tags:        series_key.tags.clone(),
                timestamp_ns: ts,
                value:        val,
            })
            .collect();

        Ok(points)
    }
}

struct DirectoryEntry {
    ts_col_offset:  u64,
    val_col_offset: u64,
    entry_count:    u32,
}

fn find_series_in_directory(
    buf: &[u8],
    series_key: &SeriesKey,
) -> Result<Option<DirectoryEntry>> {
    let series_count = u32::from_le_bytes(buf[32..36].try_into()?) as usize;
    let target_key_bytes = series_key.to_bytes();

    let mut cursor = HEADER_SIZE;

    for _ in 0..series_count {
        let key_len = u32::from_le_bytes(buf[cursor..cursor+4].try_into()?) as usize;
        cursor += 4;

        let key_bytes = &buf[cursor..cursor + key_len];
        cursor += key_len;

        let ts_offset  = u64::from_le_bytes(buf[cursor..cursor+8].try_into()?);
        cursor += 8;
        let val_offset = u64::from_le_bytes(buf[cursor..cursor+8].try_into()?);
        cursor += 8;
        let entry_count = u32::from_le_bytes(buf[cursor..cursor+4].try_into()?);
        cursor += 4;
        cursor += 16;    // skip min_value (f64) + max_value (f64)

        if key_bytes == target_key_bytes {
            return Ok(Some(DirectoryEntry {
                ts_col_offset: ts_offset,
                val_col_offset: val_offset,
                entry_count,
            }));
        }
    }

    Ok(None)
}

fn read_bloom_from_footer(buf: &[u8]) -> Result<Bloom<Vec<u8>>> {
    // Footer is at end of file: [bloom_len u32][bloom_data][checksum u32]
    // Work backwards from end of file
    let checksum_offset = buf.len() - 4;
    let bloom_len = u32::from_le_bytes(
        buf[checksum_offset - 4..checksum_offset].try_into()?
    ) as usize;
    let bloom_data = &buf[checksum_offset - 4 - bloom_len..checksum_offset - 4];
    // Reconstruct Bloom filter from raw bitmap bytes
    // bloomfilter crate: Bloom::from_existing(bitmap, m, k, sip_keys)
    // For Phase 1: read bloom_len bytes and perform check via Bloom::from_existing
    // See bloomfilter crate docs for exact reconstruction API
    todo!("reconstruct bloom filter from bitmap bytes")
}

fn validate_checksum(buf: &[u8]) -> Result<()> {
    let stored = u32::from_le_bytes(buf[buf.len()-4..].try_into()?);
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&buf[..buf.len()-4]);
    let computed = hasher.finalize();
    if stored != computed {
        bail!("Chunk file checksum mismatch: stored={}, computed={}", stored, computed);
    }
    Ok(())
}
```

---

## 10. Chunk Index and Tag Inverted Index

### Why the index is in-memory

The ChunkIndex is an in-memory data structure. It is rebuilt from the
chunk files on disk at startup (index persistence is covered in section 11).
Keeping it in memory makes all query planning operations sub-millisecond —
no disk I/O is needed to determine which chunks to read.

### The three data structures inside ChunkIndex

**series_registry** — `HashMap<SeriesKey, SeriesId>`

Maps a full SeriesKey to a compact numeric SeriesId. SeriesId is used
everywhere else in the index to avoid storing large SeriesKey structs
repeatedly. Typical SeriesKey is 20-100 bytes; SeriesId is 8 bytes.

**time_index** — `HashMap<SeriesId, BTreeMap<i64, ChunkMeta>>`

For each series, a sorted map from chunk start time to chunk metadata.
BTreeMap is used (not HashMap) because chunk lookup by time range uses
`range(..=time_end_ns)` which requires sorted order.

**tag_index** — `HashMap<(String, String), HashSet<SeriesId>>`

Inverted index for tag-based series resolution. Maps each (tag_key, tag_value)
pair to the set of series that have that tag. Multi-tag queries intersect
the sets for each required tag. Set intersection is O(min(|A|, |B|)) using
HashSet — much faster than scanning all series.

Example: query for `{service: payments, env: prod}`:
- Look up tag_index[("service", "payments")] → {1, 3, 7, 9}
- Look up tag_index[("env", "prod")]          → {1, 2, 7, 11}
- Intersect                                  → {1, 7}
- These are the series IDs that match both tags

```rust
// src/index/chunk_index.rs
use std::collections::{BTreeMap, HashMap, HashSet};
use crate::types::*;

pub struct ChunkIndex {
    series_registry: HashMap<SeriesKey, SeriesId>,
    time_index:      HashMap<SeriesId, BTreeMap<i64, ChunkMeta>>,
    tag_index:       HashMap<(String, String), HashSet<SeriesId>>,
    chunk_stats:     HashMap<ChunkId, ChunkStats>,
    next_series_id:  SeriesId,
}

impl ChunkIndex {
    pub fn new() -> Self {
        Self {
            series_registry: HashMap::new(),
            time_index:      HashMap::new(),
            tag_index:       HashMap::new(),
            chunk_stats:     HashMap::new(),
            next_series_id:  1,
        }
    }

    /// Register a new chunk after a successful memtable flush.
    /// Called once per series per flush.
    pub fn register(
        &mut self,
        series_key: &SeriesKey,
        mut meta: ChunkMeta,
        stats: ChunkStats,
    ) -> SeriesId {
        // Get or create a SeriesId for this key
        let series_id = if let Some(&id) = self.series_registry.get(series_key) {
            id
        } else {
            let id = self.next_series_id;
            self.next_series_id += 1;

            self.series_registry.insert(series_key.clone(), id);

            // Register all tag pairs in the inverted index.
            // This is done once per series — subsequent chunks for the
            // same series reuse the existing tag_index entries.
            for (k, v) in &series_key.tags {
                self.tag_index
                    .entry((k.clone(), v.clone()))
                    .or_default()
                    .insert(id);
            }

            id
        };

        meta.series_id = series_id;

        self.time_index
            .entry(series_id)
            .or_default()
            .insert(meta.time_start_ns, meta.clone());

        self.chunk_stats.insert(meta.chunk_id, stats);

        series_id
    }

    /// Remove a chunk from the index — called after compaction deletes old chunks.
    pub fn deregister(&mut self, series_id: SeriesId, chunk_id: ChunkId, time_start_ns: i64) {
        if let Some(time_map) = self.time_index.get_mut(&series_id) {
            time_map.remove(&time_start_ns);
        }
        self.chunk_stats.remove(&chunk_id);
    }

    /// Resolve which series IDs match the given metric name and tag filters.
    ///
    /// Uses the tag inverted index to intersect sets efficiently.
    /// Falls back to full scan only when tag_filters is empty.
    pub fn resolve_series(
        &self,
        metric: &str,
        tag_filters: &HashMap<String, String>,
    ) -> Vec<SeriesId> {
        if tag_filters.is_empty() {
            // No tag filters — return all series for this metric
            return self.series_registry.iter()
                .filter(|(k, _)| k.metric_name == metric)
                .map(|(_, &id)| id)
                .collect();
        }

        // Start with the smallest set (most selective tag) for efficiency.
        // Sort tag filters by estimated cardinality — smallest set first.
        // In Phase 1 we use a simple approach: start with any tag and
        // intersect the rest.
        let mut filters_iter = tag_filters.iter();
        let (first_key, first_val) = filters_iter.next().unwrap();

        let mut candidate_ids: HashSet<SeriesId> = self.tag_index
            .get(&(first_key.clone(), first_val.clone()))
            .cloned()
            .unwrap_or_default();

        for (tag_key, tag_val) in filters_iter {
            let matching = self.tag_index
                .get(&(tag_key.clone(), tag_val.clone()))
                .map(|s| s.as_ref())
                .unwrap_or(&HashSet::new());

            candidate_ids.retain(|id| matching.contains(id));

            // Early exit if intersection is already empty
            if candidate_ids.is_empty() { break; }
        }

        // Filter remaining candidates by metric name
        candidate_ids.into_iter()
            .filter(|id| {
                self.series_registry.iter()
                    .any(|(k, sid)| sid == id && k.metric_name == metric)
            })
            .collect()
    }

    /// Find chunks to read for a given series and time range.
    /// Applies three stages of pruning in order of cost:
    ///
    /// Stage 1 — Time range pruning (pure in-memory BTreeMap range scan)
    ///   Eliminates chunks whose time range doesn't overlap the query window.
    ///   Zero disk I/O. Always applied.
    ///
    /// Stage 2 — Min/max statistics pruning (in-memory HashMap lookup)
    ///   Eliminates chunks where max_value < threshold (for GT predicates)
    ///   or min_value > threshold (for LT predicates).
    ///   Zero disk I/O. Applied only when query has a value predicate.
    ///
    /// Stage 3 — Bloom filter (disk footer read — see ChunkReader)
    ///   Applied by ChunkReader.check_bloom() before reading column data.
    ///   Not applied here — ChunkIndex has no file I/O.
    pub fn prune_chunks(
        &self,
        series_id: SeriesId,
        time_start_ns: i64,
        time_end_ns: i64,
        predicate: Option<&ValuePredicate>,
    ) -> Vec<&ChunkMeta> {
        let Some(time_map) = self.time_index.get(&series_id) else {
            return vec![];
        };

        time_map
            // BTreeMap range: all chunks with start_time <= query_end
            .range(..=time_end_ns)
            // Stage 1: keep only chunks whose end overlaps the query start
            .filter(|(_, meta)| meta.time_end_ns >= time_start_ns)
            // Stage 2: stats-based predicate pushdown
            .filter(|(_, meta)| {
                predicate.map_or(true, |p| {
                    let stats = match self.chunk_stats.get(&meta.chunk_id) {
                        Some(s) => s,
                        None => return true,   // no stats — must read
                    };
                    p.matches(stats.min_value, stats.max_value)
                })
            })
            .map(|(_, meta)| meta)
            .collect()
    }

    pub fn series_count(&self) -> usize {
        self.series_registry.len()
    }

    pub fn chunk_count(&self) -> usize {
        self.chunk_stats.len()
    }
}

/// Value predicate for statistics-based chunk pruning.
/// Applied during prune_chunks() using per-chunk min/max stats.
pub enum ValuePredicate {
    GreaterThan(f64),
    LessThan(f64),
    Between(f64, f64),
}

impl ValuePredicate {
    pub fn matches(&self, min_val: f64, max_val: f64) -> bool {
        match self {
            Self::GreaterThan(t)    => max_val > *t,
            Self::LessThan(t)      => min_val < *t,
            Self::Between(lo, hi)  => min_val <= *hi && max_val >= *lo,
        }
    }
}
```

---

## 11. Index Persistence and Startup Recovery

### Why the index needs to be persisted

The ChunkIndex is in-memory. If the process restarts, the index must be
rebuilt. Without persistence, startup requires scanning every chunk file
on disk to rebuild the index — which can take minutes if there are
thousands of chunks.

With index persistence, startup is fast: load the snapshot from disk,
then replay only the WAL entries that arrived after the last snapshot.

### Two-step startup sequence

```
1. Load index snapshot from disk (if exists)
2. Scan chunk directory for any .mcs files not in the snapshot
   (handles the case where the index snapshot is older than the chunks)
3. Replay WAL — insert recovered points into a fresh memtable
4. Start gRPC server — now accepting connections
```

```rust
// src/index/persistence.rs
use anyhow::Result;
use std::path::Path;
use crate::index::chunk_index::ChunkIndex;

/// Snapshot format is JSON for Phase 1 — simple and debuggable.
/// In a production system this would be a compact binary format.
#[derive(serde::Serialize, serde::Deserialize)]
struct IndexSnapshot {
    version: u32,
    last_wal_sequence: u64,
    series_registry: Vec<(SeriesKey, SeriesId)>,
    chunk_metas: Vec<ChunkMeta>,
    chunk_stats: Vec<(ChunkId, ChunkStats)>,
}

pub async fn save_index(
    index: &ChunkIndex,
    path: &Path,
    last_wal_sequence: u64,
) -> Result<()> {
    let snapshot = index.to_snapshot(last_wal_sequence);
    let json = serde_json::to_vec_pretty(&snapshot)?;
    // Write to a temp file then atomically rename — prevents partial writes
    let tmp_path = path.with_extension("tmp");
    tokio::fs::write(&tmp_path, &json).await?;
    tokio::fs::rename(&tmp_path, path).await?;
    Ok(())
}

pub async fn load_index(path: &Path) -> Result<Option<(ChunkIndex, u64)>> {
    if !path.exists() {
        return Ok(None);
    }
    let json = tokio::fs::read(path).await?;
    let snapshot: IndexSnapshot = serde_json::from_slice(&json)?;
    let (index, last_seq) = ChunkIndex::from_snapshot(snapshot);
    Ok(Some((index, last_seq)))
}
```

---

## 12. Write Path — End to End

The full sequence for a single `Append` RPC:

```
1. gRPC handler receives AppendRequest
2. Decode proto DataPoints to internal DataPoint structs
3. Acquire WAL lock
4. WalWriter.append():
   a. Serialize points to WalEntry proto
   b. Compute CRC32 checksum
   c. Write [length][checksum][payload] to segment file
   d. fsync — blocks until data is on disk
   e. Increment sequence counter
   f. Rotate segment if size threshold exceeded
5. Release WAL lock
6. Acquire Memtable lock
7. Insert each point into Memtable (binary search, sorted insertion)
8. Check should_flush():
   If true:
     a. Drain memtable (swap in empty memtable, take the data)
     b. Release Memtable lock
     c. tokio::spawn(flush_task):
        i.  ChunkWriter.write(drained_data) → ChunkWriteResult
        ii. Acquire ChunkIndex write lock
        iii. For each series: ChunkIndex.register(meta, stats)
        iv. Release ChunkIndex write lock
        v.  Increment flush_total counter
   If false:
     b. Release Memtable lock
9. Return AppendResponse { sequence }
```

The critical invariant: **step 4 (WAL fsync) must complete before step 7
(memtable insert).** If the process crashes between WAL write and memtable
insert, the WAL entry exists and will be replayed on next startup. If the
process crashes between memtable insert and flush, the WAL replay will
re-insert the points. The system is correct in both cases.

---

## 13. Read Path — End to End

The full sequence for a single `Query` RPC:

```
1. gRPC handler receives QueryRequest
2. Parse metric_name, tag_filters, time_start_ns, time_end_ns
3. Acquire ChunkIndex read lock (allows concurrent queries)
4. ChunkIndex.resolve_series(metric, tag_filters) → Vec<SeriesId>
5. For each SeriesId:
   a. ChunkIndex.prune_chunks(series_id, start, end, predicate)
      → Vec<&ChunkMeta>  (stage 1: time, stage 2: stats)
6. Release ChunkIndex read lock
7. For each surviving ChunkMeta:
   a. ChunkReader.check_bloom(path, series_key)
      → false: skip this chunk (stage 3: bloom filter)
      → true:  proceed
   b. ChunkReader.read_series(path, series_key, start, end)
      → Vec<DataPoint>
8. Merge results from all chunks (sort by timestamp)
9. Stream QueryResponse messages back to caller
```

Also check the memtable for recent points not yet flushed:

```
10. Acquire Memtable read lock
11. Read points for the queried series from memtable
    (only needed for time ranges that overlap the memtable's time range)
12. Release Memtable lock
13. Merge memtable points with chunk points
14. Stream complete result
```

This memtable read ensures that points written in the last N seconds
(before the next flush) are visible to queries. Without it, queries
would have a blind spot for the most recent data.

---

## 14. Compaction Worker

### What compaction does and why it matters

Without compaction, each memtable flush produces a new small chunk file.
After hours of operation at high ingestion rates, you accumulate thousands
of small chunk files. This degrades query performance because each query
must open and decompress many small files instead of a few large ones.

Compaction merges small chunk files into larger ones, reducing file count
and improving read efficiency. It runs as a background Tokio task on a
configurable interval without blocking the write or read paths.

### Size-tiered compaction strategy

Size-tiered compaction groups chunks by size and merges chunks of similar
size together. The logic:

1. For each series, look at all its chunk files sorted by size.
2. Find groups of chunks where the largest is within `size_ratio` of
   the smallest (e.g., ratio 1.5 means chunks between 10 MB and 15 MB
   are in the same group).
3. If a group has at least `min_threshold` chunks, merge them.
4. Merging: read all chunks in the group, combine their data points,
   sort by timestamp, write a new chunk, update the index, delete the
   old files.

### The atomic index update

The critical correctness requirement: there must never be a moment where
a query can neither find the old chunks (deleted) nor the new chunk
(not yet in index). The safe sequence:

```
1. Write new merged chunk to disk
2. Acquire ChunkIndex write lock
3. Register new chunk in index
4. Remove old chunks from index
5. Release ChunkIndex write lock
6. Delete old chunk files from disk
```

Step 6 (file deletion) happens after the index update. If the process
crashes between steps 5 and 6, the old files are orphaned on disk but
the index already points to the new chunk — queries are correct. The
orphaned files are cleaned up on next startup.

```rust
// src/compaction/mod.rs
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::{interval, Duration};
use crate::chunk::{writer::ChunkWriter, reader::ChunkReader};
use crate::index::chunk_index::ChunkIndex;
use crate::types::{SeriesId, ChunkMeta};

pub struct CompactionWorker {
    index:              Arc<RwLock<ChunkIndex>>,
    writer:             Arc<ChunkWriter>,
    interval_secs:      u64,
    min_threshold:      usize,     // minimum chunks per series to trigger
    size_ratio:         f64,       // max/min size ratio within a merge group
}

impl CompactionWorker {
    pub async fn run(self) {
        let mut ticker = interval(Duration::from_secs(self.interval_secs));
        loop {
            ticker.tick().await;
            tracing::debug!("Compaction cycle starting");
            if let Err(e) = self.compact_once().await {
                tracing::error!(error = %e, "Compaction cycle failed");
                // Do not stop the worker on error — log and retry next cycle
            }
        }
    }

    async fn compact_once(&self) -> anyhow::Result<()> {
        // Step 1: identify merge candidates under read lock
        let candidates = {
            let index = self.index.read().await;
            self.find_merge_candidates(&index)
        };

        if candidates.is_empty() {
            tracing::debug!("No compaction candidates found");
            return Ok(());
        }

        tracing::info!(groups = candidates.len(), "Compaction: merging chunk groups");

        for group in candidates {
            self.merge_group(group).await?;
        }

        Ok(())
    }

    /// Identify groups of same-series chunks that are candidates for merging.
    fn find_merge_candidates(&self, index: &ChunkIndex) -> Vec<MergeGroup> {
        // Implementation: iterate over all series in time_index,
        // group chunks by size, return groups with >= min_threshold members
        // For Phase 1: simplified version — merge all chunks for a series
        // if there are >= min_threshold of them
        vec![]   // placeholder — implement during development
    }

    async fn merge_group(&self, group: MergeGroup) -> anyhow::Result<()> {
        // 1. Read all chunks in the group
        // 2. Merge data points (deduplicate by timestamp, sort by time)
        // 3. Write merged chunk
        // 4. Atomic index update (register new, deregister old)
        // 5. Delete old files
        Ok(())   // placeholder
    }
}

struct MergeGroup {
    series_id: SeriesId,
    chunks: Vec<ChunkMeta>,
}
```

---

## 15. Prometheus Metrics

Every component exposes metrics. The metrics are served on a separate
HTTP port (`MICIUS_METRICS_ADDR`) so Prometheus can scrape them
independently of the gRPC port.

```rust
// src/metrics.rs
use prometheus::{
    register_histogram_vec, register_int_counter_vec, register_int_gauge,
    HistogramVec, IntCounterVec, IntGauge,
};

lazy_static::lazy_static! {
    // WAL
    pub static ref WAL_APPEND_DURATION_SECONDS: HistogramVec =
        register_histogram_vec!(
            "micius_wal_append_duration_seconds",
            "Time spent appending and fsyncing a WAL entry",
            &["result"],      // "ok" | "error"
            vec![0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1]
        ).unwrap();

    pub static ref WAL_ENTRIES_TOTAL: IntCounterVec =
        register_int_counter_vec!(
            "micius_wal_entries_total",
            "Total WAL entries written",
            &["result"]
        ).unwrap();

    // Memtable
    pub static ref MEMTABLE_SIZE_BYTES: IntGauge =
        register_int_gauge!(
            "micius_memtable_size_bytes",
            "Current memtable size in bytes"
        ).unwrap();

    pub static ref MEMTABLE_FLUSH_TOTAL: IntCounterVec =
        register_int_counter_vec!(
            "micius_memtable_flush_total",
            "Total memtable flushes",
            &["result"]
        ).unwrap();

    pub static ref MEMTABLE_FLUSH_DURATION_SECONDS: HistogramVec =
        register_histogram_vec!(
            "micius_memtable_flush_duration_seconds",
            "Time to flush memtable to chunk file",
            &[],
            vec![0.01, 0.05, 0.1, 0.5, 1.0, 5.0]
        ).unwrap();

    // Chunk store
    pub static ref CHUNK_FILES_TOTAL: IntGauge =
        register_int_gauge!(
            "micius_chunk_files_total",
            "Total chunk files on disk"
        ).unwrap();

    pub static ref CHUNK_WRITE_BYTES_TOTAL: IntCounterVec =
        register_int_counter_vec!(
            "micius_chunk_write_bytes_total",
            "Total bytes written to chunk files",
            &[]
        ).unwrap();

    // Query
    pub static ref QUERY_DURATION_SECONDS: HistogramVec =
        register_histogram_vec!(
            "micius_query_duration_seconds",
            "Time to execute a query end to end",
            &["aggregation"],
            vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0]
        ).unwrap();

    pub static ref QUERY_CHUNKS_CONSIDERED: HistogramVec =
        register_histogram_vec!(
            "micius_query_chunks_considered",
            "Chunks evaluated during query planning",
            &["stage"],    // "total" | "after_time" | "after_stats" | "after_bloom"
            vec![1.0, 5.0, 10.0, 50.0, 100.0, 500.0]
        ).unwrap();

    // Compaction
    pub static ref COMPACTION_RUNS_TOTAL: IntCounterVec =
        register_int_counter_vec!(
            "micius_compaction_runs_total",
            "Total compaction cycles",
            &["result"]
        ).unwrap();

    pub static ref COMPACTION_CHUNKS_MERGED_TOTAL: IntCounterVec =
        register_int_counter_vec!(
            "micius_compaction_chunks_merged_total",
            "Total chunk files eliminated by compaction",
            &[]
        ).unwrap();

    // Index
    pub static ref INDEX_SERIES_COUNT: IntGauge =
        register_int_gauge!(
            "micius_index_series_count",
            "Number of distinct time series in the index"
        ).unwrap();
}
```

The `QUERY_CHUNKS_CONSIDERED` metric with the `stage` label is the most
important metric for demonstrating query optimization. It lets you plot
in Grafana: for a given query, how many chunks were evaluated at each
pruning stage versus how many were actually read. A well-optimized query
should show: 100 chunks total → 30 after time pruning → 10 after stats
pruning → 2 after bloom filter → 2 read from disk. That 98% reduction
in disk I/O is your query optimization story made visible.

---

## 16. gRPC Server

```rust
// src/server/mod.rs
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tonic::{Request, Response, Status};
use tokio_stream::wrappers::ReceiverStream;

use crate::types::*;
use crate::wal::writer::WalWriter;
use crate::memtable::Memtable;
use crate::chunk::{writer::ChunkWriter, reader::ChunkReader};
use crate::index::chunk_index::ChunkIndex;
use crate::metrics::*;

// Generated by tonic from storage.proto
use crate::generated::storage_service_server::StorageService;
use crate::generated::*;

pub struct StorageServer {
    pub wal:      Arc<Mutex<WalWriter>>,
    pub memtable: Arc<Mutex<Memtable>>,
    pub index:    Arc<RwLock<ChunkIndex>>,
    pub writer:   Arc<ChunkWriter>,
    pub reader:   Arc<ChunkReader>,
}

#[tonic::async_trait]
impl StorageService for StorageServer {

    async fn append(
        &self,
        request: Request<AppendRequest>,
    ) -> Result<Response<AppendResponse>, Status> {
        let timer = WAL_APPEND_DURATION_SECONDS
            .with_label_values(&["ok"])
            .start_timer();

        let points = request.into_inner().points
            .into_iter()
            .map(proto_to_datapoint)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        // Step 1: WAL — must complete before memtable insert
        let seq = {
            let mut wal = self.wal.lock().await;
            wal.append(&points).await
                .map_err(|e| Status::internal(format!("WAL error: {}", e)))?
        };
        WAL_ENTRIES_TOTAL.with_label_values(&["ok"]).inc();

        // Step 2: Memtable insert
        let should_flush = {
            let mut mem = self.memtable.lock().await;
            for point in &points {
                mem.insert(point.clone());
            }
            MEMTABLE_SIZE_BYTES.set(mem.size_bytes() as i64);
            mem.should_flush()
        };

        // Step 3: Trigger async flush if needed
        if should_flush {
            let drained = {
                let mut mem = self.memtable.lock().await;
                mem.drain()
            };

            let index  = Arc::clone(&self.index);
            let writer = Arc::clone(&self.writer);

            tokio::spawn(async move {
                let flush_start = std::time::Instant::now();

                match writer.write(drained).await {
                    Ok(result) => {
                        let mut idx = index.write().await;
                        for sr in result.series_results {
                            idx.register(&sr.series_key, sr.meta, sr.stats);
                        }
                        INDEX_SERIES_COUNT.set(idx.series_count() as i64);
                        CHUNK_FILES_TOTAL.inc();
                        MEMTABLE_FLUSH_TOTAL.with_label_values(&["ok"]).inc();
                        MEMTABLE_FLUSH_DURATION_SECONDS
                            .with_label_values(&[])
                            .observe(flush_start.elapsed().as_secs_f64());
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "Memtable flush failed");
                        MEMTABLE_FLUSH_TOTAL.with_label_values(&["error"]).inc();
                    }
                }
            });
        }

        timer.observe_duration();
        Ok(Response::new(AppendResponse { sequence: seq }))
    }

    type QueryStream = ReceiverStream<Result<QueryResponse, Status>>;

    async fn query(
        &self,
        request: Request<QueryRequest>,
    ) -> Result<Response<Self::QueryStream>, Status> {
        let req = request.into_inner();
        let (tx, rx) = tokio::sync::mpsc::channel(256);

        let index  = Arc::clone(&self.index);
        let reader = Arc::clone(&self.reader);
        let memtable = Arc::clone(&self.memtable);

        tokio::spawn(async move {
            let query_start = std::time::Instant::now();

            let tag_filters: std::collections::HashMap<String, String> =
                req.tag_filters.clone();

            // Resolve series and prune chunks under read lock
            let chunk_metas = {
                let idx = index.read().await;
                let series_ids = idx.resolve_series(&req.metric_name, &tag_filters);

                let mut all_metas = Vec::new();
                let mut total_chunks = 0u64;
                let mut after_time = 0u64;
                let mut after_stats = 0u64;

                for series_id in series_ids {
                    let metas = idx.prune_chunks(
                        series_id,
                        req.time_start_ns,
                        req.time_end_ns,
                        None,   // predicate pushdown added in Phase 3
                    );
                    total_chunks += metas.len() as u64;
                    after_time += metas.len() as u64;
                    after_stats += metas.len() as u64;   // no stats pruning yet
                    all_metas.extend(metas.into_iter().cloned());
                }

                QUERY_CHUNKS_CONSIDERED
                    .with_label_values(&["total"])
                    .observe(total_chunks as f64);
                QUERY_CHUNKS_CONSIDERED
                    .with_label_values(&["after_time"])
                    .observe(after_time as f64);

                all_metas
            };

            // Read surviving chunks from disk
            let series_key = SeriesKey {
                metric_name: req.metric_name.clone(),
                tags: tag_filters,
            };

            let mut after_bloom = 0u64;

            for meta in &chunk_metas {
                // Stage 3: bloom filter check
                match reader.check_bloom(&meta.file_path, &series_key).await {
                    Ok(false) => continue,    // definitely absent
                    Ok(true)  => {}
                    Err(e)    => {
                        tracing::warn!(error = %e, "Bloom filter read failed");
                        // On error, proceed with reading — safe to skip bloom
                    }
                }

                after_bloom += 1;

                match reader.read_series(
                    &meta.file_path,
                    &series_key,
                    req.time_start_ns,
                    req.time_end_ns,
                ).await {
                    Ok(points) => {
                        for point in points {
                            let response = QueryResponse {
                                timestamp_ns: point.timestamp_ns,
                                value: point.value,
                            };
                            if tx.send(Ok(response)).await.is_err() {
                                return;    // client disconnected
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!(error = %e, path = ?meta.file_path, "Chunk read failed");
                        let _ = tx.send(Err(Status::internal(e.to_string()))).await;
                        return;
                    }
                }
            }

            // Also read from memtable for recent un-flushed points
            {
                let mem = memtable.lock().await;
                // memtable read implementation here
            }

            QUERY_CHUNKS_CONSIDERED
                .with_label_values(&["after_bloom"])
                .observe(after_bloom as f64);

            QUERY_DURATION_SECONDS
                .with_label_values(&["none"])
                .observe(query_start.elapsed().as_secs_f64());
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn compact(
        &self,
        _request: Request<CompactRequest>,
    ) -> Result<Response<CompactResponse>, Status> {
        // In Phase 1: trigger compaction immediately rather than waiting
        // for the background interval. Useful for testing.
        // Full implementation deferred to compaction worker.
        Ok(Response::new(CompactResponse { chunks_merged: 0 }))
    }

    async fn snapshot(
        &self,
        _request: Request<SnapshotRequest>,
    ) -> Result<Response<SnapshotResponse>, Status> {
        // Persist index to disk on demand — used by replication layer in Phase 4
        Ok(Response::new(SnapshotResponse {
            snapshot_path: String::new(),
        }))
    }
}
```

---

## 17. main.rs — Wiring Everything Together

```rust
// src/main.rs
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tonic::transport::Server;

mod config;
mod error;
mod types;
mod wal;
mod memtable;
mod chunk;
mod index;
mod compaction;
mod metrics;
mod server;

mod generated {
    tonic::include_proto!("storage.v1");
}

use generated::storage_service_server::StorageServiceServer;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("micius_storage=info".parse()?)
        )
        .init();

    let config = config::StorageConfig::from_env()?;

    // Ensure data directories exist
    tokio::fs::create_dir_all(&config.wal_dir).await?;
    tokio::fs::create_dir_all(&config.chunk_dir).await?;

    // ── Step 1: Startup recovery ──────────────────────────────────────

    // Load persisted index snapshot (if any)
    let (mut index, last_flushed_seq) =
        index::persistence::load_index(&config.index_path)
            .await?
            .unwrap_or_else(|| (index::chunk_index::ChunkIndex::new(), 0));

    tracing::info!(
        series = index.series_count(),
        chunks = index.chunk_count(),
        last_flushed_seq,
        "Index snapshot loaded"
    );

    // Replay WAL to recover un-flushed points
    let recovery = wal::recovery::recover(&config.wal_dir).await?;
    tracing::info!(
        points = recovery.points.len(),
        entries = recovery.entries_replayed,
        "WAL recovery complete"
    );

    // Insert recovered points into a fresh memtable, then flush immediately
    // if there are any recovered points
    let mut memtable = memtable::Memtable::new(config.memtable_flush_threshold_bytes);
    let chunk_writer = chunk::writer::ChunkWriter::new(config.chunk_dir.clone());

    if !recovery.points.is_empty() {
        for point in recovery.points {
            memtable.insert(point);
        }
        let drained = memtable.drain();
        let result = chunk_writer.write(drained).await?;
        for sr in result.series_results {
            index.register(&sr.series_key, sr.meta, sr.stats);
        }
        tracing::info!("Recovered points flushed to chunk file");
    }

    // ── Step 2: Initialize shared state ──────────────────────────────

    let wal_writer = wal::writer::WalWriter::open(
        &config.wal_dir,
        config.wal_max_segment_bytes,
    ).await?;

    // Restore WAL sequence counter from recovery
    // (WalWriter needs to know the last sequence to continue from)
    let wal = Arc::new(Mutex::new(wal_writer));
    let mem = Arc::new(Mutex::new(memtable));
    let idx = Arc::new(RwLock::new(index));
    let writer = Arc::new(chunk_writer);
    let reader = Arc::new(chunk::reader::ChunkReader::new());

    // ── Step 3: Start background workers ─────────────────────────────

    let compaction_worker = compaction::CompactionWorker {
        index:          Arc::clone(&idx),
        writer:         Arc::clone(&writer),
        interval_secs:  config.compaction_interval_secs,
        min_threshold:  config.compaction_min_threshold,
        size_ratio:     config.compaction_size_ratio,
    };

    tokio::spawn(async move {
        compaction_worker.run().await;
    });

    // Periodically persist the index snapshot to disk
    {
        let idx_clone = Arc::clone(&idx);
        let index_path = config.index_path.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(
                std::time::Duration::from_secs(60)
            );
            loop {
                ticker.tick().await;
                let index = idx_clone.read().await;
                if let Err(e) = index::persistence::save_index(
                    &index, &index_path, 0   // TODO: pass last WAL seq
                ).await {
                    tracing::error!(error = %e, "Index snapshot failed");
                }
            }
        });
    }

    // Start Prometheus metrics HTTP server
    {
        let metrics_addr = config.metrics_addr.clone();
        tokio::spawn(async move {
            serve_metrics(metrics_addr).await;
        });
    }

    // ── Step 4: Start gRPC server ─────────────────────────────────────

    let storage_server = server::StorageServer {
        wal:      Arc::clone(&wal),
        memtable: Arc::clone(&mem),
        index:    Arc::clone(&idx),
        writer:   Arc::clone(&writer),
        reader:   Arc::clone(&reader),
    };

    let grpc_addr = config.grpc_addr.parse()?;

    tracing::info!(addr = %grpc_addr, "Storage engine gRPC server starting");

    Server::builder()
        .add_service(StorageServiceServer::new(storage_server))
        .serve(grpc_addr)
        .await?;

    Ok(())
}

async fn serve_metrics(addr: String) {
    use prometheus::Encoder;
    let addr: std::net::SocketAddr = addr.parse().unwrap();
    // Serve /metrics endpoint using a minimal HTTP server
    // hyper or tiny_http both work for this purpose
}
```

---

## 18. Test Plan

All tests must pass before moving to Phase 2. Run with:

```bash
cargo test                          # all unit tests
cargo test --test wal_test          # specific integration test
cargo test -- --nocapture           # show println! output
```

### WAL tests — `tests/wal_test.rs`

| Test | What it verifies |
|------|-----------------|
| `test_append_and_recover` | Write 3 batches, drop writer, recover — all points returned |
| `test_torn_write_stops_recovery` | Truncate last entry mid-payload — recovery returns only complete entries |
| `test_checksum_mismatch_stops_recovery` | Flip a bit in a payload — recovery stops at that entry |
| `test_segment_rotation` | Write until max_segment_bytes exceeded — new segment file created |
| `test_recovery_across_segments` | Write across two segments — all entries recovered |
| `test_duplicate_timestamp_overwrite` | Same timestamp twice — last value wins |

### Memtable tests — `tests/memtable_test.rs`

| Test | What it verifies |
|------|-----------------|
| `test_insert_and_drain` | Insert points, drain — all points returned in sorted order |
| `test_out_of_order_insert` | Insert timestamps out of order — drain returns sorted |
| `test_flush_threshold` | Insert until should_flush() returns true |
| `test_double_buffer_pattern` | Drain then insert — new inserts go into fresh state |
| `test_size_tracking` | Insert N points — size_bytes() returns expected value |

### Chunk tests — `tests/chunk_test.rs`

| Test | What it verifies |
|------|-----------------|
| `test_write_and_read_single_series` | Write one series, read back all points — values match |
| `test_write_and_read_multi_series` | Write 10 series, query each — correct isolation |
| `test_time_range_filter` | Write 1000 points, query middle 100 — correct boundary |
| `test_bloom_filter_negative` | Query for absent series — check_bloom returns false |
| `test_bloom_filter_false_positive_rate` | Write 1000 series, query 1000 absent — FP rate < 1% |
| `test_delta_encode_decode_roundtrip` | encode then decode — original values recovered exactly |
| `test_lz4_compress_decompress_roundtrip` | compress then decompress — byte-perfect recovery |
| `test_checksum_corruption_detected` | Flip bit in chunk file — read_series returns error |
| `test_magic_bytes_validated` | Write invalid magic — read_series returns error |

### Index tests — `tests/index_test.rs`

| Test | What it verifies |
|------|-----------------|
| `test_single_tag_resolution` | Register series with tag service:payments — resolved by that tag |
| `test_multi_tag_intersection` | Register 10 series, query with 2 tags — only matching series returned |
| `test_no_matching_tags` | Query with tag that no series has — empty result |
| `test_time_range_pruning` | Register 5 chunks at different times — only overlapping chunks returned |
| `test_stats_predicate_gt` | Register chunk with max=50, query GT 100 — chunk pruned |
| `test_stats_predicate_between` | Register chunk with min=10 max=20, query Between 25 50 — pruned |
| `test_register_deregister` | Register then deregister chunk — no longer appears in prune_chunks |
| `test_index_persistence_roundtrip` | Save index, load index — identical series and chunk data |

### Compaction tests — `tests/compaction_test.rs`

| Test | What it verifies |
|------|-----------------|
| `test_compacted_data_queryable` | Compact 4 chunks → 1 chunk — all original data still queryable |
| `test_old_chunks_deleted` | After compaction — original chunk files no longer on disk |
| `test_compaction_below_threshold` | 2 chunks (below min_threshold=4) — no compaction triggered |
| `test_atomic_index_update` | Kill process after new chunk written but before old removed — query still correct on restart |

### Integration tests — `tests/integration_test.rs`

| Test | What it verifies |
|------|-----------------|
| `test_full_write_read_cycle` | Write via gRPC Append, query via gRPC Query — data matches |
| `test_crash_recovery_full` | Write, kill process, restart, query — all committed data present |
| `test_concurrent_writes` | 10 concurrent gRPC clients writing — no data loss, no panics |
| `test_concurrent_reads_during_flush` | Write and query concurrently — reads never blocked by flushes |
| `test_query_chunk_pruning_metrics` | Query with selective tag filter — Prometheus shows pruning savings |

---

## Phase 1 Completion Gate

Phase 2 starts only when all of the following are true:

1. All tests in the test plan above pass with `cargo test`
2. `cargo clippy -- -D warnings` produces zero warnings
3. WAL recovery test demonstrates zero data loss after `kill -9` on the process
4. Query of 1 million points completes in under 100ms with correct results
5. Prometheus metrics endpoint at `:9091/metrics` shows all defined metrics
6. Docker Compose `make up` starts the storage engine and Grafana shows metrics
```
