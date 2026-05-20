//! Write-ahead log — per-shard segment files with group commit, CRC32, and crash recovery.

/// Channel-based group commit — one fsync per batch of concurrent Append RPCs.
pub mod group_commit;
/// Protobuf structs for WAL frame serialization.
pub mod proto;
/// Crash recovery — replay WAL segments and detect torn writes.
pub mod recovery;
/// WAL segment writer — append, rotate, and GC completed segments.
pub mod writer;
