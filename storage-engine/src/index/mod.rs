//! ChunkIndex — in-memory inverted tag index, time-range pruning, and snapshot persistence.

/// In-memory chunk index — inverted tag index, time-range and stats-based pruning.
pub mod chunk_index;
/// Snapshot persistence — save and load the ChunkIndex to/from disk (bincode).
pub mod persistence;
