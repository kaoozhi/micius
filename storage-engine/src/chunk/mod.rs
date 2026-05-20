//! Chunk file storage — columnar binary format with delta-encoding and compression.

/// Binary format constants and helpers — magic bytes, block layout, CRC32.
pub mod format;
/// ChunkReader — decompress and decode chunks, read series by tag filter.
pub mod reader;
/// ChunkWriter — write columnar chunks, delta-encode, lz4, bloom filter.
pub mod writer;
