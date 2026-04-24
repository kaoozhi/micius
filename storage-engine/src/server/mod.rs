// // Capture sequence at drain time — entries up to this point are in the flush
// let flush_seq = wal.current_sequence();

// // ... drain memtable → ChunkWriter.write() → index.register() ...

// // Safe to delete after chunk file is fsync'd
// let paths = wal.drain_completed_before(flush_seq);
// for path in paths {
//     if let Err(e) = tokio::fs::remove_file(&path).await {
//         // Log and continue — file will be cleaned up on next startup scan
//         tracing::warn!(path = ?path, error = %e, "failed to delete WAL segment");
//     }
// }
