// #![allow(unused)]

use anyhow::Result;
use std::path::PathBuf;

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

    /// mpsc channel capacity between gRPC handlers and the WAL task (default 1024).
    /// Backpressure kicks in when the WAL task falls behind this many pending requests.
    /// Increase if you see `WAL task shut down` errors under very high write bursts.
    pub wal_channel_capacity: usize,

    /// Maximum number of Append requests batched into a single fsync (default 256).
    /// Higher values increase throughput under load; lower values reduce tail latency
    /// during disk stalls by capping how many callers share one failed fsync.
    pub wal_max_batch: usize,

    /// Memtable flushes to disk after this many bytes (default 32 MB)
    /// Larger threshold = fewer chunk files but more memory usage
    pub memtable_flush_threshold_bytes: usize,

    /// Memtable shards number
    pub memtable_shards: usize,

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
    pub fn load() -> Result<Self> {
        let memtable_shards = env_usize("MICIUS_MEMTABLE_SHARDS", 16);
        anyhow::ensure!(
            memtable_shards >= 1 && memtable_shards.is_power_of_two(),
            "MICIUS_MEMTABLE_SHARDS must be a power of 2 >= 1, got {memtable_shards}"
        );
        Ok(Self {
            wal_dir: env_path("MICIUS_WAL_DIR", "/var/micius/data/wal"),
            chunk_dir: env_path("MICIUS_CHUNK_DIR", "/var/micius/data/chunks"),
            index_path: env_path("MICIUS_INDEX_PATH", "/var/micius/data/index.bin"),
            wal_max_segment_bytes: env_u64("MICIUS_WAL_MAX_SEGMENT_MB", 64) * 1024 * 1024,
            wal_channel_capacity: env_usize("MICIUS_WAL_CHANNEL_CAPACITY", 1024),
            wal_max_batch: env_usize("MICIUS_WAL_MAX_BATCH", 256),
            memtable_flush_threshold_bytes: env_usize("MICIUS_MEMTABLE_FLUSH_MB", 32) * 1024 * 1024,
            memtable_shards,
            compaction_interval_secs: env_u64("MICIUS_COMPACTION_INTERVAL_SECS", 300),
            compaction_min_threshold: env_usize("MICIUS_COMPACTION_MIN_THRESHOLD", 4),
            compaction_size_ratio: env_f64("MICIUS_COMPACTION_SIZE_RATIO", 1.5),
            grpc_addr: env_string("MICIUS_GRPC_ADDR", "0.0.0.0:50051"),
            metrics_addr: env_string("MICIUS_METRICS_ADDR", "0.0.0.0:9091"),
        })
    }

    pub async fn ensure_dirs(&self) -> Result<()> {
        tokio::fs::create_dir_all(&self.wal_dir).await?;
        tokio::fs::create_dir_all(&self.chunk_dir).await?;
        // index_path's parent directory
        if let Some(parent) = self.index_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        Ok(())
    }
}

fn env_path(key: &str, default: &str) -> PathBuf {
    std::env::var(key)
        .unwrap_or_else(|_| default.to_string())
        .into()
}

fn env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_f64(key: &str, default: f64) -> f64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_string(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_defaults() {
        // Unset all vars to test fallback defaults
        unsafe {
            std::env::remove_var("MICIUS_WAL_DIR");
            std::env::remove_var("MICIUS_GRPC_ADDR");
        }

        let config = StorageConfig::load().unwrap();

        assert_eq!(config.wal_dir, PathBuf::from("/var/micius/data/wal"));
        assert_eq!(config.grpc_addr, "0.0.0.0:50051");
        assert_eq!(config.wal_max_segment_bytes, 64 * 1024 * 1024);
    }

    #[test]
    fn test_config_from_env() {
        unsafe {
            std::env::set_var("MICIUS_WAL_DIR", "/tmp/test-wal");
            std::env::set_var("MICIUS_MEMTABLE_FLUSH_MB", "16");
        }

        let config = StorageConfig::load().unwrap();

        assert_eq!(config.wal_dir, PathBuf::from("/tmp/test-wal"));
        assert_eq!(config.memtable_flush_threshold_bytes, 16 * 1024 * 1024);

        // Cleanup
        unsafe {
            std::env::remove_var("MICIUS_WAL_DIR");
            std::env::remove_var("MICIUS_MEMTABLE_FLUSH_MB");
        }
    }
}
