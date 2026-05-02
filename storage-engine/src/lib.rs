pub mod chunk;
pub mod compaction;
pub mod config;
pub mod metrics;
pub mod index;
pub mod memtable;
pub mod server;
pub mod types;
pub mod wal;
pub mod proto {
    pub mod storage {
        pub mod v1 {
            tonic::include_proto!("storage.v1");
        }
    }
}
