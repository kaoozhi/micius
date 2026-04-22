pub mod chunk;
pub mod config;
pub mod index;
pub mod memtable;
pub mod types;
pub mod wal;
pub mod proto {
    pub mod storage {
        pub mod v1 {
            tonic::include_proto!("storage.v1");
        }
    }
}
