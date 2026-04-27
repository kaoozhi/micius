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
use crate::types::*;
use crate::{
    chunk::reader::ChunkReader,
    chunk::writer::ChunkWriter,
    compaction::CompactionWorker,
    index::chunk_index::ChunkIndex,
    memtable::Memtable,
    proto::storage::v1::{
        AppendRequest, AppendResponse, CompactRequest, CompactResponse, QueryRequest,
        QueryResponse, SnapshotRequest, SnapshotResponse,
        storage_service_server::{StorageService, StorageServiceServer},
    },
    wal::writer::WalWriter,
};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

pub struct StorageServer {
    pub wal: Arc<Mutex<WalWriter>>,
    pub memtable: Arc<Mutex<Memtable>>,
    pub index: Arc<RwLock<ChunkIndex>>,
    pub chunk_writer: Arc<ChunkWriter>,
    pub chunk_reader: Arc<ChunkReader>,
    pub compaction_worker: Arc<Mutex<CompactionWorker>>,
}

#[tonic::async_trait]
impl StorageService for StorageServer {
    type QueryStream = ReceiverStream<Result<QueryResponse, Status>>;

    async fn append(
        &self,
        request: Request<AppendRequest>,
    ) -> Result<Response<AppendResponse>, Status> {
        todo!()
    }

    async fn compact(
        &self,
        request: Request<CompactRequest>,
    ) -> Result<Response<CompactResponse>, Status> {
        todo!()
    }

    async fn query(
        &self,
        request: Request<QueryRequest>,
    ) -> Result<Response<Self::QueryStream>, Status> {
        todo!()
    }

    async fn snapshot(
        &self,
        request: Request<SnapshotRequest>,
    ) -> Result<Response<SnapshotResponse>, Status> {
        todo!()
    }
}
