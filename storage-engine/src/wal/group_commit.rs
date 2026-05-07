use crate::{types::*, wal::writer::WalWriter};
use anyhow::Result;
use std::path::PathBuf;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::{mpsc, oneshot};

pub enum WalMessage {
    Append {
        points: Arc<Vec<DataPoint>>,
        reply: oneshot::Sender<Result<Sequence>>,
    },
    Rotate {
        reply: oneshot::Sender<Result<Vec<PathBuf>>>,
    },
}

#[derive(Clone)]
pub struct WalSender {
    tx: mpsc::Sender<WalMessage>,
    /// Last sequence number successfully fsynced. Updated atomically after
    /// every batch fsync so snapshot() can read it without a round-trip message.
    last_seq: Arc<AtomicU64>,
}

impl WalSender {
    pub async fn append(&self, points: Arc<Vec<DataPoint>>) -> Result<Sequence> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(WalMessage::Append {
                points,
                reply: reply_tx,
            })
            .await
            .map_err(|_| anyhow::anyhow!("WAL task shut down"))?;
        reply_rx
            .await
            .map_err(|_| anyhow::anyhow!("WAL task dropped reply"))?
    }

    pub async fn rotate_and_drain(&self) -> Result<Vec<PathBuf>> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(WalMessage::Rotate { reply: reply_tx })
            .await
            .map_err(|_| anyhow::anyhow!("WAL task shut down"))?;
        reply_rx
            .await
            .map_err(|_| anyhow::anyhow!("WAL task dropped reply"))?
    }

    pub fn spawn(writer: WalWriter, capacity: usize, max_batch: usize) -> Self {
        let last_seq = Arc::new(AtomicU64::new(writer.current_seq));
        let (tx, rx) = mpsc::channel(capacity);
        tokio::spawn(wal_task(rx, writer, max_batch, Arc::clone(&last_seq)));
        Self { tx, last_seq }
    }

    /// Returns the last sequence number committed to durable storage.
    /// Used by snapshot() to record the WAL watermark without a channel round-trip.
    pub fn current_sequence(&self) -> Sequence {
        self.last_seq.load(Ordering::Acquire)
    }
}

async fn wal_task(
    mut rx: mpsc::Receiver<WalMessage>,
    mut writer: WalWriter,
    max_batch: usize,
    last_seq: Arc<AtomicU64>,
) {
    loop {
        // ── 1. Park until at least one message arrives ─────────────────────
        let mut batch = Vec::new(); // ← declare inside loop
        match rx.recv().await {
            Some(msg) => batch.push(msg),
            None => return,
        }

        // ── 2. Drain backlog non-blocking ──────────────────────────────────
        while batch.len() < max_batch {
            match rx.try_recv() {
                Ok(msg) => batch.push(msg),
                Err(TryRecvError::Empty | TryRecvError::Disconnected) => break,
            }
        }

        let mut replies: Vec<oneshot::Sender<Result<Sequence>>> = Vec::new();
        let mut frames: Vec<u8> = Vec::new();
        let mut seqs: Vec<Sequence> = Vec::new();
        let mut rotate_reply: Option<oneshot::Sender<Result<Vec<PathBuf>>>> = None;

        // gather all Append into batch, and keep at most one Rotate
        for msg in batch {
            match msg {
                WalMessage::Append { points, reply } => {
                    writer.current_seq += 1;
                    let seq = writer.current_seq;
                    let frame = WalWriter::encode_frame(seq, &points);

                    frames.extend_from_slice(&frame);

                    seqs.push(seq);
                    replies.push(reply);
                }
                WalMessage::Rotate { reply } => {
                    if rotate_reply.is_none() {
                        rotate_reply = Some(reply);
                    } else {
                        let _ = reply.send(Err(anyhow::anyhow!("concurrent Rotate in same batch")));
                    }
                }
            }
        }

        // one write + one fsync for the whole batch
        let io_result = async {
            if !frames.is_empty() {
                writer.file.write_all(&frames).await?;
                writer.file.sync_all().await?;
                writer.current_size += frames.len() as u64;
            }
            Ok::<(), anyhow::Error>(())
        }
        .await;

        match io_result {
            Ok(()) => {
                // Publish the highest committed seq before replying to callers
                // so snapshot() always sees a seq that is already on disk.
                if let Some(&max_seq) = seqs.last() {
                    last_seq.store(max_seq, Ordering::Release);
                }
                for (seq, reply) in seqs.into_iter().zip(replies) {
                    let _ = reply.send(Ok(seq));
                }
                if writer.current_size >= writer.max_segment_bytes
                    && let Err(e) = writer.rotate().await
                {
                    tracing::error!(error = %e, "WAL segment rotation failed");
                }

                // handle explicit Rotate request
                if let Some(rotate) = rotate_reply {
                    let result = writer
                        .rotate()
                        .await
                        .map(|_| writer.drain_completed_before(u64::MAX));
                    let _ = rotate.send(result);
                }
            }

            Err(e) => {
                let msg = e.to_string();
                for reply in replies {
                    let _ = reply.send(Err(anyhow::anyhow!("{}", msg)));
                }
                // Also fail the checkpoint caller
                if let Some(rotate) = rotate_reply {
                    let _ = rotate.send(Err(anyhow::anyhow!("WAL write failed: {}", msg)));
                }
            }
        }
    }
}
