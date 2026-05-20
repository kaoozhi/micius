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

/// Messages sent from RPC handlers to the per-shard WAL background task.
#[derive(Debug)]
pub enum WalMessage {
    /// Write a batch of points and reply with the assigned sequence number.
    Append {
        /// Points to append.
        points: Arc<Vec<DataPoint>>,
        /// Channel to send the sequence number (or error) back to the caller.
        reply: oneshot::Sender<Result<Sequence>>,
    },
    /// Force-rotate the current segment and drain completed segments.
    Rotate {
        /// Channel to return the list of rotated segment paths.
        reply: oneshot::Sender<Result<Vec<PathBuf>>>,
    },
    /// Drain segments whose max sequence is ≤ `seq` (WAL GC after flush).
    DrainBefore {
        /// Sequence threshold — segments at or below this are eligible for deletion.
        seq: Sequence,
        /// Channel to return the list of deleted segment paths.
        reply: oneshot::Sender<Result<Vec<PathBuf>>>,
    },
}

/// Client handle for a per-shard WAL background task.
#[derive(Debug, Clone)]
pub struct WalSender {
    tx: mpsc::Sender<WalMessage>,
    /// Last sequence number successfully fsynced. Updated atomically after
    /// every batch fsync so snapshot() can read it without a round-trip message.
    last_seq: Arc<AtomicU64>,
}

impl WalSender {
    /// Sends points to the WAL task and waits for the fsync'd sequence number.
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

    /// Rotates the current WAL segment and returns paths of all completed segments.
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

    /// Instructs the WAL task to delete segments whose max sequence is ≤ `seq`.
    pub async fn drain_completed_before(&self, seq: Sequence) -> Result<Vec<PathBuf>> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(WalMessage::DrainBefore {
                seq,
                reply: reply_tx,
            })
            .await
            .map_err(|_| anyhow::anyhow!("WAL task shut down"))?;
        reply_rx
            .await
            .map_err(|_| anyhow::anyhow!("WAL task dropped reply"))?
    }

    /// Spawns the WAL background task and returns the sender handle.
    pub fn spawn(
        writer: WalWriter,
        capacity: usize,
        max_batch: usize,
        batch_delay_us: u64,
    ) -> Self {
        let last_seq = Arc::new(AtomicU64::new(writer.current_seq));
        let (tx, rx) = mpsc::channel(capacity);
        tokio::spawn(wal_task(
            rx,
            writer,
            max_batch,
            batch_delay_us,
            Arc::clone(&last_seq),
        ));
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
    batch_delay_us: u64,
    last_seq: Arc<AtomicU64>,
) {
    loop {
        // ── 1. Park until at least one message arrives ─────────────────────
        let mut batch = Vec::new(); // ← declare inside loop
        match rx.recv().await {
            Some(msg) => batch.push(msg),
            None => return,
        }

        // ── 2. Optional collect window ─────────────────────────────────────
        // On fast storage the natural batch window is too short to accumulate
        // many requests. A non-zero delay extends it at the cost of added latency.
        if batch_delay_us > 0 {
            tokio::time::sleep(tokio::time::Duration::from_micros(batch_delay_us)).await;
        }

        // ── 3. Drain backlog non-blocking ──────────────────────────────────
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
        let mut drain_replies: Vec<(Sequence, oneshot::Sender<Result<Vec<PathBuf>>>)> = Vec::new();

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
                        reply
                            .send(Err(anyhow::anyhow!("concurrent Rotate in same batch")))
                            .ok();
                    }
                }
                WalMessage::DrainBefore { seq, reply } => {
                    drain_replies.push((seq, reply));
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
                    reply.send(Ok(seq)).ok();
                }
                if writer.current_size >= writer.max_segment_bytes
                    && let Err(e) = writer.rotate().await
                {
                    tracing::error!(error = %e, "WAL segment rotation failed");
                }

                // handle explicit periodic sweep request
                if !drain_replies.is_empty() {
                    let max_seq = drain_replies.iter().map(|(seq, _)| *seq).max().unwrap_or(0);
                    let paths = writer.drain_completed_before(max_seq);
                    for (_, reply) in drain_replies {
                        reply.send(Ok(paths.clone())).ok();
                    }
                }

                // handle explicit Rotate request
                if let Some(rotate) = rotate_reply {
                    let result = writer
                        .rotate()
                        .await
                        .map(|_| writer.drain_completed_before(u64::MAX));
                    rotate.send(result).ok();
                }
            }

            Err(e) => {
                let msg = e.to_string();
                for reply in replies {
                    reply.send(Err(anyhow::anyhow!("{}", msg))).ok();
                }
                for (_, reply) in drain_replies {
                    reply
                        .send(Err(anyhow::anyhow!("WAL write failed: {}", msg)))
                        .ok();
                }
                if let Some(rotate) = rotate_reply {
                    rotate
                        .send(Err(anyhow::anyhow!("WAL write failed: {}", msg)))
                        .ok();
                }
            }
        }
    }
}
