use anyhow::{Context, Result};
use std::path::{Path, PathBuf};
use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncWriteExt;

use crate::types::{DataPoint, Sequence};
use crate::wal::proto::WalEntry;
use prost::Message;

pub struct WalWriter {
    pub file: File,
    pub current_seq: Sequence,
    pub current_size: u64,
    current_segment: u64,
    wal_dir: PathBuf,
    pub max_segment_bytes: u64,
    completed_segments: Vec<(u64, Sequence)>,
}

impl WalWriter {
    /// Opens the WAL for appending. Must be called after WAL recovery so that
    /// `resume_seq` can be set to `RecoveryResult.last_sequence`, ensuring
    /// sequence numbers are continuous across restarts.
    pub async fn open(wal_dir: &Path, max_segment_bytes: u64, resume_seq: u64) -> Result<Self> {
        tokio::fs::create_dir_all(wal_dir).await?;

        // Resume the most recent segment, or start fresh at segment 1.
        let segment_number = highest_segment_number(wal_dir).await?.unwrap_or(1);
        let path = segment_path(wal_dir, segment_number);

        // After a crash-restart, pre-existing segments should be re-populated
        let mut completed_segments = Vec::new();
        for n in 1..segment_number {
            completed_segments.push((n, u64::MAX)); // all entries in prior segments are safe to delete
        }

        // Append mode — never truncate an existing segment on open.
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await
            .with_context(|| format!("failed to open WAL segment {:?}", path))?;

        // Track existing size so rotation threshold is evaluated correctly.
        let current_size = file.metadata().await?.len();

        Ok(Self {
            file,
            current_seq: resume_seq,
            current_size,
            current_segment: segment_number,
            wal_dir: wal_dir.to_path_buf(),
            max_segment_bytes,
            completed_segments,
        })
    }

    /// Appends a batch of data points to the current WAL segment.
    ///
    /// The batch is serialized as a single WalEntry proto, framed with a
    /// length prefix and CRC32 checksum, then fsynced before returning.
    /// Returns the sequence number assigned to this batch.
    pub async fn append(&mut self, points: &[DataPoint]) -> Result<Sequence> {
        self.current_seq += 1;
        let frame = WalWriter::encode_frame(self.current_seq, &points);

        // Write then fsync — fsync must complete before returning to the
        // caller. This is the durability guarantee: once append() returns
        // Ok, the data survives a process crash.
        self.file.write_all(&frame).await?;
        self.file.sync_all().await?;

        self.current_size += frame.len() as u64;

        // Rotate to a new segment if the size threshold is exceeded.
        // New writes after this point go into the fresh segment.
        if self.current_size >= self.max_segment_bytes {
            self.rotate().await?;
        }

        Ok(self.current_seq)
    }

    pub fn encode_frame(seq: Sequence, points: &[DataPoint]) -> Vec<u8> {
        let wal_entry = WalEntry {
            sequence: seq,
            points: points.iter().map(Into::into).collect(),
        };

        // Serialize to protobuf bytes.
        let payload = wal_entry.encode_to_vec();

        // CRC32 over payload — detects bit-flip corruption on recovery.
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&payload);
        let checksum = hasher.finalize();

        // Frame: [length: u32 LE 4 bytes][checksum: u32 LE 4 bytes][payload "length" bytes]
        // Length prefix allows the recovery reader to know how many bytes
        // to read. A mismatch between length and available bytes signals
        // a torn write — recovery stops at that boundary.
        let mut frame = Vec::with_capacity(8 + payload.len());
        frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        frame.extend_from_slice(&checksum.to_le_bytes());
        frame.extend_from_slice(&payload);

        frame
    }

    pub async fn rotate(&mut self) -> Result<()> {
        self.completed_segments
            .push((self.current_segment, self.current_seq));
        self.current_segment += 1;
        let path = self.current_segment_path();
        self.file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await
            .with_context(|| format!("failed to open new WAL segment {:?}", path))?;
        self.current_size = 0;
        Ok(())
    }

    /// Returns the last sequence number written to the WAL.
    /// Called by the flush handler before draining the memtable to capture
    /// the sequence boundary — segments completed up to this point are safe
    /// to delete after the flush succeeds.
    pub fn current_sequence(&self) -> Sequence {
        self.current_seq
    }

    /// Returns the path of the current WAL segment.
    pub fn current_segment_path(&self) -> PathBuf {
        segment_path(&self.wal_dir, self.current_segment)
    }

    pub fn drain_completed_before(&mut self, flushed_seq: Sequence) -> Vec<PathBuf> {
        let mut to_delete = Vec::new();
        self.completed_segments.retain(|(seg, max_seq)| {
            if *max_seq <= flushed_seq {
                to_delete.push(segment_path(&self.wal_dir, *seg));
                false
            } else {
                true
            }
        });
        to_delete
    }
}

fn segment_path(wal_dir: &Path, segment: u64) -> PathBuf {
    wal_dir.join(format!("{:020}.wal", segment))
}

async fn highest_segment_number(wal_dir: &Path) -> Result<Option<u64>> {
    let mut max: Option<u64> = None;
    let mut dir = tokio::fs::read_dir(wal_dir).await?;
    while let Some(entry) = dir.next_entry().await? {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.ends_with(".wal")
            && let Ok(n) = name.trim_end_matches(".wal").parse::<u64>()
        {
            match max {
                None => max = Some(n),
                Some(m) if n > m => max = Some(n),
                _ => {}
            }
        }
    }
    Ok(max)
}
