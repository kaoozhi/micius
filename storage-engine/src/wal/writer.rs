#![allow(unused)]

use anyhow::{Context, Result};
use std::path::{Path, PathBuf};
use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncWriteExt;

use crate::types::{DataPoint, Sequence};
use crate::wal::proto::WalEntry;
use prost::Message;

pub struct WalWriter {
    file: File,
    current_seq: Sequence,
    current_size: u64,
    current_segment: u32,
    wal_dir: PathBuf,
    max_segment_bytes: u64,
}

impl WalWriter {
    /// Opens the WAL for appending. Resumes the most recent segment if one
    /// exists, otherwise creates segment 1. Called once at startup, before
    /// WAL recovery replays entries into the memtable.
    pub async fn open(wal_dir: &Path, max_segment_bytes: u64) -> Result<Self> {
        tokio::fs::create_dir_all(wal_dir).await?;

        // Resume the most recent segment, or start fresh at segment 1.
        let segment_number = highest_segment_number(wal_dir).await?.unwrap_or(1);
        let path = segment_path(wal_dir, segment_number);

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
            current_seq: 0,
            current_size,
            current_segment: segment_number,
            wal_dir: wal_dir.to_path_buf(),
            max_segment_bytes,
        })
    }

    /// Appends a batch of data points to the current WAL segment.
    ///
    /// The batch is serialized as a single WalEntry proto, framed with a
    /// length prefix and CRC32 checksum, then fsynced before returning.
    /// Returns the sequence number assigned to this batch.
    pub async fn append(&mut self, points: &[DataPoint]) -> Result<Sequence> {
        self.current_seq += 1;

        let wal_entry = WalEntry {
            sequence: self.current_seq,
            points: points.iter().map(Into::into).collect(),
        };

        // Serialize to protobuf bytes.
        let payload = wal_entry.encode_to_vec();

        // CRC32 over payload — detects bit-flip corruption on recovery.
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&payload);
        let checksum = hasher.finalize();

        // Frame: [length: u32 LE][checksum: u32 LE][payload]
        // Length prefix allows the recovery reader to know how many bytes
        // to read. A mismatch between length and available bytes signals
        // a torn write — recovery stops at that boundary.
        let mut frame = Vec::with_capacity(8 + payload.len());
        frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        frame.extend_from_slice(&checksum.to_le_bytes());
        frame.extend_from_slice(&payload);

        // Write then fsync — fsync must complete before returning to the
        // caller. This is the durability guarantee: once append() returns
        // Ok, the data survives a process crash.
        self.file.write_all(&frame).await?;
        self.file.sync_all().await?;

        self.current_size += frame.len() as u64;

        // Rotate to a new segment if the size threshold is exceeded.
        // New writes after this point go into the fresh segment.
        if self.current_size > self.max_segment_bytes {
            self.current_segment += 1;
            let path = segment_path(&self.wal_dir, self.current_segment);
            self.file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
                .await
                .with_context(|| format!("failed to open new WAL segment {:?}", path))?;
            self.current_size = 0;
        }

        Ok(self.current_seq)
    }
}

fn segment_path(wal_dir: &Path, segment: u32) -> PathBuf {
    wal_dir.join(format!("{:020}.wal", segment))
}

async fn highest_segment_number(wal_dir: &Path) -> Result<Option<u32>> {
    let mut max: Option<u32> = None;
    let mut dir = tokio::fs::read_dir(wal_dir).await?;
    while let Some(entry) = dir.next_entry().await? {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.ends_with(".wal") {
            if let Ok(n) = name.trim_end_matches(".wal").parse::<u32>() {
                match max {
                    None => max = Some(n),
                    Some(m) if n > m => max = Some(n),
                    _ => {}
                }
            }
        }
    }
    Ok(max)
}
