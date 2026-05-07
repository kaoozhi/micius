use crate::types::DataPoint;
use crate::wal::proto::WalEntry;
use anyhow::{Context, Result};
use crc32fast::Hasher;
use prost::Message;
use std::path::Path;
use std::path::PathBuf;
use tokio::fs::File;
use tokio::io::AsyncReadExt;

pub struct RecoveryResult {
    pub points: Vec<DataPoint>,
    pub last_sequence: u64,
    pub segments_replayed: u32,
    pub entries_replayed: u64,
    pub torn_write_detected: bool,
}

pub fn get_wal_files(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut files: Vec<_> = std::fs::read_dir(dir)
        .context("failed to read WAL directory")?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "wal"))
        .map(|e| e.path())
        .collect();
    files.sort();
    Ok(files)
}

pub async fn recover(wal_dir: &Path) -> Result<RecoveryResult> {
    let mut result = RecoveryResult {
        points: Vec::new(),
        last_sequence: 0,
        segments_replayed: 0,
        entries_replayed: 0,
        torn_write_detected: false,
    };
    let paths = get_wal_files(wal_dir)?;
    for path in paths {
        let mut file = File::open(path).await?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf).await?;

        let mut cursor = 0usize;

        loop {
            // Need at least 8 bytes for the frame header
            if cursor + 8 > buf.len() {
                break;
            }
            // Read frame header
            // Parse the first 4 bytes to get the original payload length
            let stored_length = u32::from_le_bytes(buf[cursor..cursor + 4].try_into()?) as usize;
            if cursor + stored_length + 8 > buf.len() {
                // torn write found stop
                result.torn_write_detected = true;
                break;
            }
            // Parse the checksum
            let stored_checksum = u32::from_le_bytes(buf[cursor + 4..cursor + 8].try_into()?);
            cursor += 8;

            // Get the checksum of recovered payload
            let payload = &buf[cursor..cursor + stored_length];
            cursor += stored_length;
            let mut hasher = Hasher::new();
            hasher.update(payload);
            let computed_checksum = hasher.finalize();

            if stored_checksum != computed_checksum {
                // data corruption found stop
                result.torn_write_detected = true;
                break;
            }

            // Now good to decode WalEntry proto and collect DataPoints
            let entry = WalEntry::decode(payload)?;
            let points: Vec<DataPoint> = entry.points.into_iter().map(Into::into).collect();
            result.points.extend_from_slice(&points);
            result.last_sequence = result.last_sequence.max(entry.sequence);
            result.entries_replayed += 1;
        }
        result.segments_replayed += 1;
    }

    Ok(result)
}
