use crate::memtable;
use flate2::write::GzEncoder;
use flate2::Compression;
use log;

use serde::{Deserialize, Serialize};
use std::fs;
use std::io;
use std::io::Write;
use std::path;
use std::time;

pub mod reader;

#[derive(Debug)]
struct Entry {
    flags: u8,
    key_length: u32,
    key: Vec<u8>,
    value_length: u32,
    value: Vec<u8>,
    deleted: bool,
}

#[derive(Debug, Deserialize, Serialize)]
struct TableMeta {
    blocks: Vec<BlockMeta>,
    timestamp: u128,
}

impl TableMeta {
    fn new() -> Self {
        TableMeta {
            blocks: vec![],
            timestamp: time::SystemTime::now()
                .duration_since(time::UNIX_EPOCH)
                .unwrap()
                .as_millis(),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct BlockMeta {
    count: u32,
    size: u32,
    size_compressed: u32,
    start_key: Vec<u8>,
    start_offset: u32,
}

//
// TODO
// - comment about what this is doing
// - delete the WAL
pub fn flush_to_sstable(memtable: &memtable::Memtable) -> io::Result<u32> {
    log::info!(
        "flushing memtable id = {}, size = {}",
        memtable.id,
        memtable.size()
    );
    let mut table_meta = TableMeta::new();

    let iter = memtable.iter();
    let entries: Vec<Entry> = iter
        .map(|(key, value)| {
            let key_length = key.len() as u32;
            let mut value_length = 0;
            let mut entry_value = vec![];
            let mut deleted = true;
            if value.is_some() {
                value_length = value.as_ref().unwrap().len() as u32;
                entry_value = value.unwrap();
                deleted = false;
            }

            let mut flags: u8 = 0;
            if deleted {
                flags += 1 << 7;
            }

            Entry {
                flags,
                key,
                key_length,
                value: entry_value,
                value_length,
                deleted,
            }
        })
        .collect();

    let filename = format!("/tmp/sstable-data-{}", memtable.id);
    let path = path::Path::new(&filename);
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)?;

    let mut current_block = BlockMeta {
        count: 0,
        size: 0,
        size_compressed: 0,
        start_key: vec![],
        start_offset: 0,
    };

    let max_block_size_uncompressed = 20;
    let mut total_bytes_written = 0u32;
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());

    for entry in &entries {
        encoder.write(&[entry.flags])?;
        current_block.count += 1;

        encoder.write(&[
            (entry.key_length >> 24) as u8,
            (entry.key_length >> 16) as u8,
            (entry.key_length >> 8) as u8,
            entry.key_length as u8,
        ])?;
        encoder.write(&entry.key)?;
        current_block.size += entry.key_length;

        if !entry.deleted {
            encoder.write(&[
                (entry.value_length >> 24) as u8,
                (entry.value_length >> 16) as u8,
                (entry.value_length >> 8) as u8,
                entry.value_length as u8,
            ])?;
            encoder.write(&entry.value)?;
            current_block.size += entry.value_length;
        }

        if current_block.count == 0 {
            current_block.start_key = entry.key.clone();
        }

        // flush compressed block
        if current_block.size > max_block_size_uncompressed {
            let bytes: Vec<u8> = encoder.finish()?;
            current_block.size_compressed = bytes.len() as u32;
            log::debug!(
                "writing block # {}. count entries = {}, uncompressed size = {}, compressed size = {}, start_key = {:?}",
                table_meta.blocks.len(),
                current_block.count,
                current_block.size,
                current_block.size_compressed,
                current_block.start_key,
            );
            table_meta.blocks.push(current_block);
            total_bytes_written += bytes.len() as u32;
            file.write(&bytes)?;

            encoder = GzEncoder::new(Vec::new(), Compression::default());
            current_block = BlockMeta {
                count: 0,
                size: 0,
                size_compressed: 0,
                start_key: vec![],
                start_offset: total_bytes_written,
            };
        }
    }

    if current_block.count > 0 {
        let bytes: Vec<u8> = encoder.finish()?;
        current_block.size_compressed = bytes.len() as u32;
        log::debug!(
            "writing block #{} with uncompressed size {} / compressed size {} start_key: {:?}",
            table_meta.blocks.len(),
            current_block.size,
            bytes.len(),
            current_block.start_key,
        );
        table_meta.blocks.push(current_block);
        file.write(&bytes)?;
    }
    file.flush()?;

    flush_sstable_meta(memtable, &table_meta)?;

    return Ok(1);
}

#[cfg(test)]
mod flush_to_sstable_tests {
    use super::*;
    use crate::memtable;

    #[test]
    fn smoke_test() {
        let mut m = memtable::Memtable::new();
        m.insert("abc".bytes().collect(), Some("abc".bytes().collect()));
        m.insert("def".bytes().collect(), Some("dev".bytes().collect()));
        let result = flush_to_sstable(&m);
        println!("{:?} {:?}", m, result);
    }
}

fn flush_sstable_meta(memtable: &memtable::Memtable, metadata: &TableMeta) -> io::Result<()> {
    let filename = format!("/tmp/sstable-meta-{}", memtable.id);
    let path = path::Path::new(&filename);
    let file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)?;

    let result = serde_yaml::to_writer(file, metadata);
    match result {
        Ok(_) => return Ok(()),
        Err(err) => {
            log::error!("Error happen {:?}", err);
            // TODO handle this
            // return Err(std::io::Error::from(err))
        }
    };
    return Ok(());
}
