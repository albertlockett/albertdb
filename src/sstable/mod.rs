use flate2::write::GzEncoder;
use flate2::Compression;
use log;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io;
use std::io::Write;
use std::path;
use std::time;

use crate::bloom;
use crate::config;
use crate::memtable;

pub mod reader;

#[derive(Debug)]
pub struct Entry {
    flags: u8,
    key_length: u32,
    pub key: Vec<u8>,
    value_length: u32,
    pub value: Vec<u8>,
    pub deleted: bool,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct TableMeta {
    blocks: Vec<BlockMeta>,
    bloom_filter: bloom::BloomFilter,
    timestamp: u128,
    pub level: u8,
}

impl TableMeta {
    fn new(level: u8) -> Self {
        TableMeta {
            blocks: vec![],
            bloom_filter: bloom::BloomFilter::new(2048, 2142 /* <- random seed */, 3),
            timestamp: time::SystemTime::now()
                .duration_since(time::UNIX_EPOCH)
                .unwrap()
                .as_millis(),
            level,
        }
    }

    pub fn table_size_compressed(&self) -> u64 {
        let mut table_size = 0u64;
        for block in &self.blocks {
            table_size += block.size_compressed as u64
        }
        return table_size;
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
pub fn flush_to_sstable(
    config: &config::Config,
    memtable: &memtable::Memtable,
    level: u8,
) -> io::Result<u32> {
    log::info!(
        "flushing memtable id = {}, size = {}",
        memtable.id,
        memtable.size()
    );
    let mut table_meta = TableMeta::new(level);

    let iter = memtable.iter();
    let entries: Vec<Entry> = iter
        .map(|(key, value)| {
            table_meta.bloom_filter.insert(&key);
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
                flags += 1 << 6;
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

    let filename = format!("{}/sstable-data-{}", config.data_dir, memtable.id);
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

    let mut total_bytes_written = 0u32;
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());

    for entry in &entries {
        encoder.write(&[entry.flags])?;
        encoder.write(&[
            (entry.key_length >> 24) as u8,
            (entry.key_length >> 16) as u8,
            (entry.key_length >> 8) as u8,
            entry.key_length as u8,
        ])?;
        encoder.write(&entry.key)?;
        if !entry.deleted {
            encoder.write(&[
                (entry.value_length >> 24) as u8,
                (entry.value_length >> 16) as u8,
                (entry.value_length >> 8) as u8,
                entry.value_length as u8,
            ])?;
            encoder.write(&entry.value)?;
        }

        if current_block.count == 0 {
            current_block.start_key = entry.key.clone();
        }

        current_block.count += 1;
        current_block.size += entry.key_length;
        if !entry.deleted {
            current_block.size += entry.value_length;
        }

        // flush compressed block
        if current_block.size >= config.sstable_block_size {
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

    flush_sstable_meta(config, memtable, &table_meta)?;

    return Ok(1);
}

fn flush_sstable_meta(
    config: &config::Config,
    memtable: &memtable::Memtable,
    metadata: &TableMeta,
) -> io::Result<()> {
    let filename = format!("{}/sstable-meta-{}", config.data_dir, memtable.id);
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
            panic!(
                "error deserializsing {}/sstable-meta-{}",
                config.data_dir, memtable.id
            );
        }
    };
}

#[cfg(test)]
mod mod_tests {
    use super::*;
    use crate::memtable;

    #[test]
    fn smoke_test() {
        let data_dir = "/tmp/sstable_tests/smoke_test";
        fs::remove_dir_all(data_dir);
        fs::create_dir_all(data_dir).unwrap();

        let mut config = config::Config::new();
        config.data_dir = String::from(data_dir);
        config.sstable_block_size = 12;

        let mut memtable = memtable::Memtable::new();
        // block 1
        memtable.insert("1bc".bytes().collect(), Some("abc".bytes().collect()));
        memtable.insert("1ef".bytes().collect(), Some("def".bytes().collect()));

        // block 2
        memtable.insert("2bc".bytes().collect(), Some("abc".bytes().collect()));
        memtable.insert("2ef".bytes().collect(), Some("def".bytes().collect()));

        // block 3
        memtable.insert("3bc".bytes().collect(), Some("abc".bytes().collect()));

        let result = flush_to_sstable(&config, &memtable, 0);

        // expect result to be OK
        assert_eq!(true, result.is_ok());

        // expect there is both a data table a meta table
        let data_meta_r = fs::metadata(format!("{}/sstable-data-{}", data_dir, memtable.id));
        assert_eq!(true, data_meta_r.is_ok());
        let data_meta = data_meta_r.unwrap();
        assert_eq!(true, data_meta.len() > 0);

        // expect there to me a metadata file
        let meta_meta_r = fs::metadata(format!("{}/sstable-meta-{}", data_dir, memtable.id));
        assert_eq!(true, meta_meta_r.is_ok());
        let meta_meta = meta_meta_r.unwrap();
        assert_eq!(true, meta_meta.len() > 0);

        // read back the meta and assert on it's structure
        let file = fs::OpenOptions::new()
            .read(true)
            .open(path::Path::new(&format!(
                "{}/sstable-meta-{}",
                data_dir, memtable.id
            )))
            .unwrap();
        let table_meta: TableMeta = serde_yaml::from_reader(file).unwrap();
        assert_eq!(3, table_meta.blocks.len());

        let block0 = &table_meta.blocks[0];
        assert_eq!(2, block0.count);
        assert_eq!(12, block0.size);
        assert_eq!(String::from("1bc").into_bytes(), block0.start_key);
        assert_eq!(0, block0.start_offset);

        let block1 = &table_meta.blocks[1];
        assert_eq!(2, block1.count);
        assert_eq!(12, block1.size);
        assert_eq!(String::from("2bc").into_bytes(), block1.start_key);
        assert_eq!(40, block1.start_offset);

        let block2 = &table_meta.blocks[2];
        assert_eq!(1, block2.count);
        assert_eq!(6, block2.size);
        assert_eq!(String::from("3bc").into_bytes(), block2.start_key);
        assert_eq!(80, block2.start_offset);

        fs::remove_dir_all(data_dir).unwrap();
    }
}

pub fn delete_by_id(config: &config::Config, sstable_id: &str) -> io::Result<()> {
    let data_file = format!("{}/sstable-data-{}", config.data_dir, sstable_id);
    fs::remove_file(data_file)?;
    let meta_file = format!("{}/sstable-meta-{}", config.data_dir, sstable_id);
    fs::remove_file(meta_file)?;

    Ok(())
}
