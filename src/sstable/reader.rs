use flate2::read::GzDecoder;
use log;
use regex::Regex;
use std::collections::VecDeque;
use std::fs;
use std::io;
use std::io::{Read, Seek};
use std::path;

use super::{BlockMeta, Entry, TableMeta};
use crate::config;
use crate::memtable;

pub struct Reader {
    sstables: VecDeque<(TableMeta, Box<path::Path>)>,
}

impl Reader {
    pub fn new() -> Self {
        Reader {
            sstables: VecDeque::new(),
        }
    }

    // this scans the sstable directory for sstables
    // TODO
    // - try to ignore half-way flushed sstables
    // - try to ignore files called sstables, that aren't sstables (could do this by checking metadata)
    pub fn init(&mut self, config: &config::Config) {
        log::info!("initializing sstable reader");

        let mut sstables = vec![];
        for file in fs::read_dir(&config.data_dir).unwrap() {
            let path: Box<path::Path> = file.unwrap().path().into_boxed_path();
            if is_sstable(&path) {
                let meta_path = String::from(
                    Regex::new("sstable-data")
                        .unwrap()
                        .replace(path.to_str().unwrap(), "sstable-meta"),
                );
                let table_meta = read_table_meta(path::Path::new(&meta_path));

                log::debug!(
                    "found memtable = {:?}, num_blocks = {:?}",
                    path,
                    table_meta.blocks.len()
                );

                sstables.push((table_meta, path));
            }
        }

        // make sure the sstables are ordered newest to oldest
        sstables.sort_by(|a, b| {
            let (a_meta, _1) = a;
            let (b_meta, _2) = b;

            if a_meta.timestamp > b_meta.timestamp {
                std::cmp::Ordering::Less
            } else {
                std::cmp::Ordering::Greater
            }
        });

        sstables.into_iter().for_each(|v| {
            self.sstables.push_back(v);
        });

        log::info!("initialized with {} memtables", self.sstables.len());
    }

    pub fn find(&self, key: &[u8]) -> Option<Vec<u8>> {
        for (table_meta, path) in &self.sstables {
            log::debug!("searching for '{:?}' in '{:?}", key, path);

            if !table_meta.bloom_filter.contains(key) {
                log::debug!("not found in bloom filter");
                continue;
            }

            let block = find_block(key, table_meta);
            if block.is_none() {
                log::debug!("not found in blocks");
                continue;
            }

            let result = find_from_table(key, path, &table_meta.blocks[block.unwrap()]);
            match result {
                Ok((Some(entry), _)) => {
                    log::debug!("found '{:?}' in '{:?}", key, path);
                    return Some(entry.value);
                }
                Ok((None, true)) => return None,
                Ok((None, false)) => {
                    log::debug!("not found '{:?}' in '{:?}", key, path);
                }
                Err(err) => {
                    // TODO handle in a smarter way
                    panic!("error happened reading from file {:?} {:?}", path, err)
                }
            }
        }
        None
    }

    pub fn add_memtable(&mut self, memtable: &memtable::Memtable) {
        let filename = format!("/tmp/sstable-data-{}", memtable.id);
        let path = path::PathBuf::from(filename).into_boxed_path();
        let meta_path = to_metadata_path(&path);
        let table_meta = read_table_meta(path::Path::new(&meta_path));
        log::debug!(
            "memtable added = {:?}, num_blocks = {:?}. There are now {:?} reader memtables",
            path,
            table_meta.blocks.len(),
            self.sstables.len() + 1,
        );

        // TODO need to re-sort sstable
        self.sstables.push_front((table_meta, path));
    }

    pub fn remove_memtable(&mut self, memtable_id: &str) {
        let index = self
            .sstables
            .iter()
            .position(|(_, path)| path.to_str().unwrap().ends_with(memtable_id));

        if index.is_some() {
            log::debug!("memtable {} removed", memtable_id);
            self.sstables.remove(index.unwrap());
        } else {
            log::debug!("no memtable {} to remove", memtable_id);
        }
    }
}

fn to_metadata_path(path: &path::Path) -> String {
    let meta_path = String::from(
        Regex::new("sstable-data")
            .unwrap()
            .replace(path.to_str().unwrap(), "sstable-meta"),
    );
    return meta_path;
}

fn read_table_meta(path: &path::Path) -> TableMeta {
    let file = fs::OpenOptions::new().read(true).open(path);
    match file {
        Ok(file) => {
            let result: TableMeta = serde_yaml::from_reader(file).unwrap();
            return result;
        }
        Err(err) => {
            log::error!(
                "An error happened reading sstable meta at {:?}: {:?}",
                path,
                err
            );
            panic!("could not read table meta, invalid state");
        }
    };
}

fn is_sstable(path: &path::Path) -> bool {
    let re = regex::Regex::new(r".*/sstable-data.*$").unwrap();
    re.is_match(path.to_str().unwrap())
}

fn deserialize_block(path: &path::Path, block: &BlockMeta) -> io::Result<Vec<u8>> {
    let mut file = fs::OpenOptions::new().read(true).open(path)?;
    let start = block.start_offset as u64;
    let seek_start = io::SeekFrom::Start(start);
    file.seek(seek_start)?;
    let mut bytes = Vec::<u8>::with_capacity(block.size_compressed as usize);
    file.read_to_end(&mut bytes)?;

    let mut decoder = GzDecoder::new(&*bytes);
    let mut decompressed = Vec::<u8>::with_capacity(block.size as usize);
    decoder.read_to_end(&mut decompressed)?;
    return Ok(decompressed);
}

fn find_from_table(
    search_key: &[u8],
    path: &path::Path,
    block: &BlockMeta,
) -> io::Result<(Option<Entry>, bool)> {
    let bytes1 = deserialize_block(path, block)?;
    let mut bytes = bytes1.into_iter().map(|b| Ok::<u8, io::Error>(b));

    loop {
        let flags_1_option = bytes.next();
        if flags_1_option.is_none() {
            return Ok((None, false));
        }

        let flags_1 = flags_1_option.unwrap()?;
        let deleted = flags_1 & (1 << 6) > 0;

        let key_length = ((bytes.next().unwrap()? as u32) << 24)
            + ((bytes.next().unwrap()? as u32) << 16)
            + ((bytes.next().unwrap()? as u32) << 8)
            + (bytes.next().unwrap()? as u32);

        let mut key: Vec<u8> = Vec::with_capacity(key_length as usize);
        for _ in 0..key_length {
            key.push(bytes.next().unwrap()?);
        }

        let mut value_length = 0;
        if !deleted {
            value_length = ((bytes.next().unwrap()? as u32) << 24)
                + ((bytes.next().unwrap()? as u32) << 16)
                + ((bytes.next().unwrap()? as u32) << 8)
                + (bytes.next().unwrap()? as u32);
        }

        // if key matches, return result
        if &key == search_key {
            if deleted {
                return Ok((None, true));
            }

            let mut value = Vec::with_capacity(value_length as usize);
            for _ in 0..value_length {
                value.push(bytes.next().unwrap()?);
            }

            return Ok((
                Some(Entry {
                    flags: 0,
                    key_length,
                    key,
                    value_length,
                    value,
                    deleted,
                }),
                true,
            ));
        }

        // skip over the value
        for _ in 0..value_length {
            bytes.next();
        }
    }
}

// do binary seach on the block data for the key
// returns an option of the index of the block that would contain the key
fn find_block(search_key: &[u8], table_meta: &TableMeta) -> Option<usize> {
    let mut max = table_meta.blocks.len() - 1;
    let mut min = 0;
    let mut idx = table_meta.blocks.len() / 2;

    loop {
        let curr_key = &table_meta.blocks[idx].start_key;

        if *search_key == **curr_key {
            return Some(idx);
        }

        if *search_key < **curr_key {
            if idx - min == 1 {
                let prev_key = &table_meta.blocks[idx - 1].start_key;
                if **prev_key <= *search_key {
                    return Some(idx - 1);
                } else {
                    return None;
                }
            }

            max = idx;
            idx = idx - ((idx - min) / 2);
        } else {
            if idx >= table_meta.blocks.len() - 1 {
                let last_key = &table_meta.blocks[table_meta.blocks.len() - 1].start_key;
                if **last_key <= *search_key {
                    return Some(table_meta.blocks.len() - 1);
                } else {
                    return None;
                }
            }

            min = idx;
            idx = 1 + idx + (max - idx) / 2;
        }
    }
}

#[cfg(test)]
mod reader_tests {
    use super::*;
    use crate::sstable;

    #[test]
    fn smoke_test() {
        let data_dir = "/tmp/sstable_reader_tests/smoke_test";
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
        sstable::flush_to_sstable(&config, &memtable, 0).unwrap();

        let mut memtable = memtable::Memtable::new();
        // block 1
        memtable.insert("4bc".bytes().collect(), Some("abc".bytes().collect()));
        memtable.insert("4ef".bytes().collect(), Some("def".bytes().collect()));
        // block 2
        memtable.insert("5bc".bytes().collect(), Some("abc".bytes().collect()));
        memtable.insert("5ef".bytes().collect(), Some("def".bytes().collect()));
        // block 3
        memtable.insert("6bc".bytes().collect(), Some("abc".bytes().collect()));
        sstable::flush_to_sstable(&config, &memtable, 0).unwrap();

        let mut reader = Reader::new();
        reader.init(&config);

        assert_eq!(2, reader.sstables.len());

        let find_none = reader.find("7bc".as_bytes());
        assert_eq!(true, find_none.is_none());

        let find1 = reader.find("1bc".as_bytes());
        assert_eq!(true, find1.is_some());
        assert_eq!(String::from("abc").into_bytes(), find1.unwrap());
        let find1 = reader.find("1ef".as_bytes());
        assert_eq!(true, find1.is_some());
        assert_eq!(String::from("def").into_bytes(), find1.unwrap());

        let find1 = reader.find("2bc".as_bytes());
        assert_eq!(true, find1.is_some());
        assert_eq!(String::from("abc").into_bytes(), find1.unwrap());
        let find1 = reader.find("2ef".as_bytes());
        assert_eq!(true, find1.is_some());
        assert_eq!(String::from("def").into_bytes(), find1.unwrap());

        let find1 = reader.find("3bc".as_bytes());
        assert_eq!(true, find1.is_some());
        assert_eq!(String::from("abc").into_bytes(), find1.unwrap());

        let find1 = reader.find("4bc".as_bytes());
        assert_eq!(true, find1.is_some());
        assert_eq!(String::from("abc").into_bytes(), find1.unwrap());
        let find1 = reader.find("4ef".as_bytes());
        assert_eq!(true, find1.is_some());
        assert_eq!(String::from("def").into_bytes(), find1.unwrap());

        let find1 = reader.find("5bc".as_bytes());
        assert_eq!(true, find1.is_some());
        assert_eq!(String::from("abc").into_bytes(), find1.unwrap());
        let find1 = reader.find("5ef".as_bytes());
        assert_eq!(true, find1.is_some());
        assert_eq!(String::from("def").into_bytes(), find1.unwrap());

        let find1 = reader.find("6bc".as_bytes());
        assert_eq!(true, find1.is_some());
        assert_eq!(String::from("abc").into_bytes(), find1.unwrap());

        fs::remove_dir_all(data_dir).unwrap();
    }

    #[test]
    fn remove_from_reader() {
        let data_dir = "/tmp/sstable_reader_tests/remove_from_reader";
        fs::remove_dir_all(data_dir);
        fs::create_dir_all(data_dir).unwrap();

        let mut config = config::Config::new();
        config.data_dir = String::from(data_dir);
        config.sstable_block_size = 12;

        let mut memtable = memtable::Memtable::new();
        memtable.insert("abc".bytes().collect(), Some("abc".bytes().collect()));
        sstable::flush_to_sstable(&config, &memtable, 0).unwrap();

        let mut reader = Reader::new();
        reader.init(&config);

        assert_eq!(1, reader.sstables.len());
        let find1 = reader.find("abc".as_bytes());
        assert_eq!(true, find1.is_some());

        reader.remove_memtable(&memtable.id);
        assert_eq!(0, reader.sstables.len());
        assert_eq!(false, reader.find("abc".as_bytes()).is_some());

        fs::remove_dir_all(data_dir).unwrap();
    }
}

#[cfg(test)]
mod find_block_tests {
    use super::super::BlockMeta;
    use super::*;

    #[test]
    fn test5() {
        let mut table_meta = TableMeta::new(0);
        table_meta.blocks.push(BlockMeta {
            count: 10,
            size: 10,
            size_compressed: 10,
            start_key: "marche".bytes().collect(),
            start_offset: 0,
        });
        table_meta.blocks.push(BlockMeta {
            count: 10,
            size: 10,
            size_compressed: 10,
            start_key: "toyota".bytes().collect(),
            start_offset: 10,
        });

        let search_key: Vec<u8> = "rue".bytes().collect();

        assert_eq!(0 as usize, find_block(&search_key, &table_meta).unwrap());
    }

    #[test]
    fn smoke_test() {
        let mut table_meta = TableMeta::new(0);
        table_meta.blocks.push(BlockMeta {
            count: 10,
            size: 10,
            size_compressed: 10,
            start_key: "a".bytes().collect(),
            start_offset: 0,
        });
        table_meta.blocks.push(BlockMeta {
            count: 10,
            size: 10,
            size_compressed: 10,
            start_key: "c".bytes().collect(),
            start_offset: 10,
        });
        table_meta.blocks.push(BlockMeta {
            count: 10,
            size: 10,
            size_compressed: 10,
            start_key: "d".bytes().collect(),
            start_offset: 20,
        });

        let search_key: Vec<u8> = "b".bytes().collect();

        assert_eq!(0 as usize, find_block(&search_key, &table_meta).unwrap());
    }

    #[test]
    fn test2() {
        let mut table_meta = TableMeta::new(0);
        table_meta.blocks.push(BlockMeta {
            count: 10,
            size: 10,
            size_compressed: 10,
            start_key: "a".bytes().collect(),
            start_offset: 0,
        });
        table_meta.blocks.push(BlockMeta {
            count: 10,
            size: 10,
            size_compressed: 10,
            start_key: "c".bytes().collect(),
            start_offset: 10,
        });
        table_meta.blocks.push(BlockMeta {
            count: 10,
            size: 10,
            size_compressed: 10,
            start_key: "e".bytes().collect(),
            start_offset: 20,
        });

        let search_key: Vec<u8> = "d".bytes().collect();
        assert_eq!(1 as usize, find_block(&search_key, &table_meta).unwrap());
    }

    #[test]
    fn test3() {
        let mut table_meta = TableMeta::new(0);
        table_meta.blocks.push(BlockMeta {
            count: 10,
            size: 10,
            size_compressed: 10,
            start_key: "b".bytes().collect(),
            start_offset: 0,
        });
        table_meta.blocks.push(BlockMeta {
            count: 10,
            size: 10,
            size_compressed: 10,
            start_key: "c".bytes().collect(),
            start_offset: 10,
        });
        table_meta.blocks.push(BlockMeta {
            count: 10,
            size: 10,
            size_compressed: 10,
            start_key: "e".bytes().collect(),
            start_offset: 20,
        });

        let search_key: Vec<u8> = "a".bytes().collect();
        assert_eq!(None, find_block(&search_key, &table_meta));
    }

    #[test]
    fn test4() {
        let mut table_meta = TableMeta::new(0);
        table_meta.blocks.push(BlockMeta {
            count: 10,
            size: 10,
            size_compressed: 10,
            start_key: "b".bytes().collect(),
            start_offset: 0,
        });
        table_meta.blocks.push(BlockMeta {
            count: 10,
            size: 10,
            size_compressed: 10,
            start_key: "c".bytes().collect(),
            start_offset: 10,
        });
        table_meta.blocks.push(BlockMeta {
            count: 10,
            size: 10,
            size_compressed: 10,
            start_key: "e".bytes().collect(),
            start_offset: 20,
        });

        let search_key: Vec<u8> = "f".bytes().collect();
        assert_eq!(2, find_block(&search_key, &table_meta).unwrap());
    }

    #[test]
    fn test6() {
        let mut table_meta = TableMeta::new(0);
        let starts = vec!["a", "g", "j", "l", "r", "u", "z"];
        for i in 0..starts.len() {
            table_meta.blocks.push(BlockMeta {
                count: 10,
                size: 10,
                size_compressed: 10,
                start_key: starts[i].bytes().collect(),
                start_offset: i as u32 * 10,
            })
        }

        let mut search_key: Vec<u8> = "f".bytes().collect();
        assert_eq!(0, find_block(&search_key, &table_meta).unwrap());

        search_key = "h".bytes().collect();
        assert_eq!(1, find_block(&search_key, &table_meta).unwrap());

        search_key = "k".bytes().collect();
        assert_eq!(2, find_block(&search_key, &table_meta).unwrap());

        search_key = "m".bytes().collect();
        assert_eq!(3, find_block(&search_key, &table_meta).unwrap());

        search_key = "s".bytes().collect();
        assert_eq!(4, find_block(&search_key, &table_meta).unwrap());

        search_key = "w".bytes().collect();
        assert_eq!(5, find_block(&search_key, &table_meta).unwrap());

        search_key = "z2".bytes().collect();
        assert_eq!(6, find_block(&search_key, &table_meta).unwrap());

        search_key = "1".bytes().collect();
        assert_eq!(None, find_block(&search_key, &table_meta));
    }
}

#[derive(Debug)]
pub struct SstableIterator {
    path: Box<path::Path>,
    table_meta: super::TableMeta,
    table_index: usize,
    block_index: usize,
    curr_block: Vec<super::Entry>,
}

impl SstableIterator {
    pub fn new(path: Box<path::Path>, table_meta: super::TableMeta) -> Self {
        SstableIterator {
            path,
            table_meta,
            table_index: 0,
            block_index: 0,
            curr_block: vec![],
        }
    }

    fn goto_next_block(&mut self) -> io::Result<()> {
        let block = &self.table_meta.blocks[self.table_index];
        self.table_index += 1;
        let bytes1 = super::reader::deserialize_block(&self.path, block).unwrap();
        let mut bytes = bytes1.into_iter().map(|b| Ok::<u8, io::Error>(b));

        let mut next_block = vec![];
        loop {
            let flags_1_option = bytes.next();
            if flags_1_option.is_none() {
                break;
            }

            let flags_1 = flags_1_option.unwrap().unwrap();
            let deleted = flags_1 & (1 << 6) > 0;

            let key_length = ((bytes.next().unwrap()? as u32) << 24)
                + ((bytes.next().unwrap()? as u32) << 16)
                + ((bytes.next().unwrap()? as u32) << 8)
                + (bytes.next().unwrap()? as u32);

            let mut key: Vec<u8> = Vec::with_capacity(key_length as usize);
            for _ in 0..key_length {
                key.push(bytes.next().unwrap()?);
            }

            let mut value_length = 0;
            if !deleted {
                value_length = ((bytes.next().unwrap()? as u32) << 24)
                    + ((bytes.next().unwrap()? as u32) << 16)
                    + ((bytes.next().unwrap()? as u32) << 8)
                    + (bytes.next().unwrap()? as u32);
            }

            let mut value = Vec::with_capacity(value_length as usize);
            for _ in 0..value_length {
                value.push(bytes.next().unwrap()?);
            }

            let entry = super::Entry {
                flags: flags_1,
                deleted,
                key,
                key_length,
                value,
                value_length,
            };
            next_block.push(entry);
        }

        self.curr_block = next_block;
        self.block_index = 0;

        Ok(())
    }
}

impl Iterator for SstableIterator {
    type Item = super::Entry;

    fn next(&mut self) -> Option<Self::Item> {
        if self.block_index >= self.curr_block.len() {
            if self.table_index >= self.table_meta.blocks.len() {
                return None;
            }
            self.goto_next_block().unwrap();
        }

        let entry = std::mem::replace(
            &mut self.curr_block[self.block_index],
            super::Entry {
                key: vec![],
                value: vec![],
                key_length: 0,
                value_length: 0,
                flags: 0,
                deleted: false,
            },
        );
        self.block_index += 1;

        Some(entry)
    }
}
