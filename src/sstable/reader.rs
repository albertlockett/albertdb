use log;
use regex::Regex;
use std::collections::HashMap;
use std::fs;
use std::io;
use std::io::Read;
use std::path;

use super::{BlockMeta, Entry, TableMeta};

// TODO
// - some implementation to let us know that there is a newly flushd sstable

pub struct Reader {
    sstables: Vec<Box<path::Path>>,
    blockMeta: HashMap<String, TableMeta>,
}

impl Reader {
    pub fn new() -> Self {
        Reader {
            sstables: vec![],
            blockMeta: HashMap::new(),
        }
    }

    // this scans the sstable directory for sstables
    // TODO
    // - try to ignore half-way flushed sstables
    // - try to ignore files called sstables, that aren't sstables (could do this by checking metadata)
    pub fn init(&mut self) {
        log::info!("initializing sstable reader");
        let data_dir = "/tmp"; // TODO not have hard coded
        for file in fs::read_dir(data_dir).unwrap() {
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

                self.blockMeta.insert(meta_path, table_meta);
                self.sstables.push(path);
            }
        }

        log::info!("initialized with {} memtables", self.sstables.len());
    }

    pub fn find(&self, key: &[u8]) -> Option<Vec<u8>> {
        for path in &self.sstables {
            log::debug!("searching for '{:?}' in '{:?}", key, path);
            let result = find_from_table(key, path);
            match result {
                Ok(Some(entry)) => {
                    log::debug!("found '{:?}' in '{:?}", key, path);
                    return Some(entry.value);
                }
                Ok(None) => {
                    log::debug!("not found '{:?}' in '{:?}", key, path);
                    // skip - could debug log?
                }
                Err(err) => {
                    // TODO handle more smartly?
                    panic!("error happened reading from file {:?} {:?}", path, err)
                }
            }
        }
        None
    }
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

fn find_from_table(search_key: &[u8], path: &path::Path) -> io::Result<Option<Entry>> {
    let file = fs::OpenOptions::new().read(true).open(path)?;
    let mut bytes = file.bytes();

    loop {
        let flags_1_option = bytes.next();
        if flags_1_option.is_none() {
            return Ok(None);
        }

        let key_length = ((bytes.next().unwrap()? as u32) << 24)
            + ((bytes.next().unwrap()? as u32) << 16)
            + ((bytes.next().unwrap()? as u32) << 8)
            + (bytes.next().unwrap()? as u32);

        let mut key: Vec<u8> = Vec::with_capacity(key_length as usize);
        for _ in 0..key_length {
            key.push(bytes.next().unwrap()?);
        }

        let value_length = ((bytes.next().unwrap()? as u32) << 24)
            + ((bytes.next().unwrap()? as u32) << 16)
            + ((bytes.next().unwrap()? as u32) << 8)
            + (bytes.next().unwrap()? as u32);

        // if key matches, return result
        if &key == search_key {
            let mut value = Vec::with_capacity(value_length as usize);
            for _ in 0..value_length {
                value.push(bytes.next().unwrap()?);
            }

            return Ok(Some(Entry {
                flags: 0,
                key_length,
                key,
                value_length,
                value,
            }));
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
        if *search_key <= **curr_key {
            if idx == 0 {
                return None;
            }

            let prev_key = &table_meta.blocks[idx - 1].start_key;
            if *search_key > **prev_key {
                return Some(idx);
            }

            max = idx;
            if idx - min == 1 {
                idx = min
            } else {
                idx = idx - ((idx - min) / 2);
            }
        } else {
            if idx >= table_meta.blocks.len() - 1 {
                return None;
            }

            min = idx;
            idx = 1 + idx + (max - idx) / 2;
            
        }
    }
}

#[cfg(test)]
mod find_block_tests {
    use super::super::BlockMeta;
    use super::*;

    #[test]
    fn smoke_test() {
        let mut table_meta = TableMeta::new();
        table_meta.blocks.push(BlockMeta {
            count: 10,
            size: 10,
            size_compressed: 10,
            start_key: "a".bytes().collect(),
        });
        table_meta.blocks.push(BlockMeta {
            count: 10,
            size: 10,
            size_compressed: 10,
            start_key: "c".bytes().collect(),
        });
        table_meta.blocks.push(BlockMeta {
            count: 10,
            size: 10,
            size_compressed: 10,
            start_key: "d".bytes().collect(),
        });

        let search_key: Vec<u8> = "b".bytes().collect();

        assert_eq!(1 as usize, find_block(&search_key, &table_meta).unwrap());
    }

    #[test]
    fn test2() {
        let mut table_meta = TableMeta::new();
        table_meta.blocks.push(BlockMeta {
            count: 10,
            size: 10,
            size_compressed: 10,
            start_key: "a".bytes().collect(),
        });
        table_meta.blocks.push(BlockMeta {
            count: 10,
            size: 10,
            size_compressed: 10,
            start_key: "c".bytes().collect(),
        });
        table_meta.blocks.push(BlockMeta {
            count: 10,
            size: 10,
            size_compressed: 10,
            start_key: "e".bytes().collect(),
        });

        let search_key: Vec<u8> = "d".bytes().collect();
        assert_eq!(2 as usize, find_block(&search_key, &table_meta).unwrap());
    }

    #[test]
    fn test3() {
        let mut table_meta = TableMeta::new();
        table_meta.blocks.push(BlockMeta {
            count: 10,
            size: 10,
            size_compressed: 10,
            start_key: "b".bytes().collect(),
        });
        table_meta.blocks.push(BlockMeta {
            count: 10,
            size: 10,
            size_compressed: 10,
            start_key: "c".bytes().collect(),
        });
        table_meta.blocks.push(BlockMeta {
            count: 10,
            size: 10,
            size_compressed: 10,
            start_key: "e".bytes().collect(),
        });

        let search_key: Vec<u8> = "a".bytes().collect();
        assert_eq!(None, find_block(&search_key, &table_meta));
    }

    #[test]
    fn test4() {
        let mut table_meta = TableMeta::new();
        table_meta.blocks.push(BlockMeta {
            count: 10,
            size: 10,
            size_compressed: 10,
            start_key: "b".bytes().collect(),
        });
        table_meta.blocks.push(BlockMeta {
            count: 10,
            size: 10,
            size_compressed: 10,
            start_key: "c".bytes().collect(),
        });
        table_meta.blocks.push(BlockMeta {
            count: 10,
            size: 10,
            size_compressed: 10,
            start_key: "e".bytes().collect(),
        });

        let search_key: Vec<u8> = "f".bytes().collect();
        assert_eq!(None, find_block(&search_key, &table_meta));
    }
}
