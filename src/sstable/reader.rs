use flate2::read::GzDecoder;
use log;
use regex::Regex;
use std::fs;
use std::io;
use std::io::{Read, Seek};
use std::path;

use super::{BlockMeta, Entry, TableMeta};

// TODO
// - some implementation to let us know that there is a newly flushd sstable

pub struct Reader {
    sstables: Vec<(TableMeta, Box<path::Path>)>,
}

impl Reader {
    pub fn new() -> Self {
        Reader { sstables: vec![] }
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

                self.sstables.push((table_meta, path));
            }
        }

        log::info!("initialized with {} memtables", self.sstables.len());
    }

    pub fn find(&self, key: &[u8]) -> Option<Vec<u8>> {
        for (table_meta, path) in &self.sstables {
            let block = find_block(key, table_meta);
            if block.is_none() {
                continue;
            }

            log::debug!("searching for '{:?}' in '{:?}", key, path);
            let result = find_from_table(key, path, &table_meta.blocks[block.unwrap()]);
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
) -> io::Result<Option<Entry>> {
    let bytes1 = deserialize_block(path, block)?;
    let mut bytes = bytes1.into_iter().map(|b| Ok::<u8, io::Error>(b));

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

        if *search_key == **curr_key {
            return Some(idx);
        }

        if *search_key < **curr_key {
            if idx - min == 1 {
                let prev_key = &table_meta.blocks[idx - 1].start_key;
                if **prev_key < *search_key {
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
                if **last_key < *search_key {
                    return Some(table_meta.blocks.len() - 1)
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
mod find_block_tests {
    use super::super::BlockMeta;
    use super::*;

    #[test]
    fn test5() {
        let mut table_meta = TableMeta::new();
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
        let mut table_meta = TableMeta::new();
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
        let mut table_meta = TableMeta::new();
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
        let mut table_meta = TableMeta::new();
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
        let mut table_meta = TableMeta::new();
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
        let mut table_meta = TableMeta::new();
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
