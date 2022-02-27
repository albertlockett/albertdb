use log;
use std::fs;
use std::io;
use std::path;

use std::io::Read;
use std::io::Write;

use super::Entry;

// TODO
// - some implementation to let us know that there is a newly flushd sstable

pub struct Reader {
    sstables: Vec<Box<path::Path>>,
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
                self.sstables.push(path);
            }
        }

        log::info!("initialized with {} memtables", self.sstables.len());
        log::debug!("memtables: {:?}", self.sstables);
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

fn is_sstable(path: &path::Path) -> bool {
    let re = regex::Regex::new(r".*/sstable-.*$").unwrap();
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
