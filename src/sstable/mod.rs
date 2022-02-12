use crate::memtable;
use std::fs;
use std::path;

use std::io;
use std::io::Read;
use std::io::Write;

#[derive(Debug)]
struct Entry {
    flags: u8,
    key_length: u32,
    key: Vec<u8>,
    // value_length: u32,
    // value: Vec<u8>,
}

pub fn flush_to_sstable(memtable: &memtable::Memtable) -> io::Result<u32> {
    let iter = memtable.iter();
    let entries: Vec<Entry> = iter
        .map(|key| {
            let key_length = key.len() as u32;
            Entry {
                flags: 0,
                key,
                key_length,
            }
        })
        .collect();

    let path = path::Path::new("/tmp/sstable1");
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)?;

    for entry in entries {
        file.write(&[entry.flags])?;
        let key_length = [
            (entry.key_length >> 24) as u8,
            (entry.key_length >> 16) as u8,
            (entry.key_length >> 8) as u8,
            entry.key_length as u8,
        ];
        file.write(&key_length)?;
        file.write(&entry.key)?;
    }

    file.flush()?;

    return Ok(1);
}

fn find_entry(search_key: &Vec<u8>) -> io::Result<Option<Entry>> {
    let path = path::Path::new("/tmp/sstable1");
    let file = fs::OpenOptions::new().read(true).open(path)?;

    let mut bytes = file.bytes();

    loop {
        let flags_1_option = bytes.next();
        if flags_1_option.is_none() {
            return Ok(None);
        }
        // TODO do something with flags_1

        let key_length = ((bytes.next().unwrap()? as u32) << 24)
            + ((bytes.next().unwrap()? as u32) << 16)
            + ((bytes.next().unwrap()? as u32) << 8)
            + (bytes.next().unwrap()? as u32);

        let mut key: Vec<u8> = Vec::with_capacity(key_length as usize);
        for _ in 0..key_length {
            key.push(bytes.next().unwrap()?);
        }

        if &key == search_key {
            return Ok(Some(Entry {
                flags: 0,
                key_length,
                key,
            }));
        }
    }
}

#[cfg(test)]
mod flush_to_sstable_tests {
    use super::*;
    use crate::memtable;

    #[test]
    fn smoke_test() {
        let mut m = memtable::Memtable::new();
        m.insert("abc".bytes().collect(), "abc".bytes().collect());
        m.insert("def".bytes().collect(), "dev".bytes().collect());
        let result = flush_to_sstable(&m);
        println!("{:?} {:?}", m, result);
    }
}

#[cfg(test)]
mod find_entry_tests {
    use super::*;
    use crate::memtable;

    #[test]
    fn smoke_test() {
        let search_key = "abc".bytes().collect();
        let result = find_entry(&search_key);
        println!("{:?}", result);

        let search_key = "def".bytes().collect();
        let result = find_entry(&search_key);
        println!("{:?}", result);

        let search_key = "eee".bytes().collect();
        let result = find_entry(&search_key);
        println!("{:?}", result);
    }
}
