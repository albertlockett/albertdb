use crate::memtable;
use log;
use std::fs;
use std::path;
use std::io;
use std::io::Write;

pub mod reader;

#[derive(Debug)]
struct Entry {
    flags: u8,
    key_length: u32,
    key: Vec<u8>,
    value_length: u32,
    value: Vec<u8>,
}

//
// TODO
// - delete the WAL
pub fn flush_to_sstable(memtable: &memtable::Memtable) -> io::Result<u32> {
    log::info!("flushing memtable (id: {})", memtable.id);

    let iter = memtable.iter();
    let entries: Vec<Entry> = iter
        .map(|(key, value)| {
            let key_length = key.len() as u32;
            let value_length = value.len() as u32;
            Entry {
                flags: 0,
                key,
                key_length,
                value,
                value_length,
            }
        })
        .collect();

    let filename = format!("/tmp/sstable-{}", memtable.id);
    let path = path::Path::new(&filename);
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)?;

    for entry in entries {
        println!("{:?}", entry);
        file.write(&[entry.flags])?;
        file.write(&[
            (entry.key_length >> 24) as u8,
            (entry.key_length >> 16) as u8,
            (entry.key_length >> 8) as u8,
            entry.key_length as u8,
        ])?;
        file.write(&entry.key)?;

        file.write(&[
            (entry.value_length >> 24) as u8,
            (entry.value_length >> 16) as u8,
            (entry.value_length >> 8) as u8,
            entry.value_length as u8,
        ])?;
        file.write(&entry.value)?;
    }

    file.flush()?;

    return Ok(1);
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
