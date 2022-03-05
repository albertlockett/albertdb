use regex::Regex;
use std::fs;
use std::io;
use std::io::{Read, Write};
use std::path;

use crate::memtable;

#[derive(Debug)]
struct WriteEntry {
    flags: u8,
    key_length: u32,
    key: Vec<u8>,
    value_length: u32,
    value: Vec<u8>,
}

pub struct Wal {
    pub id: String,
    file: fs::File,
}

impl Wal {
    pub fn new(id: String) -> Self {
        let filename = wal_filename(&id);
        let path = path::Path::new(&filename);
        let file = fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(path)
            .unwrap();
        Wal { file, id }
    }

    pub fn write(&mut self, key: &[u8], value: Option<&[u8]>) -> io::Result<u32> {
        let write_entry = WriteEntry {
            flags: 0,
            key_length: key.len() as u32,
            key: key.to_owned(),
            value_length: value.unwrap().len() as u32,
            value: value.unwrap().to_owned(),
        };

        // TODO could move this next block into a new func idk
        self.file.write(&[
            // write flags
            write_entry.flags,
            // write key length
            (write_entry.key_length >> 24) as u8,
            (write_entry.key_length >> 16) as u8,
            (write_entry.key_length >> 8) as u8,
            write_entry.key_length as u8,
            // write value length
            (write_entry.value_length >> 24) as u8,
            (write_entry.value_length >> 16) as u8,
            (write_entry.value_length >> 8) as u8,
            write_entry.value_length as u8,
        ])?;
        // write key and value
        self.file.write(&write_entry.key)?;
        self.file.write(&write_entry.value)?;
        self.file.flush()?;

        return Ok(1);
    }

    pub fn read(&self) -> io::Result<bool> {
        let filename = wal_filename(&self.id);
        let path = path::Path::new(&filename);
        let file = fs::OpenOptions::new().read(true).open(path)?;
        let mut bytes = file.bytes();
        // TODO implement the full readback
        let g = bytes.next();
        println!("{:?}", g);
        return Ok(true);
    }

    pub fn delete(&self) -> io::Result<bool> {
        let filename = wal_filename(&self.id);
        let path = path::Path::new(&filename);
        fs::remove_file(path)?;
        return Ok(true);
    }
}

fn wal_filename(id: &str) -> String {
    let filename = format!("/tmp/wal-{}", id);
    return filename;
}

pub struct WalRecovery {
    pub writable_memtable: Option<memtable::Memtable>,
    pub flushing_memtables: Vec<memtable::Memtable>,
}

pub fn recover() -> io::Result<WalRecovery> {
    let data_dir = "/tmp"; // TODO not have this hard-coded

    // for any memtable that was writable during the last shutdown, we'll add
    // values into this new memtable and also create a new recovery wal for the
    // memtable. we'll also be deleting the old memtables as we go
    let mut writable_memtable = memtable::Memtable::new();
    let mut recovery_wal = Wal::new(writable_memtable.id.clone());
    let recovery_wal_filename = &wal_filename(&writable_memtable.id);

    let mut flushing_memtables = vec![];

    for file in fs::read_dir(data_dir).unwrap() {
        let path: Box<path::Path> = file.unwrap().path().into_boxed_path();
        if is_wal(&path) {
            if path::Path::new(&recovery_wal_filename) == &*path {
                continue;
            }

            let memtable = recover_memtable(&path)?;
            let flushing = is_flushing(&path);

            log::debug!(
                "recovered memtable. num_records = {:?}, path = {:?}, flushing = {:?}",
                memtable.size(),
                path,
                flushing
            );
            if !flushing {
                if writable_memtable.size() > 0 {
                    // this is because we don't know which WAL was for more recent
                    // data, so data from the older one could overwrite the newer
                    // TODO actually fix this somehow
                    log::warn!("recovered more than one memtable via WAL that were not in process of flushing - this could lead to invalid recovery state");
                }
                memtable.into_iter().for_each(|(k, v)| {
                    recovery_wal.write(&k, Some(v.as_ref().unwrap())).unwrap();
                    writable_memtable.insert(k, v);
                });
                fs::remove_file(path)?;
            } else {
                flushing_memtables.push(memtable);
            }
        }
    }

    Ok(WalRecovery {
        writable_memtable: Some(writable_memtable),
        flushing_memtables,
    })
}

// check if the file at this path is a WAL
fn is_wal(path: &path::Path) -> bool {
    let re = regex::Regex::new(r".*/wal-.*$").unwrap();
    return re.is_match(path.to_str().unwrap());
}

// check if the path was in the process of flushing when the database shut down last
fn is_flushing(wal_path: &path::Path) -> bool {
    let sstable_data_path = String::from(
        Regex::new("wal-")
            .unwrap()
            .replace(wal_path.to_str().unwrap(), "sstable-data-"),
    );
    let is_flushing = fs::metadata(&path::Path::new(&sstable_data_path)).is_ok();
    return is_flushing;
}

fn recover_memtable(path: &path::Path) -> io::Result<memtable::Memtable> {
    let mut memtable = memtable::Memtable::new();
    let file = fs::OpenOptions::new().read(true).open(path)?;
    let mut bytes = file.bytes();

    loop {
        let flags_1_o = bytes.next();
        if flags_1_o.is_none() {
            return Ok(memtable);
        }

        let key_length = ((bytes.next().unwrap()? as u32) << 24)
            + ((bytes.next().unwrap()? as u32) << 16)
            + ((bytes.next().unwrap()? as u32) << 8)
            + (bytes.next().unwrap()? as u32);

        let value_length = ((bytes.next().unwrap()? as u32) << 24)
            + ((bytes.next().unwrap()? as u32) << 16)
            + ((bytes.next().unwrap()? as u32) << 8)
            + (bytes.next().unwrap()? as u32);

        let mut key: Vec<u8> = Vec::with_capacity(key_length as usize);
        for _ in 0..key_length {
            key.push(bytes.next().unwrap()?);
        }

        let mut value = Vec::with_capacity(value_length as usize);
        for _ in 0..value_length {
            value.push(bytes.next().unwrap()?);
        }

        memtable.insert(key, Some(value));
    }
}
