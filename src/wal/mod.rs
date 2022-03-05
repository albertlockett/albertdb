use std::fs;
use std::io;
use std::io::{Read, Write};
use std::path;

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

    pub fn write(&mut self, key: &[u8], value: &[u8]) -> io::Result<u32> {
        let write_entry = WriteEntry {
            flags: 0,
            key_length: key.len() as u32,
            key: key.to_owned(),
            value_length: value.len() as u32,
            value: value.to_owned(),
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
        return Ok(true)
    }

}

fn wal_filename(id: &str) -> String {
    let filename = format!("/tmp/wal-{}", id);
    return filename;
}
