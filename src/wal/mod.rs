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
        let filename = format!("/tmp/wal-{}", id);
        let path = path::Path::new(&filename);
        let file = fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(path)
            .unwrap();
        Wal { file, id }
    }

    pub fn write(&mut self) -> io::Result<u32> {
        self.file.write(&[1])?;
        return Ok(1);
    }

    pub fn read(&self) -> io::Result<bool> {
        let filename = format!("/tmp/wal-{}", self.id);
        let path = path::Path::new(&filename);
        let file = fs::OpenOptions::new().read(true).open(path)?;
        let mut bytes = file.bytes();

        let g = bytes.next();
        println!("{:?}", g);
        return Ok(true);
    }
}
