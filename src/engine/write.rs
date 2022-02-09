use serde::Deserialize;
use std::sync::mpsc;
use std::thread;

use crate::memtable;
use crate::sstable;

const memtable_size_threshold: u32 = 10;

#[derive(Clone, Debug, Deserialize)]
pub struct WritePayload {
    key: String,
    value: String,
}

pub struct WriteEngine {
    // pub sender: mpsc::Sender<WritePayload>,
    pub reciever: mpsc::Receiver<WritePayload>,
    pub memtable: memtable::Memtable,
}

impl WriteEngine {
    pub fn new() -> Self {
        let (sender, reciever) = std::sync::mpsc::channel::<WritePayload>();
        let write_engine = WriteEngine {
            // sender,
            reciever,
            memtable: memtable::Memtable::new(),
        };
        return write_engine;
    }

    pub fn start(&mut self) {
        while let Ok(payload) = self.reciever.recv() {
            let key = payload.key.clone().into_bytes();
            let value = payload.value.clone().into_bytes();
            self.handle_write(&key, &value);
        }
    }

    pub fn handle_write(&mut self, key: &Vec<u8>, value: &Vec<u8>) {
        self.memtable.insert(key.clone(), 64.0);
        println!("{:?}, {:?}", self.memtable.size(), self.memtable);

        if self.memtable.size() > memtable_size_threshold {
            let mut flushing_memtable = memtable::Memtable::new();
            std::mem::swap(&mut self.memtable, &mut flushing_memtable);
            let result = sstable::flush_to_sstable(&flushing_memtable);
            println!("{:?}", result);
        }
    }
}
