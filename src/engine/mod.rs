use std::sync::mpsc;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;

use crate::memtable;
use crate::sstable;
use crate::wal;

pub struct Engine {
    _handle: std::thread::JoinHandle<()>,
    sender: Mutex<mpsc::Sender<Arc<memtable::Memtable>>>,
    writable_table: memtable::Memtable,
    writable_wal: wal::Wal,
    flushing_memtables: Vec<Arc<memtable::Memtable>>,
}

impl Engine {
    pub fn new() -> Self {
        let (sender, receiver) = mpsc::channel::<Arc<memtable::Memtable>>();
        let _handle = thread::spawn(move || {
            while let Ok(value) = receiver.recv() {
                println!("flushing memtable!!");
                sstable::flush_to_sstable(&value).unwrap();
            }
        });
        let memtable = memtable::Memtable::new();
        let wal = wal::Wal::new(memtable.id.clone());
        Engine {
            _handle,
            sender: Mutex::new(sender),
            writable_table: memtable,
            writable_wal: wal,
            flushing_memtables: vec![],
        }
    }

    pub fn write(&mut self, key: &[u8], value: &[u8]) {
        self.writable_wal.write().unwrap();
        self.writable_table.insert(key.to_vec(), value.to_vec());

        if self.writable_table.size() > 3 {
            let mut tmp = memtable::Memtable::new();
            let mut new_wal = wal::Wal::new(tmp.id.clone());
            std::mem::swap(&mut self.writable_table, &mut tmp);
            std::mem::swap(&mut self.writable_wal, &mut new_wal);

            println!("flushing a bitch {:?}", tmp);
            let mt_pointer = Arc::new(tmp);
            self.flushing_memtables.push(mt_pointer.clone());
            let sender = self.sender.lock().unwrap();
            let flush_result = sender.send(mt_pointer.clone());
            println!("flush send result = {:?}", flush_result);
        }
    }

    pub fn find(&self, key: &[u8]) -> Option<Vec<u8>> {
        let found = self.writable_table.search(key);
        if found.is_some() {
            return found;
        };

        for mt in &self.flushing_memtables {
            let found = mt.search(&key);
            if found.is_some() {
                return found;
            }
        }

        return None;
    }
}
