use log;
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
    sstable_reader: sstable::reader::Reader,
}

impl Engine {
    pub fn new() -> Self {
        let (sender, receiver) = mpsc::channel::<Arc<memtable::Memtable>>();
        let _handle = thread::spawn(move || {
            while let Ok(value) = receiver.recv() {
                sstable::flush_to_sstable(&value).unwrap();
            }
        });
        let memtable = memtable::Memtable::new();
        let wal = wal::Wal::new(memtable.id.clone());
        let mut sstable_reader = sstable::reader::Reader::new();
        sstable_reader.init();

        Engine {
            _handle,
            sstable_reader,
            sender: Mutex::new(sender),
            writable_table: memtable,
            writable_wal: wal,
            flushing_memtables: vec![],
        }
    }

    pub fn write(&mut self, key: &[u8], value: &[u8]) {
        self.writable_wal.write(key, value).unwrap();
        self.writable_table.insert(key.to_vec(), value.to_vec());

        // TODO memtable size needs to be configurable
        if self.writable_table.size() > 3 {
            let mut tmp = memtable::Memtable::new();
            let mut new_wal = wal::Wal::new(tmp.id.clone());
            std::mem::swap(&mut self.writable_table, &mut tmp);
            std::mem::swap(&mut self.writable_wal, &mut new_wal);

            log::debug!("sending memtable to flush (id: {:?})", tmp.id);
            let mt_pointer = Arc::new(tmp);
            self.flushing_memtables.push(mt_pointer.clone());
            let sender = self.sender.lock().unwrap();
            let flush_result = sender.send(mt_pointer.clone());
            
            // TODO need to handle this result
            println!("flush send result = {:?}", flush_result);
        }
    }

    pub fn find(&self, key: &[u8]) -> Option<Vec<u8>> {
        log::debug!("searching for key {:?}", key);
        let found = self.writable_table.search(key);
        if found.is_some() {
            if log::log_enabled!(log::Level::Debug) {
                log::debug!(
                    "found '{:?}' in writable memtable (id: {}). value: '{:?}'",
                    key,
                    self.writable_table.id,
                    found.as_ref().unwrap()
                );
            }
            return found;
        };

        for mt in &self.flushing_memtables {
            let found = mt.search(&key);
            if found.is_some() {
                if log::log_enabled!(log::Level::Debug) {
                    log::debug!(
                        "found '{:?}' in flushing memtable (id: {}). value: '{:?}'",
                        key,
                        mt.id,
                        found.as_ref().unwrap()
                    );
                }
                return found;
            }
        }

        let disk_result = self.sstable_reader.find(key);
        if disk_result.is_some() {
            if log::log_enabled!(log::Level::Debug) {
                log::debug!(
                    "found '{:?}' in sstable. value: '{:?}'",
                    key,
                    disk_result.as_ref().unwrap()
                );
            }
            return disk_result;
        }

        log::debug!("key '{:?}' not found", key);
        return None;
    }
}
