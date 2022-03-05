use log;
use std::sync::mpsc;
use std::sync::Arc;
use std::sync::{Mutex, RwLock};
use std::thread;

use crate::memtable;
use crate::sstable;
use crate::wal;

pub struct Engine {
    // _handle: std::thread::JoinHandle<()>,
    flush_sender: Mutex<mpsc::Sender<Arc<memtable::Memtable>>>,
    writable_table: memtable::Memtable,
    writable_wal: wal::Wal,
    flushing_memtables: Arc<RwLock<Vec<Arc<memtable::Memtable>>>>,
    sstable_reader: Arc<RwLock<sstable::reader::Reader>>,
}

impl Engine {
    pub fn new() -> Self {
        let (flush_sender, flush_receiver) = mpsc::channel::<Arc<memtable::Memtable>>();

        let memtable = memtable::Memtable::new();
        let wal = wal::Wal::new(memtable.id.clone());

        let mut sstable_reader = sstable::reader::Reader::new();
        sstable_reader.init();
        let sstable_reader_ptr = Arc::new(RwLock::new(sstable_reader));

        let flushing_memtables = Arc::new(RwLock::new(vec![]));

        let engine = Engine {
            // _handle,
            sstable_reader: sstable_reader_ptr.clone(),
            flushing_memtables: flushing_memtables.clone(),
            flush_sender: Mutex::new(flush_sender),
            writable_table: memtable,
            writable_wal: wal,
        };

        // handle sending the memtables to be flushed and update internal state
        let _handle = thread::spawn(move || {
            while let Ok(value) = flush_receiver.recv() {
                // flush the memtable
                sstable::flush_to_sstable(&value).unwrap();

                // delete the WAL
                let wal = wal::Wal::new(value.id.clone());
                wal.delete().unwrap(); // TODO could handle this error

                // signal to the reader that there's a new memtable to read
                let mut reader = sstable_reader_ptr.write().unwrap();
                reader.add_memtable(&value);

                // remove the memtable from the list of flushing memtables
                let mut memtables = flushing_memtables.write().unwrap();
                let position_o = memtables.iter().position(|v| Arc::ptr_eq(v, &value));
                if position_o.is_some() {
                    let position = position_o.unwrap();
                    log::debug!(
                        "removing flushing memtable at position {:?}. There are now {:?} flushing memtables", 
                        position,
                        memtables.len() - 1
                    );
                    memtables.remove(position);
                }
            }
        });

        return engine;
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
            self.flushing_memtables
                .write()
                .unwrap()
                .push(mt_pointer.clone());
            let sender = self.flush_sender.lock().unwrap();
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

        let mts: &Vec<Arc<memtable::Memtable>> = &self.flushing_memtables.read().unwrap();

        for mt in mts {
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

        let disk_result = self.sstable_reader.read().unwrap().find(key);
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
