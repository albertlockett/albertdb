use log;
use std::sync::mpsc;
use std::sync::Arc;
use std::sync::{Mutex, RwLock};
use std::thread;

use crate::config;
use crate::memtable;
use crate::sstable;
use crate::wal;

pub struct Engine {
    config: config::Config,
    flush_sender: Mutex<mpsc::Sender<Arc<memtable::Memtable>>>,
    writable_table: memtable::Memtable,
    writable_wal: wal::Wal,
    flushing_memtables: Arc<RwLock<Vec<Arc<memtable::Memtable>>>>,
    sstable_reader: Arc<RwLock<sstable::reader::Reader>>,
}

impl Engine {
    // TODO consider whether it's a dumbo thing to do to do all this init stuff in constructor
    pub fn new() -> Self {
        // derive init state from the WAL that are on disk
        // TODO handle this error
        let mut wal_recovery = wal::recover().unwrap();

        // when we want to flush a memtable, we send a pointer to it in this channel
        let (flush_sender, flush_receiver) = mpsc::channel::<Arc<memtable::Memtable>>();

        // setup the memtable we'll be putting new writes into and the WAL
        let mut memtable = memtable::Memtable::new();
        if wal_recovery.writable_memtable.is_some() {
            let recovered_memtable: &mut memtable::Memtable =
                wal_recovery.writable_memtable.as_mut().unwrap();
            std::mem::swap(&mut memtable, recovered_memtable);
        }
        let wal = wal::Wal::new(memtable.id.clone());

        // setup the thing to read from sstables (on disk)
        let mut sstable_reader = sstable::reader::Reader::new();
        sstable_reader.init();
        let sstable_reader_ptr = Arc::new(RwLock::new(sstable_reader));

        // setup out list of memtables that we'll be reading from while they're still in the
        //process of beling flushed to disk
        let mut flushing_memtables = vec![];
        wal_recovery.flushing_memtables.into_iter().for_each(|v| {
            let mt_ptr = Arc::new(v);
            flushing_memtables.push(mt_ptr.clone());
        });
        let flushing_memtables_ptr = Arc::new(RwLock::new(flushing_memtables));

        let config = config::Config::new();

        // finally create the engine
        let engine = Engine {
            config: config.clone(),
            sstable_reader: sstable_reader_ptr.clone(),
            flushing_memtables: flushing_memtables_ptr.clone(),
            flush_sender: Mutex::new(flush_sender),
            writable_table: memtable,
            writable_wal: wal,
        };

        // setup handlir for sending the memtables to be flushed and update internal state
        let _handle = thread::spawn(move || {
            while let Ok(value) = flush_receiver.recv() {
                // flush the memtable
                sstable::flush_to_sstable(&config, &value).unwrap();

                // delete the WAL
                let wal = wal::Wal::new(value.id.clone());
                wal.delete().unwrap(); // TODO could handle this error

                // signal to the reader that there's a new memtable to read
                let mut reader = sstable_reader_ptr.write().unwrap();
                reader.add_memtable(&value);

                // remove the memtable from the list of flushing memtables
                let mut memtables = flushing_memtables_ptr.write().unwrap();
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

        // send all the flushing memtables that we read during tecovery to be flushed..
        // the flush didn't complete before the last shutdown so we'll retry it
        engine
            .flushing_memtables
            .read()
            .unwrap()
            .iter()
            .for_each(|v| {
                engine.flush_sender.lock().unwrap().send(v.clone()).unwrap();
            });

        return engine;
    }

    pub fn write(&mut self, key: &[u8], value: &[u8]) {
        self.writable_wal.write(key, Some(value)).unwrap();
        self.writable_table
            .insert(key.to_vec(), Some(value.to_vec()));
        if self.writable_table.size() > self.config.memtable_max_count {
            self.flush_writable_memtable();
        }
    }

    pub fn delete(&mut self, key: &[u8]) {
        self.writable_wal.write(key, None).unwrap();
        self.writable_table.insert(key.to_vec(), None);
        if self.writable_table.size() > self.config.memtable_max_count {
            self.flush_writable_memtable();
        }
    }

    fn flush_writable_memtable(&mut self) {
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
        flush_result.unwrap(); // TODO could handle this
    }

    pub fn find(&self, key: &[u8]) -> Option<Vec<u8>> {
        log::debug!("searching for key {:?}", key);
        let (val_found, found) = self.writable_table.search(key);
        if val_found.is_some() {
            if log::log_enabled!(log::Level::Debug) {
                log::debug!(
                    "found '{:?}' in writable memtable (id: {}). value: '{:?}'",
                    key,
                    self.writable_table.id,
                    val_found.as_ref().unwrap()
                );
            }
            return val_found;
        };
        if found {
            return None;
        }

        let mts: &Vec<Arc<memtable::Memtable>> = &self.flushing_memtables.read().unwrap();
        for mt in mts {
            let (val_found, found) = mt.search(&key);
            if val_found.is_some() {
                if log::log_enabled!(log::Level::Debug) {
                    log::debug!(
                        "found '{:?}' in flushing memtable (id: {}). value: '{:?}'",
                        key,
                        mt.id,
                        val_found.as_ref().unwrap()
                    );
                }
                return val_found;
            }
            if found {
                return None;
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
