use crate::memtable;

pub struct Engine {
    writable_table: memtable::Memtable,
    flushing_memtables: Vec<memtable::Memtable>,
}

impl Engine {
    pub fn new() -> Self {
        Engine {
            writable_table: memtable::Memtable::new(),
            flushing_memtables: vec![],
        }
    }

    pub fn write(&mut self, key: &[u8], value: &[u8]) {
        self.writable_table.insert(key.to_vec(), value.to_vec());

        if self.writable_table.size() > 3 {
            let mut tmp = memtable::Memtable::new();
            std::mem::swap(&mut self.writable_table, &mut tmp);
            self.flushing_memtables.push(tmp);
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
