use crate::memtable;

pub struct MemtableManager {
    writable_table: memtable::Memtable,
    flushing_memtables: Vec<memtable::Memtable>,
}

impl MemtableManager {
    pub fn new() -> Self {
        MemtableManager {
            writable_table: memtable::Memtable::new(),
            flushing_memtables: vec![],
        }
    }

    pub fn write(&mut self) {
        let key = vec![1, 2, 3];
        let priority = 0.5f64;
        self.writable_table.insert(key, priority);

        if self.writable_table.size() > 3 {
            let mut tmp = memtable::Memtable::new();
            std::mem::swap(&mut self.writable_table, &mut tmp);
            self.flushing_memtables.push(tmp);
        }
    }

    pub fn find(&self) -> bool {
        let key = vec![1, 2, 3];
        let found = self.writable_table.search(&key);
        if found {
            return found;
        };

        for mt in &self.flushing_memtables {
            let found = mt.search(&key);
            if found {
                return found;
            }
        }

        return false;
    }
}
