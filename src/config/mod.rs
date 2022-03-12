use serde::Deserialize;

#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    // path on disk where data files will be stored
    pub data_dir: String,

    // number of records that will be in a memtable before it is flushed to disk
    pub memtable_max_count: u32,

    // approximate size of compressed blocks in sstables
    pub sstable_block_size: u32,
}

impl Config {
    pub fn new() -> Self {
        // TODO initialize this somehow
        Config {
            data_dir: String::from("/tmp"),
            memtable_max_count: 1024,
            sstable_block_size: 1024,
        }
    }
}
