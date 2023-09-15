use log;
use serde::Deserialize;
use std::fs;

#[derive(Clone, Debug, Default, Deserialize)]
pub struct Config {
    // id of the node
    pub node_id: String,

    // path on disk where data files will be stored
    pub data_dir: String,

    // number of records that will be in a memtable before it is flushed to disk
    pub memtable_max_count: u32,

    // approximate size of compressed blocks in sstables
    pub sstable_block_size: u32,

    // size of sstables on disk before they will be compacted
    pub compaction_threshold: u64,

    // how often we check if we should compact tables (millis)
    pub compaction_check_period: u64,

    // number of levels for leveld compaction
    pub compaction_max_levels: u8,

    // the port to listen on for http front-end
    pub http_listen_port: u32,

    // the port the ring topology server will listen on
    pub ring_svc_listen_port: u32,

    // list of seed nodes to connect to when trying to join cluster
    pub ring_svc_seed_nodes: Vec<String>,

    // hostname for this node that is broadcast to other nodes
    pub ring_svc_broadcast_host: String,
}

impl Config {
    pub fn new() -> Self {
        // TODO initialize this somehow & choose more reasonable defaults
        Config {
            node_id: String::from("standalone"),
            data_dir: String::from("/tmp"),
            memtable_max_count: 3,
            sstable_block_size: 64,
            compaction_threshold: 256,
            compaction_check_period: 30000,
            compaction_max_levels: 4,
            http_listen_port: 4000,
            ring_svc_listen_port: 5147,
            ring_svc_seed_nodes: vec!["http://127.0.0.1:5147".to_owned()],
            ring_svc_broadcast_host: String::from("127.0.0.1"),
        }
    }

    pub fn from_file(x: &str) -> Self {
        let file_o = fs::OpenOptions::new().read(true).open(x);
        if file_o.is_err() {
            log::error!("No file can be found {}", x);
        }

        let config: Config = serde_yaml::from_reader(file_o.unwrap()).unwrap();
        return config;
    }
}
