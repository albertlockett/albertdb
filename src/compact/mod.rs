use regex::Regex;
use std::fs;
use std::io;
use std::path;

use crate::config;
use crate::sstable;

fn compact(config: &config::Config, level: u8) {
    let compact_candidates = find_compact_candidates(config, level);
}

fn find_compact_candidates(config: &config::Config, level: u8) -> io::Result<Vec<Box<path::Path>>> {
    let mut results: Vec<Box<path::Path>> = vec![];
    let files: fs::ReadDir = fs::read_dir(&config.data_dir)?;

    let mut total_size = 0u64;

    for file in files {
        let file_path = file.unwrap().path();
        if !is_sstable(&file_path) {
            continue
         }
         if is_flushing(&file_path) {
            continue;
        }

        let meta_path = to_metadata_path(&file_path);
        let table_meta = read_table_meta(&path::Path::new(&meta_path));
        total_size += table_meta.table_size_compressed();
        results.push(file_path.into_boxed_path());
    }

    if total_size < config.compaction_threshold {
        return Ok(vec![]);
    }

    Ok(results)
}

#[cfg(test)]
mod find_compact_candidates_tets {
    use super::*;
    use crate::memtable;

    #[test]
    fn it_can_choose_the_right_tables_to_compact() {
        let data_dir =
            "/tmp/compact_find_compact_candidates_tets/it_can_choose_the_right_tables_to_compact";
        fs::remove_dir_all(data_dir);
        fs::create_dir_all(data_dir).unwrap();

        let mut config = config::Config::new();
        config.data_dir = String::from(data_dir);
        config.compaction_threshold = 1;

        let mut memtable1 = memtable::Memtable::new();
        memtable1.insert("abc".bytes().collect(), Some("abc".bytes().collect()));
        assert_eq!(true, sstable::flush_to_sstable(&config, &memtable1).is_ok());

        let mut memtable2 = memtable::Memtable::new();
        memtable2.insert("abc".bytes().collect(), Some("abc".bytes().collect()));
        assert_eq!(true, sstable::flush_to_sstable(&config, &memtable2).is_ok());

        let results_r = find_compact_candidates(&config, 0);
        assert_eq!(true, results_r.is_ok());
        let results = results_r.unwrap();
        assert_eq!(2, results.len());

        fs::remove_dir_all(data_dir).unwrap();
    }
}

// TODO this could be a util function as it's shared w/ sstable module (reader)
fn is_sstable(path: &path::Path) -> bool {
    let re = regex::Regex::new(r".*/sstable-data.*$").unwrap();
    re.is_match(path.to_str().unwrap())
}

// TODO this could be a util function as it's shared with WAL module (but its backwards)
// check if the path was in the process of flushing when the database shut down last
fn is_flushing(sstable_path: &path::Path) -> bool {
    let sstable_data_path = String::from(
        Regex::new("stable-data-")
            .unwrap()
            .replace(sstable_path.to_str().unwrap(), "wal-"),
    );
    let is_flushing = fs::metadata(&path::Path::new(&sstable_data_path));
    return is_flushing.is_ok();
}

// TODO this could be a util function as it's shared w/ sstable module (reader)
fn to_metadata_path(path: &path::Path) -> String {
    let meta_path = String::from(
        Regex::new("sstable-data")
            .unwrap()
            .replace(path.to_str().unwrap(), "sstable-meta"),
    );
    return meta_path;
}

// TODO make this a util function casue it's shared w/ sstable module (reader)
fn read_table_meta(path: &path::Path) -> sstable::TableMeta {
    let file = fs::OpenOptions::new().read(true).open(path);
    match file {
        Ok(file) => {
            let result: sstable::TableMeta = serde_yaml::from_reader(file).unwrap();
            return result;
        }
        Err(err) => {
            log::error!(
                "An error happened reading sstable meta at {:?}: {:?}",
                path,
                err
            );
            panic!("could not read table meta, invalid state");
        }
    };
}
