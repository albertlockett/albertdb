use std::sync::Arc;
use std::sync::RwLock;

pub struct Node {
  key: Vec<u8>,
  value: Vec<u8>,
  priority: f64,
  left: Option<Arc<RwLock<Node>>>,
}

pub struct Memtable {
  root: Option<Node>,
  size: u32,
}

impl Memtable {
  pub fn new() -> Self {
    Memtable {
      root: None,
      size: 0
    }
  }

  pub fn insert(&mut self) {

  }
}