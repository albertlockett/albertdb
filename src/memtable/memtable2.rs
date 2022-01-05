struct Node<T> {
  key: T,
  left: Link<T>
}

type Link<T> = Option<Box<Node<T>>>;

struct Memtable2<T> {
  root: Link<T>
}

impl<T> Memtable2<T> {

  pub fn insert(&mut self, key: T) {
    let new_node = Box::new(Node {
      key: key,
      left: None
    });
    
    self.root = Some(new_node);
  }

  pub fn insert2(&mut self, key: T) {
    let mut parent: Option<&mut Node<T>> = None;

    if !matches!(&self.root, None) {
      let rootO: Link<T> = self.root.take();
      let mut rootB: Box<Node<T>> = rootO.unwrap();
      let x: &mut Node<T> = &mut *rootB;
      parent = Some(x);
    }
  }

  pub fn insert3(&mut self, key: T) {
    let rootO: Link<T> = self.root.take();
    let mut rootB: Box<Node<T>> = rootO.unwrap();
    let node: &mut Node<T> = &mut *rootB;
    Self::update_node(node)
  }

  fn update_node(node: &mut Node<T>) {
    node.left = None;
  }

  pub fn insert4(&mut self, key: T) {
    let new_node = Box::new(Node{
      key: key,
      left: None,
    });

    if matches!(&self.root, None) {
      self.root = Some(new_node);
      return;
    }

    let mut parent: &mut Node<T> = self.root.as_mut().unwrap();
    loop {
      if matches!(parent.left, None) {
        parent.left = Some(new_node);
        break;
      } else {
        parent = parent.left.as_mut().unwrap();
      }
    }
  }

}