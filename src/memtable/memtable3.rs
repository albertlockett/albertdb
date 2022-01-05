struct Node<T: PartialOrd, U: PartialOrd> {
  key: T,
  priority: U,
  left: Link<T, U>,
  right: Link<T, U>,
}

type Link<T, U> = Option<Box<Node<T, U>>>;

struct Memtable3<T: PartialOrd, U: PartialOrd> {
  root: Link<T, U>
}

impl<T, U> Memtable3<T, U> where T: PartialOrd, U: PartialOrd {
  pub fn insert(&mut self, key: T, priority: U) {
    let new_node = Box::new(Node {
      key,
      priority,
      left: None,
      right: None
    });

    if matches!(&self.root, None) {
      self.root = Some(new_node);
      return;
    }

    let mut parent: &mut Node<T, U> = self.root.as_mut().unwrap();
    loop {
      if new_node.priority <= parent.priority {
        if matches!(parent.left, None) {
          parent.left = Some(new_node);
          break;
        } else {
          parent = parent.left.as_mut().unwrap();
        }
      } else {
        if matches!(parent.right, None) {
          parent.right = Some(new_node);
          break;
        } else {
          parent = parent.right.as_mut().unwrap();
        }
      }
    }
  }
}