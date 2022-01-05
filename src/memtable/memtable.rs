use std::rc::Rc;

struct Node<T, U> {
  key: T,
  priority: U,
  left: Link<T, U>,
  right: Link<T, U>,
  parent: Link<T, U>
}

type Link<T, U> = Option<Box<Node<T, U>>>;

pub struct Memtable<T: PartialOrd, U> {
  root: Link<T, U>,
}

impl<T: PartialOrd, U> Memtable<T, U> {
  pub fn new() -> Self {
    Memtable { root: None, }
  }

  pub fn insert(&mut self, key: T, priority: U) {
    // let mut node = &self.root;
    // let newNode = Rc::new(Node {
    //   key,
    //   priority,
    //   left: None,
    //   right: None,
    //   parent: None,
    // });
    
    let x = &self.root;
    if matches!(x, None)  {

    }

    let node = x.as_ref().unwrap();
    let mut parent = &node.parent;
    let mut y = parent.as_ref().unwrap();
    let z = &*y;
    


    // *p2.left = Some(newNode);

    // if newNode.key <= parent.as_ref().unwrap().key {
      // let parentRc = parent.as_deref().unwrap();

      // parent.unwrap().left = Some(newNode);
    // } else {
      // parent.unwrap().right = Some(newNode);
    // }
  }
}

