use std::rc::Rc;
use std::cell::RefCell;
use std::fmt::Debug;

struct Node<T: PartialOrd, U: PartialOrd> {
  key: T,
  priority: U,
  left: Link<T, U>,
  right: Link<T, U>,
  parent: Link<T, U>,
}

trait NodeStuff<T, U> where T: PartialOrd + Debug, U: PartialOrd + Debug {
  fn get_parent(&self) -> Link<T, U>;

  fn set_parent(&self, parent: Link<T, U>);

  fn get_left(&self) -> Link<T, U>;

  fn set_left(&self, new_left: Link<T, U>);

  fn get_right(&self) -> Link<T, U>;

  fn set_right(&self, new_right: Link<T, U>);

  fn is_left_child(&self, child: Link<T, U>) -> bool;
  
  fn is_right_child(&self, child: Link<T, U>) -> bool;

  fn is_parent(&self, child: Link<T, U>) -> bool;
}

impl<T, U> std::fmt::Debug for Node<T, U> where T: PartialOrd + Debug, U: PartialOrd + Debug {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
      f.debug_struct("Node")
       .field("key", &self.key)
       .field("priority", &self.priority)
       .field("left", &self.left)
       .field("right", &self.right)
       .field("parent", match &self.parent {
         Some(_x) => {
           &"some"
         },
         None => {
           &"none"
         }
       })
       .finish()
  }
}

type Link<T, U> = Option<Rc<RefCell<Node<T, U>>>>;

impl<T, U> NodeStuff<T, U> for Rc<RefCell<Node<T, U>>> where T: PartialOrd + Debug, U: PartialOrd + Debug {
  fn get_parent(&self) -> Link<T, U> {
    if matches!(self.borrow().parent, None) {
      return None;
    }
    return Some(self.borrow_mut().parent.as_mut().unwrap().clone());
  }

  fn set_parent(&self, parent: Link<T, U>) {
    let node = self.clone();
    node.borrow_mut().parent = parent; 
  }

  fn get_left(&self) -> Link<T, U>{
    if matches!(self.borrow().left, None) {
      return None
    }
    return Some(self.borrow_mut().left.as_mut().unwrap().clone());
  }

  fn set_left(&self, new_left: Link<T, U>) {
    let node = self.clone();
    node.borrow_mut().left = new_left;
  }

  fn get_right(&self) -> Link<T, U>{
    if matches!(self.borrow().right, None) {
      return None
    }
    return Some(self.borrow_mut().right.as_mut().unwrap().clone());
  }

  fn set_right(&self, new_right: Link<T, U>) {
    let node = self.clone();
    node.borrow_mut().right = new_right;
  }

  fn is_left_child(&self, child: Link<T, U>) -> bool {
    if matches!(self.borrow().left, None) {
      return matches!(child, None);
    }

    if matches!(child, None) {
      return matches!(self.borrow().left, None)
    }

    let my_child = self.borrow().left.as_ref().unwrap().clone();
    return Rc::ptr_eq(&my_child, &child.unwrap())
  }

  fn is_right_child(&self, child: Link<T, U>) -> bool {
    if matches!(self.borrow().right, None) {
      return matches!(child, None);
    }

    if matches!(child, None) {
      return matches!(self.borrow().right, None)
    }

    let my_child = self.borrow().right.as_ref().unwrap().clone();
    return Rc::ptr_eq(&my_child, &child.unwrap())
  }

  fn is_parent(&self, parent: Link<T, U>) -> bool {
    if matches!(self.borrow().parent, None) {
      return matches!(parent, None);
    }
    if matches!(parent, None) {
      return matches!(self.borrow().parent, None)
    }
    let my_parent = self.borrow().parent.as_ref().unwrap().clone();
    return Rc::ptr_eq(&my_parent, &parent.unwrap())
  }
}

#[derive(Debug)]
pub struct Memtable3<T: PartialOrd + Debug, U: PartialOrd + Debug> {
  root: Link<T, U>
}

impl<T, U> Memtable3<T, U> where T: PartialOrd + Debug, U: PartialOrd + Debug{

  pub fn new() -> Self {
    Memtable3 {
      root: None
    }
  }

  pub fn insert(&mut self, key: T, priority: U) {
    let new_node = Rc::new(RefCell::new(Node {
      key,
      priority,
      left: None,
      right: None,
      parent: None
    }));

    if matches!(&self.root, None) {
      self.root = Some(new_node.clone());
      return;
    }

    let mut parent = self.root.as_mut().unwrap().clone();
    loop {
      if new_node.borrow().key <= parent.borrow().key {
        if matches!(parent.borrow().left, None) {
          parent.borrow_mut().left = Some(new_node.clone());
          break;
        } else {
          parent = parent.clone().borrow_mut().left.as_mut().unwrap().clone();
        }
      } else {
        if matches!(parent.borrow().right, None) {
          parent.borrow_mut().right = Some(new_node.clone());
          break;
        } else {
          parent = parent.clone().borrow_mut().right.as_mut().unwrap().clone();
        }
      }
    }

    new_node.clone().borrow_mut().parent = Some(parent.clone());
    while !matches!(new_node.borrow().parent, None) 
        && new_node.borrow().priority < new_node.borrow().parent.as_ref().unwrap().borrow().priority {

      println!("new_node = {:?}", new_node);
      
      let mut rotate_right = false;
      
      // massive brain move
      {
        let nnb = new_node.borrow();
        let parent_left_o = &nnb.parent.as_ref().unwrap().borrow().left;
        if !matches!(parent_left_o, None) {
          rotate_right = Rc::ptr_eq(&new_node, parent_left_o.as_ref().unwrap())
        }
      }

      if rotate_right {
        self.rotate_right(&mut new_node.clone());
      } else {
        self.rotate_left(&mut new_node.clone());
      }
      
      // println!("parent is none {:?}", matches!(new_node.borrow().parent, None) );
      // println!("new_node_priority = {:?} parent_priority = {:?}", 
      //   new_node.borrow().priority,
      //   new_node.borrow().parent.as_ref()
      // );
      // println!("here: {:?}", self);
    }

    if matches!(new_node.borrow().parent, None) {
      self.root = Some(new_node);
    }
  }

  fn rotate_left(&mut self, x: &mut Rc<RefCell<Node<T, U>>>) {
    if matches!(x.get_parent(), None) {
      panic!("cannot rorate root of tree");
    }

    let y = x.get_parent().unwrap();

    if y.is_left_child(Some(x.clone())) {
      panic!("cannot rotate_left on a left child");
    }

    if matches!(y.get_parent(), None) {
      x.set_parent(None);
      self.root = Some(x.clone());
    } else {
      let p = y.get_parent().unwrap();
      if p.is_left_child(Some(y.clone())) {
        p.set_left(Some(x.clone()));
      } else {
        p.set_right(Some(x.clone()));
      }
      x.set_parent(Some(p.clone()));
    }

    y.set_right(x.get_left());
    if !matches!(x.get_left(), None) {
      let x_left = x.get_left().unwrap();
      x_left.set_parent(Some(y.clone()));
    }

    x.set_left(Some(y.clone()));
    y.set_parent(Some(x.clone()));
  }

  fn rotate_right(&mut self, x: &mut Rc<RefCell<Node<T, U>>>) {
    if matches!(x.get_parent(), None) {
      panic!("cannot rotate the root of the tree");
    }

    let y = x.get_parent().unwrap();

    if y.is_right_child(Some(x.clone())) {
      panic!("cannot rotate_right on a right child");
    }

    if matches!(y.get_parent(), None) {
      x.set_parent(None);
      self.root = Some(x.clone());
    } else {
      let p = y.get_parent().unwrap();
      if p.is_left_child(Some(y.clone())) {
        p.set_left(Some(x.clone()));
      } else {
        p.set_right(Some(x.clone()));
      }
      x.set_parent(Some(p.clone()));
    }

    y.set_left(x.get_right());
    if !matches!(x.get_right(), None) {
      let x_right = x.get_right().unwrap();
      x_right.set_parent(Some(y.clone()));
    }

    x.set_right(Some(y.clone()));
    y.set_parent(Some(x.clone()));
  }

}

#[cfg(test)]
mod rotate_left_tests {
  use super::*;

  #[test]
  fn test_rotate_left_full_rotation() {
    let mut m = Memtable3::<i32,i32>::new();
    let p = Rc::new(RefCell::new(Node::<i32, i32> {
      key: 0,
      priority: 0,
      left: None,
      right: None,
      parent: None
    }));
    let y = Rc::new(RefCell::new(Node::<i32, i32> {
      key: 1,
      priority: 0,
      left: None,
      right: None,
      parent: None
    }));
    let x = Rc::new(RefCell::new(Node::<i32, i32> {
      key: 2,
      priority: 0,
      left: None,
      right: None,
      parent: None,
    }));
    let x_left = Rc::new(RefCell::new(Node::<i32, i32> {
      key: 3,
      priority: 0,
      left: None,
      right: None,
      parent: None,
    }));

    m.root = Some(p.clone());
    p.set_right(Some(y.clone()));
    y.set_parent(Some(p.clone()));

    y.set_right(Some(x.clone()));
    x.set_parent(Some(y.clone()));

    x.set_left(Some(x_left.clone()));
    x_left.set_parent(Some(y.clone()));

    m.rotate_left(&mut x.clone());

    // check x has now replaced y as the right child of P
    assert_eq!(false, matches!(p.get_right(), None));
    assert_eq!(true, p.is_right_child(Some(x.clone())));
    assert_eq!(false, matches!(x.get_parent(), None));
    assert_eq!(true, x.is_parent(Some(p.clone())));

    // check Y is now the left child of X
    assert_eq!(false, matches!(x.get_left(), None));
    assert_eq!(true, x.is_left_child(Some(y.clone())));
    assert_eq!(false, matches!(y.get_parent(), None));
    assert_eq!(true, y.is_parent(Some(x)));

    // check X's left is now the right child of Y
    assert_eq!(false, matches!(y.get_right(), None));
    assert_eq!(true, y.is_right_child(Some(x_left.clone())));
    assert_eq!(false, matches!(x_left.get_parent(), None));
    assert_eq!(true, x_left.is_parent(Some(y)));
  }

  #[test]
  fn test_rotate_left_full_y_is_left_child_of_p() {
    let mut m = Memtable3::<i32,i32>::new();
    let p = Rc::new(RefCell::new(Node::<i32, i32> {
      key: 0,
      priority: 0,
      left: None,
      right: None,
      parent: None
    }));
    let y = Rc::new(RefCell::new(Node::<i32, i32> {
      key: 1,
      priority: 0,
      left: None,
      right: None,
      parent: None
    }));
    let x = Rc::new(RefCell::new(Node::<i32, i32> {
      key: 2,
      priority: 0,
      left: None,
      right: None,
      parent: None,
    }));
    let x_left = Rc::new(RefCell::new(Node::<i32, i32> {
      key: 3,
      priority: 0,
      left: None,
      right: None,
      parent: None,
    }));

    m.root = Some(p.clone());
    p.set_left(Some(y.clone()));
    y.set_parent(Some(p.clone()));

    y.set_right(Some(x.clone()));
    x.set_parent(Some(y.clone()));

    x.set_left(Some(x_left.clone()));
    x_left.set_parent(Some(y.clone()));

    m.rotate_left(&mut x.clone());

    // check x has now replaced y as the right child of P
    assert_eq!(false, matches!(p.get_left(), None));
    assert_eq!(true, p.is_left_child(Some(x.clone())));
    assert_eq!(false, matches!(x.get_parent(), None));
    assert_eq!(true, x.is_parent(Some(p.clone())));

    // check Y is now the left child of X
    assert_eq!(false, matches!(x.get_left(), None));
    assert_eq!(true, x.is_left_child(Some(y.clone())));
    assert_eq!(false, matches!(y.get_parent(), None));
    assert_eq!(true, y.is_parent(Some(x)));

    // check X's left is now the right child of Y
    assert_eq!(false, matches!(y.get_right(), None));
    assert_eq!(true, y.is_right_child(Some(x_left.clone())));
    assert_eq!(false, matches!(x_left.get_parent(), None));
    assert_eq!(true, x_left.is_parent(Some(y)));
  }

  #[test]
  fn test_rotate_left_parent_is_root() {
    let mut m = Memtable3::<i32,i32>::new();
    let p = Rc::new(RefCell::new(Node::<i32, i32> {
      key: 0,
      priority: 0,
      left: None,
      right: None,
      parent: None
    }));

    let x = Rc::new(RefCell::new(Node::<i32, i32> {
      key: 2,
      priority: 0,
      left: None,
      right: None,
      parent: None,
    }));

    m.root = Some(p.clone());
    p.set_right(Some(x.clone()));
    x.set_parent(Some(p.clone()));

    m.rotate_left(&mut x.clone());

    assert_eq!(true, Rc::ptr_eq(&x, m.root.as_ref().unwrap()));
    assert_eq!(true, matches!(x.get_parent(), None));

    assert_eq!(true, x.is_left_child(Some(p.clone())));
    assert_eq!(true, p.is_parent(Some(x.clone())));

    assert_eq!(true, matches!(p.get_right(), None));
  }

  #[test]
  #[should_panic]
  fn test_rotate_left_panics_if_x_is_root() {
    let mut m = Memtable3::<i32,i32>::new();
    let x = Rc::new(RefCell::new(Node::<i32, i32> {
      key: 0,
      priority: 0,
      left: None,
      right: None,
      parent: None
    }));
    m.root = Some(x.clone());
    m.rotate_left(&mut x.clone());
  }

  #[test]
  #[should_panic]
  fn test_rotate_left_panics_if_x_is_left_child() {
    let mut m = Memtable3::<i32,i32>::new();
    let p = Rc::new(RefCell::new(Node::<i32, i32> {
      key: 0,
      priority: 0,
      left: None,
      right: None,
      parent: None
    }));

    let x = Rc::new(RefCell::new(Node::<i32, i32> {
      key: 2,
      priority: 0,
      left: None,
      right: None,
      parent: None,
    }));

    m.root = Some(p.clone());
    p.set_left(Some(x.clone()));
    x.set_parent(Some(p.clone()));
    m.rotate_left(&mut x.clone());
  }
}

#[cfg(test)]
mod rotate_right_tests {
  use super::*;
  
  #[test]
  #[should_panic]
  fn panics_if_x_is_root() {
    let mut m = Memtable3::<i32,i32>::new();
    let x = Rc::new(RefCell::new(Node::<i32, i32> {
      key: 0,
      priority: 0,
      left: None,
      right: None,
      parent: None
    }));
    m.root = Some(x.clone());
    m.rotate_right(&mut x.clone());
  }

  #[test]
  #[should_panic]
  fn panics_if_x_is_right_child() {
    let mut m = Memtable3::<i32, i32>::new();
    let p = Rc::new(RefCell::new(Node::<i32, i32> {
      key: 0,
      priority:0,
      left: None,
      right: None,
      parent: None,
    }));
    let x = Rc::new(RefCell::new(Node::<i32, i32> {
      key: 1,
      priority:2,
      left: None,
      right: None,
      parent: None,
    }));

    m.root = Some(p.clone());
    p.set_right(Some(x.clone()));
    x.set_parent(Some(p.clone()));

    m.rotate_right(&mut x.clone());
  }

  #[test]
  fn handles_case_where_x_parent_is_root() {
    let mut m = Memtable3::<i32, i32>::new();
    let p = Rc::new(RefCell::new(Node::<i32, i32> {
      key: 0,
      priority:0,
      left: None,
      right: None,
      parent: None,
    }));
    let x = Rc::new(RefCell::new(Node::<i32, i32> {
      key: 1,
      priority:2,
      left: None,
      right: None,
      parent: None,
    }));

    m.root = Some(p.clone());
    p.set_left(Some(x.clone()));
    x.set_parent(Some(p.clone()));

    m.rotate_right(&mut x.clone());

    assert_eq!(true, Rc::ptr_eq(&x, m.root.as_ref().unwrap()));
    assert_eq!(true, matches!(x.get_parent(), None));

    assert_eq!(true, x.is_right_child(Some(p.clone())));
    assert_eq!(true, p.is_parent(Some(x.clone())));

    assert_eq!(true, matches!(p.get_left(), None));
  }

  #[test]
  fn full_rotate_y_is_p_right_child() {
    let mut m = Memtable3::<i32, i32>::new();
    let p = Rc::new(RefCell::new(Node::<i32, i32> {
      key: 0,
      priority:0,
      left: None,
      right: None,
      parent: None,
    }));
    let y = Rc::new(RefCell::new(Node::<i32, i32> {
      key: 1,
      priority:1,
      left: None,
      right: None,
      parent: None,
    }));
    let x = Rc::new(RefCell::new(Node::<i32, i32> {
      key: 2,
      priority:2,
      left: None,
      right: None,
      parent: None,
    }));
    let x_right = Rc::new(RefCell::new(Node::<i32, i32> {
      key: 3,
      priority:3,
      left: None,
      right: None,
      parent: None,
    }));

    m.root = Some(p.clone());

    p.set_right(Some(y.clone()));
    y.set_parent(Some(p.clone()));

    y.set_left(Some(x.clone()));
    x.set_parent(Some(y.clone()));

    x.set_right(Some(x_right.clone()));
    x_right.set_parent(Some(x.clone()));

    m.rotate_right(&mut x.clone());

    assert_eq!(true, p.is_right_child(Some(x.clone())));
    assert_eq!(true, x.is_parent(Some(p.clone())));

    assert_eq!(true, x.is_right_child(Some(y.clone())));
    assert_eq!(true, y.is_parent(Some(x.clone())));

    assert_eq!(true, y.is_left_child(Some(x_right.clone())));
    assert_eq!(true, x_right.is_parent(Some(y.clone())));
  }

  #[test]
  fn full_rotate_y_is_p_left_child() {
    let mut m = Memtable3::<i32, i32>::new();
    let p = Rc::new(RefCell::new(Node::<i32, i32> {
      key: 0,
      priority:0,
      left: None,
      right: None,
      parent: None,
    }));
    let y = Rc::new(RefCell::new(Node::<i32, i32> {
      key: 1,
      priority:1,
      left: None,
      right: None,
      parent: None,
    }));
    let x = Rc::new(RefCell::new(Node::<i32, i32> {
      key: 2,
      priority:2,
      left: None,
      right: None,
      parent: None,
    }));
    let x_right = Rc::new(RefCell::new(Node::<i32, i32> {
      key: 3,
      priority:3,
      left: None,
      right: None,
      parent: None,
    }));

    m.root = Some(p.clone());

    p.set_left(Some(y.clone()));
    y.set_parent(Some(p.clone()));

    y.set_left(Some(x.clone()));
    x.set_parent(Some(y.clone()));

    x.set_right(Some(x_right.clone()));
    x_right.set_parent(Some(x.clone()));

    m.rotate_right(&mut x.clone());

    assert_eq!(true, p.is_left_child(Some(x.clone())));
    assert_eq!(true, x.is_parent(Some(p.clone())));

    assert_eq!(true, x.is_right_child(Some(y.clone())));
    assert_eq!(true, y.is_parent(Some(x.clone())));

    assert_eq!(true, y.is_left_child(Some(x_right.clone())));
    assert_eq!(true, x_right.is_parent(Some(y.clone())));
  }
}