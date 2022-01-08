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

impl<T, U> std::fmt::Debug for Node<T, U> where T: PartialOrd + Debug, U: PartialOrd + Debug {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
      f.debug_struct("Node")
       .field("key", &self.key)
       .field("priority", &self.priority)
       .field("left", &self.left)
       .field("right", &self.right)
       .field("parent", match &self.parent {
         Some(x) => {
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

#[derive(Debug)]
pub struct Memtable3<T: PartialOrd + Debug, U: PartialOrd + Debug> {
  root: Link<T, U>
}

struct RotateConfig {
  root: bool,
  set_p_left: bool,
  set_p_right: bool,
}

impl<T, U> Memtable3<T, U> where T: PartialOrd + Debug, U: PartialOrd + Debug{

  fn new() -> Self {
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
      // let rotate_right = false;
      // if 
      if Rc::ptr_eq(&new_node, new_node.borrow().parent.as_ref().unwrap().borrow().left.as_ref().unwrap()) {
        self.rotate_right(&mut new_node.clone());
      } else {
        // TODO rotate left 
        println!("gonna try rotate left");
        panic!("rotate_left")
      }
      
      println!("parent is none {:?}", matches!(new_node.borrow().parent, None) );
      println!("new_node_priority = {:?} parent_priority = {:?}", 
        new_node.borrow().priority,
        new_node.borrow().parent.as_ref()
      );
      println!("here: {:?}", self);
    }

    if matches!(new_node.borrow().parent, None) {
      self.root = Some(new_node);
    }
  }

  fn rotate_right(&mut self, x: &mut Rc<RefCell<Node<T, U>>>) {
    let y: Rc<RefCell<Node<T, U>>> = x.borrow_mut().parent.as_mut().unwrap().clone();

    println!("x = {:?}, y = {:?}", x, y);
    if matches!(y.borrow().parent, None) {
      x.clone().borrow_mut().parent = None;
      self.root = Some(x.clone());
    } else {
      println!("here 2");

      let p: Rc<RefCell<Node<T, U>>> = y.borrow_mut().parent.as_mut().unwrap().clone();
      let mut put_on_left = false;
      if !matches!(p.borrow().left, None) {
        let pl = p.borrow().left.as_ref().unwrap().clone();
        put_on_left = Rc::ptr_eq(&p, &pl);
      }
      
      if put_on_left {
        p.borrow_mut().left = Some(x.clone());
      } else {
        p.borrow_mut().right = Some(x.clone());
      }
      x.borrow_mut().parent = Some(p.clone());
    }

    
    if !matches!(x.borrow_mut().right, None) {
      let xr: Rc<RefCell<Node<T, U>>> = x.borrow_mut().right.as_mut().unwrap().clone();
      xr.borrow_mut().parent = Some(y.clone());
      y.borrow_mut().left = Some(xr.clone());
    } else {
      y.borrow_mut().left = None;
    }

    x.borrow_mut().right = Some(y.clone());
    y.borrow_mut().parent = Some(x.clone());
  }

}

#[test]
fn my_test() {
  let mut m = Memtable3::<i32,i32>::new();
  m.insert(4, 1);
  m.insert(2, 1);
  m.insert(5, 1);
  
  println!("\n\n result: {:?}", m);
}

/*
  { 
    root: Some(RefCell { 
      value: Node { 
        key: 6, 
        priority: 1, 
        left: None, 
        right: Some(RefCell { 
          value: Node {
            key: 5, 
            priority: 2, 
            left: None, 
            right: Some(RefCell { 
              value: Node { 
                key: 4,
                priority: 3, 
                left: None, 
                right: None, 
                parent: "some" 
              } 
            }), 
            parent: "some" 
          } 
        }), 
        parent: "none" 
      } 
    }) 
  }
*/