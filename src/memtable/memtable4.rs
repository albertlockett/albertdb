use std::rc::Rc;
use std::cell::RefCell;
use std::cell::Ref;
use std::cell::RefMut;
use std::ops::Deref;
use std::ops::DerefMut;
use std::mem;

struct Node<T: PartialOrd, U: PartialOrd> {
  key: T,
  priority: U,
  left: Link<T, U>,
  right: Link<T, U>,
  parent: Link<T, U>,
}

impl <T, U> Node<T, U> where T: PartialOrd, U: PartialOrd {
  fn set_right(&mut self, link: Link<T, U>) {

  }

  fn set_left(&mut self, link: Link<T, U>) {
    let right = self.right.as_mut().unwrap().borrow();
    let right_parent = right.parent.as_ref().unwrap();
    let self_ref = right_parent.clone();
    
    self.left = link;
    let node = &mut self.left.as_mut().unwrap().borrow_mut();
    node.parent = Some(self_ref);
  }
}

type Link<T, U> = Option<Rc<RefCell<Node<T, U>>>>;

struct Memtable3<T: PartialOrd, U: PartialOrd> {
  root: Link<T, U>
}

struct RotateConfig {
  root: bool,
  set_p_left: bool,
  set_p_right: bool,
}

impl<T, U> Memtable3<T, U> where T: PartialOrd, U: PartialOrd {
  fn rotate_left(&mut self, mut x: Node<T, U>) {

  }

  fn rotate_right6(&mut self, x: &mut Rc<RefCell<Node<T, U>>>) {
    let y: Rc<RefCell<Node<T, U>>> = x.borrow_mut().parent.as_mut().unwrap().clone();
    let p: Rc<RefCell<Node<T, U>>> = y.borrow_mut().parent.as_mut().unwrap().clone();
    let xr: Rc<RefCell<Node<T, U>>> = x.borrow_mut().right.as_mut().unwrap().clone();
    xr.borrow_mut().parent = Some(y.clone());
    x.borrow_mut().right = Some(y.clone());
    x.borrow_mut().parent = Some(p.clone());
    y.borrow_mut().parent = Some(x.clone());
    y.borrow_mut().left = Some(xr.clone());
    p.borrow_mut().left = Some(x.clone());
  }

  fn rotate_right5(&mut self, mut x: Node<T, U>) {
    let mut y = mem::replace(&mut x.parent, None);
    let xr = mem::replace(&mut x.right, None);

    x.right = Some(y.as_ref().unwrap().clone());
    y.as_mut().unwrap().borrow_mut().deref_mut().left = xr;
    
    let yn = &mut y.as_mut().unwrap().borrow_mut();
    let p = &mut yn.parent.as_mut().unwrap().borrow_mut();
    p.left = Some(Rc::new(RefCell::new(x)));

    
  }

  fn rotate_right4(&mut self, rconf: &RotateConfig, mut x: Node<T, U>) {
    if rconf.root {
      self.root = Some(Rc::new(RefCell::new(x)));
      return;
    }

    // 1 y = x.parent
    // 2 p = y.parent
    // 3 p.left = x
    // 4 y.right = x.right
    // 5 x.right = y

    let mut y: Link<T, U> = std::mem::replace(&mut x.parent, None);

    let xr: Link<T, U> = std::mem::replace(&mut x.right, None);
    
    // 5
    let y1: &Rc<RefCell<Node<T, U>>> = &y.as_ref().unwrap();
    x.right = Some(y1.clone());

    // 4
    let y1: &mut Rc<RefCell<Node<T, U>>> = &mut y.as_mut().unwrap().clone();
    let mut y2 = y1.borrow_mut();
    let y3 = y2.deref_mut();
    y3.right = Some(xr.as_ref().unwrap().clone());

    // 3
    let mut p: Link<T, U> = std::mem::replace(&mut y3.parent, None);
    let p1: &mut Rc<RefCell<Node<T, U>>> = p.as_mut().unwrap();
    let mut p2 = (*p1).borrow_mut();
    let p3 = p2.deref_mut();
    p3.left = Some(Rc::new(RefCell::new(x)));
  
  }
}