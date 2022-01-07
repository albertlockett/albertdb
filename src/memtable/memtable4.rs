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
  fn rotate_left(&mut self, mut x :Node<T, U>) {

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