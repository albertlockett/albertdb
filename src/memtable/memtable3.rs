use std::rc::Rc;
use std::cell::RefCell;
use std::cell::Ref;
use std::cell::RefMut;
use std::ops::Deref;
use std::ops::DerefMut;

struct Node<T: PartialOrd, U: PartialOrd> {
  key: T,
  priority: U,
  left: Link<T, U>,
  right: Link<T, U>,
  parent: ParentLink<T, U>,
}

type Link<T, U> = Option<Box<Node<T, U>>>;

type ParentLink<T, U> = Option<Rc<RefCell<Node<T, U>>>>;

struct Memtable3<T: PartialOrd, U: PartialOrd> {
  root: Link<T, U>
}

struct RotateConfig {
  root: bool,
  set_p_left: bool,
  set_p_right: bool,
}

impl<T, U> Memtable3<T, U> where T: PartialOrd, U: PartialOrd {
  pub fn insert(&mut self, key: T, priority: U) {
    let new_node = Box::new(Node {
      key,
      priority,
      left: None,
      right: None,
      parent: None
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

    Self::update_parent_refs(parent);
  }

  fn update_parent_refs(node: &mut Node<T, U>) {
    if !matches!(node.left, None) {
      let left = node.left.as_mut().unwrap();
      if !matches!(left.parent, None) && !matches!(node.right, None) {
        let right = node.right.as_mut().unwrap();
        if matches!(right.parent, None) {
          let parent = left.parent.as_ref().unwrap();
          right.parent = Some(parent.clone());
        }
      }
    }

    if !matches!(node.right, None) {
      let right = node.right.as_mut().unwrap();
      if !matches!(right.parent, None) && !matches!(node.left, None) {
        let left = node.left.as_mut().unwrap();
        if matches!(left.parent, None) {
          let parent = left.parent.as_ref().unwrap();
          left.parent = Some(parent.clone());
        }
      }
    }
  }


  fn rotate_right(&mut self, mut x: Node<T, U>) {
    // TODO check if is root and panic

    let y_pl: &mut ParentLink<T, U> = &mut x.parent;
    let y_rr: &mut Rc<RefCell<Node<T, U>>> = y_pl.as_mut().unwrap();
    let mut y_rm: RefMut<Node<T, U>> = y_rr.borrow_mut();
    
    let p_pl: &mut ParentLink<T, U> = &mut y_rm.parent;

    // if matches!(p_pl, None) {
    //   self.root = Some(Box::new(x));
    //   return
    // } else {
    //   let p_rr: &mut Rc<RefCell<Node<T, U>>> = p_pl.as_mut().unwrap();
    //   let mut p_rm: RefMut<Node<T, U>> = p_rr.borrow_mut();
    // }
  }

  fn rotate_right2(&mut self, mut x: Node<T, U>) {
    // let xp: &Rc<RefCell<Node<T, U>>> = &x.parent.unwrap();
    // let p2 = xp.borrow();
    // let p3 = &p2.parent;

    if matches!(&x.parent.as_ref().unwrap().borrow().parent, None) {
      self.root = Some(Box::new(x));
      return;
    }

    let y_pl: &mut ParentLink<T, U> = &mut x.parent;
    let y_rr: &mut Rc<RefCell<Node<T, U>>> = y_pl.as_mut().unwrap();
    let mut y_rm: RefMut<Node<T, U>> = y_rr.borrow_mut();
    let y: &mut Node<T, U> = y_rm.deref_mut();
    
    let p_pl: &mut ParentLink<T, U> = &mut y.parent;
    let p_rr: &mut Rc<RefCell<Node<T, U>>> = p_pl.as_mut().unwrap();
    let mut p_rm: RefMut<Node<T, U>> = p_rr.borrow_mut();
    let p: &mut Node<T, U> = p_rm.deref_mut();

    let p_left = p.left.as_deref_mut().unwrap();
  }

  fn rotate_right3(&mut self, mut x: Node<T, U>) {
    if matches!(&x.parent.as_ref().unwrap().borrow().parent, None) {
      self.root = Some(Box::new(x));
      return;
    }

    let y1: &Rc<RefCell<Node<T, U>>> = &x.parent.as_ref().unwrap();
    let y2: Ref<Node<T, U>> = (*y1).borrow();
    let y3: &Node<T, U> = y2.deref();

    if matches!(&y3.parent, None) {
      // self.root = Some(Box::new(x));
      // return;
    }

    let p1: &Rc<RefCell<Node<T, U>>> = &y3.parent.as_ref().unwrap();
    let p2: Ref<Node<T, U>> = (*p1).borrow();
    let p3: &Node<T, U> = p2.deref();



    let pl1o = p3.left.as_deref();
    let y_is_left = !matches!(pl1o, None) && std::ptr::eq(pl1o.unwrap(), y3);
    
    if y_is_left {
      let mut p3m = (*p1).borrow_mut();
      // p3m.right = Some(Box::new(x));
    }
  }


  fn rotate_right4(&mut self, rconf: &RotateConfig, mut x: Node<T, U>) {
    if rconf.root {
      self.root = Some(Box::new(x));
      return;
    }

    if rconf.set_p_left {
      let mut y_pl: ParentLink<T, U> = std::mem::replace(&mut x.parent, None);
      let mut x_r: Link<T, U> = std::mem::replace(&mut x.right, None);

      

    }
  }
}