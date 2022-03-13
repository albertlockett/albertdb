// Memtable is implemented as a binary treap using random priority to ensure balance

use rand::prelude::*;
use std::fmt::Debug;
use std::sync::Arc;
use std::sync::RwLock;

mod node;
use crate::memtable::node::{Link, Node, NodeMethods};

#[derive(Debug)]
pub struct Memtable {
    root: Link,
    size: u32,
    pub id: String,
}

impl Memtable {
    pub fn new() -> Self {
        // TODO needs a better implementation of random ID (collsions would be a disaster)
        let mut rng = rand::thread_rng();
        let id: u32 = rng.gen();
        Memtable {
            id: format!("{:?}", id),
            root: None,
            size: 0,
        }
    }

    pub fn size(&self) -> u32 {
        return self.size;
    }

    pub fn search(&self, key: &[u8]) -> (Option<Vec<u8>>, bool) {
        if matches!(self.root, None) {
            return (None, false);
        }

        let node = self.root.as_ref().unwrap();
        let (value, found) = node.search(&key.to_vec());
        if value.is_some() {
            return (Some(value.unwrap()), true);
        }
        return (None, found);
    }

    pub fn insert(&mut self, key: Vec<u8>, value: Option<Vec<u8>>) {
        let mut rng = rand::thread_rng();
        let priority: f64 = rng.gen();
        self.insert_with_priority(priority, key, value);
    }

    pub fn insert_with_priority(
        &mut self,
        priority: f64,
        key: Vec<u8>,
        mut value: Option<Vec<u8>>,
    ) {
        // oops the tree is empty - new node is the root
        if matches!(self.root, None) {
            let new_node = Arc::new(RwLock::new(Node {
                key,
                value,
                priority,
                left: None,
                right: None,
                parent: None,
            }));
            self.size += 1;
            self.root = Some(new_node);
            return;
        }

        // find the parent of the node we're going to insert
        let mut node_link: Link = Some(self.root.as_ref().unwrap().clone());
        let mut parent_link: Link = None;
        let mut replace = false;
        while !matches!(node_link, None) {
            let node = node_link.as_ref().unwrap().clone();
            parent_link = Some(node.clone());

            if key == node.read().unwrap().key {
                replace = true;
                break;
            } else if key > node.read().unwrap().key {
                node_link = node.get_right();
            } else {
                node_link = node.get_left();
            }
        }

        if replace {
            std::mem::swap(&mut value, &mut parent_link.unwrap().write().unwrap().value);
            return;
        }

        let new_node = Arc::new(RwLock::new(Node {
            key,
            value,
            priority,
            left: None,
            right: None,
            parent: None,
        }));

        self.size += 1;
        let parent = parent_link.as_ref().unwrap().clone();
        if parent.read().unwrap().key <= new_node.read().unwrap().key {
            parent.set_right(Some(new_node.clone()))
        } else {
            parent.set_left(Some(new_node.clone()));
        }
        new_node.set_parent(Some(parent.clone()));

        while new_node.is_heap_invariant() {
            let parent = new_node.get_parent().unwrap();
            if parent.is_left_child(Some(new_node.clone())) {
                self.rotate_right(&mut new_node.clone());
            } else {
                self.rotate_left(&mut new_node.clone());
            }
        }
    }

    fn rotate_left(&mut self, x: &mut Arc<RwLock<Node>>) {
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

    fn rotate_right(&mut self, x: &mut Arc<RwLock<Node>>) {
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
mod memtable_tests {
    use super::*;

    #[test]
    fn it_returns_false_if_empty() {
        let memtable = Memtable::new();
        let (val, found) = memtable.search(&"albert".as_bytes());
        assert_eq!(0, memtable.size());
        assert_eq!(val, None);
        assert_eq!(found, false);
    }

    #[test]
    fn it_can_find_and_delete_the_root_value() {
        let mut memtable = Memtable::new();
        assert_eq!(0, memtable.size());
        memtable.insert(
            String::from("guy").into_bytes(),
            Some(String::from("tim").into_bytes()),
        );
        assert_eq!(1, memtable.size());
        let (val_o, found) = memtable.search(&"guy".as_bytes());
        assert_eq!(val_o.is_some(), true);
        let val = val_o.unwrap();
        assert_eq!(val, String::from("tim").into_bytes());
        assert_eq!(found, true);

        memtable.insert(String::from("guy").into_bytes(), None);
        assert_eq!(1, memtable.size());
        let (val_o, found) = memtable.search(&"guy".as_bytes());
        assert_eq!(val_o, None);
        assert_eq!(found, true);
    }

    #[test]
    fn it_can_put_many_children_in_itself() {
        let mut memtable = Memtable::new();
        assert_eq!(0, memtable.size());
        memtable.insert(
            String::from("a").into_bytes(),
            Some(String::from("1").into_bytes()),
        );
        assert_eq!(1, memtable.size());

        memtable.insert(
            String::from("b").into_bytes(),
            Some(String::from("2").into_bytes()),
        );
        assert_eq!(2, memtable.size());

        memtable.insert(
            String::from("c").into_bytes(),
            Some(String::from("3").into_bytes()),
        );
        assert_eq!(3, memtable.size());

        memtable.insert(
            String::from("d").into_bytes(),
            Some(String::from("4").into_bytes()),
        );
        assert_eq!(4, memtable.size());

        let (val_o, found) = memtable.search(&"a".as_bytes());
        assert_eq!(val_o.is_some(), true);
        assert_eq!(val_o.unwrap(), String::from("1").as_bytes());
        assert_eq!(found, true);

        let (val_o, found) = memtable.search(&"b".as_bytes());
        assert_eq!(val_o.is_some(), true);
        assert_eq!(val_o.unwrap(), String::from("2").as_bytes());
        assert_eq!(found, true);

        let (val_o, found) = memtable.search(&"c".as_bytes());
        assert_eq!(val_o.is_some(), true);
        assert_eq!(val_o.unwrap(), String::from("3").as_bytes());
        assert_eq!(found, true);

        let (val_o, found) = memtable.search(&"d".as_bytes());
        assert_eq!(val_o.is_some(), true);
        assert_eq!(val_o.unwrap(), String::from("4").as_bytes());
        assert_eq!(found, true);

        // ensure we can update all the values
        memtable.insert(
            String::from("a").into_bytes(),
            Some(String::from("5").into_bytes()),
        );
        assert_eq!(4, memtable.size());

        memtable.insert(
            String::from("b").into_bytes(),
            Some(String::from("6").into_bytes()),
        );
        assert_eq!(4, memtable.size());

        memtable.insert(
            String::from("c").into_bytes(),
            Some(String::from("7").into_bytes()),
        );
        assert_eq!(4, memtable.size());

        memtable.insert(
            String::from("d").into_bytes(),
            Some(String::from("8").into_bytes()),
        );
        assert_eq!(4, memtable.size());

        let (val_o, found) = memtable.search(&"a".as_bytes());
        assert_eq!(val_o.is_some(), true);
        assert_eq!(val_o.unwrap(), String::from("5").as_bytes());
        assert_eq!(found, true);

        let (val_o, found) = memtable.search(&"b".as_bytes());
        assert_eq!(val_o.is_some(), true);
        assert_eq!(val_o.unwrap(), String::from("6").as_bytes());
        assert_eq!(found, true);

        let (val_o, found) = memtable.search(&"c".as_bytes());
        assert_eq!(val_o.is_some(), true);
        assert_eq!(val_o.unwrap(), String::from("7").as_bytes());
        assert_eq!(found, true);

        let (val_o, found) = memtable.search(&"d".as_bytes());
        assert_eq!(val_o.is_some(), true);
        assert_eq!(val_o.unwrap(), String::from("8").as_bytes());
        assert_eq!(found, true);

        // ensure can delete all the values
        memtable.insert(String::from("a").into_bytes(), None);
        assert_eq!(4, memtable.size());
        let (val_o, found) = memtable.search(&"a".as_bytes());
        assert_eq!(val_o.is_none(), true);
        assert_eq!(found, true);

        memtable.insert(String::from("b").into_bytes(), None);
        assert_eq!(4, memtable.size());
        assert_eq!(4, memtable.size());
        let (val_o, found) = memtable.search(&"a".as_bytes());
        assert_eq!(val_o.is_none(), true);
        assert_eq!(found, true);

        memtable.insert(String::from("c").into_bytes(), None);
        assert_eq!(4, memtable.size());
        assert_eq!(4, memtable.size());
        let (val_o, found) = memtable.search(&"a".as_bytes());
        assert_eq!(val_o.is_none(), true);
        assert_eq!(found, true);

        memtable.insert(String::from("d").into_bytes(), None);
        assert_eq!(4, memtable.size());
        assert_eq!(4, memtable.size());
        let (val_o, found) = memtable.search(&"a".as_bytes());
        assert_eq!(val_o.is_none(), true);
        assert_eq!(found, true);
    }
}

#[cfg(test)]
mod insert_tests {
    use super::*;

    #[test]
    fn test_insert_some_rotations() {
        let mut m = Memtable::new();
        let p = Arc::new(RwLock::new(Node {
            key: String::from("50").into_bytes(),
            value: Some(String::from("50").into_bytes()),
            priority: 50f64,
            left: None,
            right: None,
            parent: None,
        }));
        let y = Arc::new(RwLock::new(Node {
            key: String::from("40").into_bytes(),
            value: Some(String::from("40").into_bytes()),
            priority: 40f64,
            left: None,
            right: None,
            parent: None,
        }));
        let x = Arc::new(RwLock::new(Node {
            key: String::from("30").into_bytes(),
            value: Some(String::from("30").into_bytes()),
            priority: 30f64,
            left: None,
            right: None,
            parent: None,
        }));

        m.root = Some(p.clone());

        p.set_left(Some(y.clone()));
        y.set_parent(Some(p.clone()));

        y.set_left(Some(x.clone()));
        x.set_parent(Some(y.clone()));

        m.insert_with_priority(
            45f64,
            String::from("35").into_bytes(),
            Some(String::from("45").into_bytes()),
        );

        assert_eq!(true, Arc::ptr_eq(&p, m.root.as_ref().unwrap()));

        let new_node = p.get_left().unwrap().clone();
        assert_eq!(
            String::from("35").into_bytes(),
            new_node.read().unwrap().key
        );

        assert_eq!(true, new_node.is_left_child(Some(x.clone())));
        assert_eq!(true, x.is_parent(Some(new_node.clone())));

        assert_eq!(true, new_node.is_right_child(Some(y.clone())));
        assert_eq!(true, y.is_parent(Some(new_node.clone())));
    }
}

#[cfg(test)]
mod rotate_left_tests {
    use super::*;

    #[test]
    fn test_rotate_left_full_rotation() {
        let mut m = Memtable::new();
        let p = Arc::new(RwLock::new(Node {
            key: String::from("0").into_bytes(),
            value: Some(String::from("0").into_bytes()),
            priority: 0f64,
            left: None,
            right: None,
            parent: None,
        }));
        let y = Arc::new(RwLock::new(Node {
            key: String::from("1").into_bytes(),
            value: Some(String::from("1").into_bytes()),
            priority: 0f64,
            left: None,
            right: None,
            parent: None,
        }));
        let x = Arc::new(RwLock::new(Node {
            key: String::from("2").into_bytes(),
            value: Some(String::from("2").into_bytes()),
            priority: 0f64,
            left: None,
            right: None,
            parent: None,
        }));
        let x_left = Arc::new(RwLock::new(Node {
            key: String::from("3").into_bytes(),
            value: Some(String::from("3").into_bytes()),
            priority: 0f64,
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
        let mut m = Memtable::new();
        let p = Arc::new(RwLock::new(Node {
            key: String::from("0").into_bytes(),
            value: Some(String::from("0").into_bytes()),
            priority: 0f64,
            left: None,
            right: None,
            parent: None,
        }));
        let y = Arc::new(RwLock::new(Node {
            key: String::from("1").into_bytes(),
            value: Some(String::from("1").into_bytes()),
            priority: 0f64,
            left: None,
            right: None,
            parent: None,
        }));
        let x = Arc::new(RwLock::new(Node {
            key: String::from("2").into_bytes(),
            value: Some(String::from("2").into_bytes()),
            priority: 0f64,
            left: None,
            right: None,
            parent: None,
        }));
        let x_left = Arc::new(RwLock::new(Node {
            key: String::from("3").into_bytes(),
            value: Some(String::from("3").into_bytes()),
            priority: 0f64,
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
        let mut m = Memtable::new();
        let p = Arc::new(RwLock::new(Node {
            key: String::from("0").into_bytes(),
            value: Some(String::from("0").into_bytes()),
            priority: 0f64,
            left: None,
            right: None,
            parent: None,
        }));
        let x = Arc::new(RwLock::new(Node {
            key: String::from("2").into_bytes(),
            value: Some(String::from("2").into_bytes()),
            priority: 0f64,
            left: None,
            right: None,
            parent: None,
        }));

        m.root = Some(p.clone());
        p.set_right(Some(x.clone()));
        x.set_parent(Some(p.clone()));

        m.rotate_left(&mut x.clone());

        assert_eq!(true, Arc::ptr_eq(&x, m.root.as_ref().unwrap()));
        assert_eq!(true, matches!(x.get_parent(), None));

        assert_eq!(true, x.is_left_child(Some(p.clone())));
        assert_eq!(true, p.is_parent(Some(x.clone())));

        assert_eq!(true, matches!(p.get_right(), None));
    }

    #[test]
    #[should_panic]
    fn test_rotate_left_panics_if_x_is_root() {
        let mut m = Memtable::new();
        let x = Arc::new(RwLock::new(Node {
            key: String::from("0").into_bytes(),
            value: Some(String::from("0").into_bytes()),
            priority: 0f64,
            left: None,
            right: None,
            parent: None,
        }));
        m.root = Some(x.clone());
        m.rotate_left(&mut x.clone());
    }

    #[test]
    #[should_panic]
    fn test_rotate_left_panics_if_x_is_left_child() {
        let mut m = Memtable::new();
        let p = Arc::new(RwLock::new(Node {
            key: String::from("0").into_bytes(),
            value: Some(String::from("0").into_bytes()),
            priority: 0f64,
            left: None,
            right: None,
            parent: None,
        }));

        let x = Arc::new(RwLock::new(Node {
            key: String::from("2").into_bytes(),
            value: Some(String::from("2").into_bytes()),
            priority: 0f64,
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
        let mut m = Memtable::new();
        let x = Arc::new(RwLock::new(Node {
            key: String::from("2").into_bytes(),
            value: Some(String::from("2").into_bytes()),
            priority: 0f64,
            left: None,
            right: None,
            parent: None,
        }));
        m.root = Some(x.clone());
        m.rotate_right(&mut x.clone());
    }

    #[test]
    #[should_panic]
    fn panics_if_x_is_right_child() {
        let mut m = Memtable::new();
        let p = Arc::new(RwLock::new(Node {
            key: String::from("0").into_bytes(),
            value: Some(String::from("0").into_bytes()),
            priority: 0f64,
            left: None,
            right: None,
            parent: None,
        }));

        let x = Arc::new(RwLock::new(Node {
            key: String::from("2").into_bytes(),
            value: Some(String::from("2").into_bytes()),
            priority: 0f64,
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
        let mut m = Memtable::new();
        let p = Arc::new(RwLock::new(Node {
            key: String::from("0").into_bytes(),
            value: Some(String::from("0").into_bytes()),
            priority: 0f64,
            left: None,
            right: None,
            parent: None,
        }));

        let x = Arc::new(RwLock::new(Node {
            key: String::from("2").into_bytes(),
            value: Some(String::from("2").into_bytes()),
            priority: 0f64,
            left: None,
            right: None,
            parent: None,
        }));

        m.root = Some(p.clone());
        p.set_left(Some(x.clone()));
        x.set_parent(Some(p.clone()));

        m.rotate_right(&mut x.clone());

        assert_eq!(true, Arc::ptr_eq(&x, m.root.as_ref().unwrap()));
        assert_eq!(true, matches!(x.get_parent(), None));

        assert_eq!(true, x.is_right_child(Some(p.clone())));
        assert_eq!(true, p.is_parent(Some(x.clone())));

        assert_eq!(true, matches!(p.get_left(), None));
    }

    #[test]
    fn full_rotate_y_is_p_right_child() {
        let mut m = Memtable::new();
        let p = Arc::new(RwLock::new(Node {
            key: String::from("0").into_bytes(),
            value: Some(String::from("0").into_bytes()),
            priority: 0f64,
            left: None,
            right: None,
            parent: None,
        }));
        let y = Arc::new(RwLock::new(Node {
            key: String::from("1").into_bytes(),
            value: Some(String::from("1").into_bytes()),
            priority: 1f64,
            left: None,
            right: None,
            parent: None,
        }));
        let x = Arc::new(RwLock::new(Node {
            key: String::from("2").into_bytes(),
            value: Some(String::from("2").into_bytes()),
            priority: 2f64,
            left: None,
            right: None,
            parent: None,
        }));
        let x_right = Arc::new(RwLock::new(Node {
            key: String::from("3").into_bytes(),
            value: Some(String::from("3").into_bytes()),
            priority: 3f64,
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
        let mut m = Memtable::new();
        let p = Arc::new(RwLock::new(Node {
            key: String::from("0").into_bytes(),
            value: Some(String::from("0").into_bytes()),
            priority: 0f64,
            left: None,
            right: None,
            parent: None,
        }));
        let y = Arc::new(RwLock::new(Node {
            key: String::from("1").into_bytes(),
            value: Some(String::from("1").into_bytes()),
            priority: 1f64,
            left: None,
            right: None,
            parent: None,
        }));
        let x = Arc::new(RwLock::new(Node {
            key: String::from("2").into_bytes(),
            value: Some(String::from("2").into_bytes()),
            priority: 2f64,
            left: None,
            right: None,
            parent: None,
        }));
        let x_right = Arc::new(RwLock::new(Node {
            key: String::from("3").into_bytes(),
            value: Some(String::from("3").into_bytes()),
            priority: 3f64,
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

#[derive(Debug)]
pub struct MemtableIterator {
    unvisited: Vec<Link>,
}

impl MemtableIterator {
    fn push_left_edge(&mut self, link: &Link) {
        if matches!(link, None) {
            return;
        }
        let mut link = Some(link.as_ref().unwrap().clone());
        while !matches!(link, None) {
            let node = link.as_ref().unwrap();
            self.unvisited.push(Some(node.clone()));
            link = node.get_left();
        }
    }
}

impl Iterator for MemtableIterator {
    type Item = (Vec<u8>, Option<Vec<u8>>);

    fn next(&mut self) -> Option<Self::Item> {
        let link = self.unvisited.pop()?;

        let node = link.as_ref().unwrap();
        self.push_left_edge(&node.get_right());
        return Some((
            node.read().unwrap().key.clone(),
            node.read().unwrap().value.clone(),
        ));
    }
}

impl IntoIterator for Memtable {
    type Item = (Vec<u8>, Option<Vec<u8>>);
    type IntoIter = MemtableIterator;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl Memtable {
    pub fn iter(&self) -> MemtableIterator {
        let mut iter = MemtableIterator {
            unvisited: Vec::new(),
        };
        iter.push_left_edge(&self.root);
        iter
    }
}
