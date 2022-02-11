use rand::prelude::*;
use std::cell::RefCell;
use std::fmt::Debug;
use std::sync::Arc;
use std::sync::RwLock;

pub mod memtable2;

pub struct Node {
    key: Vec<u8>,
    priority: f64,
    left: Link,
    right: Link,
    parent: Link,
}

pub type Link = Option<Arc<RwLock<Node>>>;

pub trait NodeStuff {
    fn get_parent(&self) -> Link;

    fn set_parent(&self, parent: Link);

    fn get_left(&self) -> Link;

    fn set_left(&self, new_left: Link);

    fn get_right(&self) -> Link;

    fn set_right(&self, new_right: Link);

    fn is_left_child(&self, child: Link) -> bool;

    fn is_right_child(&self, child: Link) -> bool;

    fn is_parent(&self, child: Link) -> bool;

    fn is_heap_invariant(&self) -> bool;

    fn search(&self, key: &Vec<u8>) -> bool;
}

impl std::fmt::Debug for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Node")
            .field("key", &self.key)
            .field("priority", &self.priority)
            .field("left", &self.left)
            .field("right", &self.right)
            .field(
                "parent",
                match &self.parent {
                    Some(_x) => &"some",
                    None => &"none",
                },
            )
            .finish()
    }
}

impl NodeStuff for Arc<RwLock<Node>> {
    fn get_parent(&self) -> Link {
        if matches!(self.read().unwrap().parent, None) {
            return None;
        }
        return Some(self.write().unwrap().parent.as_mut().unwrap().clone());
    }

    fn set_parent(&self, parent: Link) {
        let node = self.clone();
        node.write().unwrap().parent = parent;
    }

    fn get_left(&self) -> Link {
        if matches!(self.read().unwrap().left, None) {
            return None;
        }
        return Some(self.write().unwrap().left.as_mut().unwrap().clone());
    }

    fn set_left(&self, new_left: Link) {
        let node = self.clone();
        node.write().unwrap().left = new_left;
    }

    fn get_right(&self) -> Link {
        if matches!(self.read().unwrap().right, None) {
            return None;
        }
        return Some(self.write().unwrap().right.as_mut().unwrap().clone());
    }

    fn set_right(&self, new_right: Link) {
        let node = self.clone();
        node.write().unwrap().right = new_right;
    }

    fn is_left_child(&self, child: Link) -> bool {
        if matches!(self.read().unwrap().left, None) {
            return matches!(child, None);
        }

        if matches!(child, None) {
            return matches!(self.read().unwrap().left, None);
        }

        let my_child = self.read().unwrap().left.as_ref().unwrap().clone();
        return Arc::ptr_eq(&my_child, &child.unwrap());
    }

    fn is_right_child(&self, child: Link) -> bool {
        if matches!(self.read().unwrap().right, None) {
            return matches!(child, None);
        }

        if matches!(child, None) {
            return matches!(self.read().unwrap().right, None);
        }

        let my_child = self.read().unwrap().right.as_ref().unwrap().clone();
        return Arc::ptr_eq(&my_child, &child.unwrap());
    }

    fn is_parent(&self, parent: Link) -> bool {
        if matches!(self.read().unwrap().parent, None) {
            return matches!(parent, None);
        }
        if matches!(parent, None) {
            return matches!(self.read().unwrap().parent, None);
        }
        let my_parent = self.read().unwrap().parent.as_ref().unwrap().clone();
        return Arc::ptr_eq(&my_parent, &parent.unwrap());
    }

    fn is_heap_invariant(&self) -> bool {
        let parent_link = self.get_parent();

        if matches!(parent_link, None) {
            return false;
        }

        let parent = parent_link.as_ref().unwrap();
        return self.read().unwrap().priority > parent.read().unwrap().priority;
    }

    fn search(&self, key: &Vec<u8>) -> bool {
        if self.read().unwrap().key == *key {
            return true;
        }

        let has_left = !matches!(self.get_left(), None);
        if has_left && *key < self.read().unwrap().key {
            return self.get_left().unwrap().search(key);
        }

        let has_right = !matches!(self.get_right(), None);
        if has_right && *key > self.read().unwrap().key {
            return self.get_right().unwrap().search(key);
        }

        return false;
    }
}

#[derive(Debug)]
pub struct Memtable {
    root: Link,
    size: u32,
    id: f64,
}

/**
 * this is the real implementation
 */
impl Memtable {
    pub fn new() -> Self {
        println!("making new memtable");
        let mut rng = rand::thread_rng();
        let y: f64 = rng.gen();
        Memtable {
            id: y,
            root: None,
            size: 0,
        }
    }

    pub fn size(&self) -> u32 {
        return self.size;
    }

    pub fn search(&self, key: &Vec<u8>) -> bool {
        if matches!(self.root, None) {
            return false;
        }

        let node = self.root.as_ref().unwrap().clone();
        return node.search(key);
    }

    pub fn insert(&mut self, key: Vec<u8>, priority: f64) {
        let new_node = Arc::new(RwLock::new(Node {
            key,
            priority,
            left: None,
            right: None,
            parent: None,
        }));

        // oops the tree is empty - new node is the root
        if matches!(self.root, None) {
            println!("seting root!!");
            self.root = Some(new_node);
            return;
        }

        // find the parent of the node we're going to insert
        let mut node_link: Link = Some(self.root.as_ref().unwrap().clone());
        let mut parent_link: Link = None;

        while !matches!(node_link, None) {
            let node = node_link.as_ref().unwrap().clone();
            parent_link = Some(node.clone());
            if new_node.read().unwrap().key > node.read().unwrap().key {
                node_link = node.get_right();
            } else {
                node_link = node.get_left();
            }
        }

        let parent = parent_link.as_ref().unwrap().clone();
        if parent.read().unwrap().key <= new_node.read().unwrap().key {
            parent.set_right(Some(new_node.clone()))
        } else {
            parent.set_left(Some(new_node.clone()));
        }
        new_node.set_parent(Some(parent.clone()));

        println!("checkin heap invariant");
        while new_node.is_heap_invariant() {
            let parent = new_node.get_parent().unwrap();
            if parent.is_left_child(Some(new_node.clone())) {
                self.rotate_right(&mut new_node.clone());
            } else {
                self.rotate_left(&mut new_node.clone());
            }
        }

        self.size += 1;
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

/*
#[cfg(test)]
mod search_tests {
    use super::*;

    #[test]
    fn test_it_can_search_find() {
        let mut m = Memtable::<i32, i32> { root: None };
        m.insert(3, 4);
        assert_eq!(true, m.search(&3));
    }

    #[test]
    fn test_it_can_search_if_not_in_tree() {
        let mut m = Memtable::<i32, i32> { root: None };
        m.insert(3, 0);
        assert_eq!(false, m.search(&2));
    }

    #[test]
    fn test_it_can_search_nested_find_yes() {
        let mut m = Memtable::<u32, u32> { root: None };
        m.insert(3, 3);
        m.insert(2, 2);
        m.insert(1, 1);
        assert_eq!(true, m.search(&1));
    }

    #[test]
    fn test_it_can_search_nested_find_not_in_tree() {
        let mut m = Memtable::<u32, u32> { root: None };
        m.insert(3, 3);
        m.insert(2, 2);
        m.insert(1, 1);
        assert_eq!(false, m.search(&0));
    }
}

#[cfg(test)]
mod insert_tests {
    use super::*;

    #[test]
    fn test_insert_to_root() {
        // TODO
    }

    #[test]
    fn test_insert_some_rotations() {
        let mut m = Memtable::<u32, u32> { root: None };
        let p = Arc::new(RefCell::new(Node {
            key: 50,
            priority: 50,
            left: None,
            right: None,
            parent: None,
        }));
        let y = Arc::new(RefCell::new(Node {
            key: 40,
            priority: 40,
            left: None,
            right: None,
            parent: None,
        }));
        let x = Arc::new(RefCell::new(Node {
            key: 30,
            priority: 30,
            left: None,
            right: None,
            parent: None,
        }));

        m.root = Some(p.clone());

        p.set_left(Some(y.clone()));
        y.set_parent(Some(p.clone()));

        y.set_left(Some(x.clone()));
        x.set_parent(Some(y.clone()));

        m.insert(35, 45);

        assert_eq!(true, Arc::ptr_eq(&p, m.root.as_ref().unwrap()));

        let new_node = p.get_left().unwrap().clone();
        assert_eq!(35, new_node.read().unwrap().key);

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
        let mut m = Memtable::<i32, i32>::new();
        let p = Arc::new(RefCell::new(Node::<i32, i32> {
            key: 0,
            priority: 0,
            left: None,
            right: None,
            parent: None,
        }));
        let y = Arc::new(RefCell::new(Node::<i32, i32> {
            key: 1,
            priority: 0,
            left: None,
            right: None,
            parent: None,
        }));
        let x = Arc::new(RefCell::new(Node::<i32, i32> {
            key: 2,
            priority: 0,
            left: None,
            right: None,
            parent: None,
        }));
        let x_left = Arc::new(RefCell::new(Node::<i32, i32> {
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
        let mut m = Memtable::<i32, i32>::new();
        let p = Arc::new(RefCell::new(Node::<i32, i32> {
            key: 0,
            priority: 0,
            left: None,
            right: None,
            parent: None,
        }));
        let y = Arc::new(RefCell::new(Node::<i32, i32> {
            key: 1,
            priority: 0,
            left: None,
            right: None,
            parent: None,
        }));
        let x = Arc::new(RefCell::new(Node::<i32, i32> {
            key: 2,
            priority: 0,
            left: None,
            right: None,
            parent: None,
        }));
        let x_left = Arc::new(RefCell::new(Node::<i32, i32> {
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
        let mut m = Memtable::<i32, i32>::new();
        let p = Arc::new(RefCell::new(Node::<i32, i32> {
            key: 0,
            priority: 0,
            left: None,
            right: None,
            parent: None,
        }));

        let x = Arc::new(RefCell::new(Node::<i32, i32> {
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

        assert_eq!(true, Arc::ptr_eq(&x, m.root.as_ref().unwrap()));
        assert_eq!(true, matches!(x.get_parent(), None));

        assert_eq!(true, x.is_left_child(Some(p.clone())));
        assert_eq!(true, p.is_parent(Some(x.clone())));

        assert_eq!(true, matches!(p.get_right(), None));
    }

    #[test]
    #[should_panic]
    fn test_rotate_left_panics_if_x_is_root() {
        let mut m = Memtable::<i32, i32>::new();
        let x = Arc::new(RefCell::new(Node::<i32, i32> {
            key: 0,
            priority: 0,
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
        let mut m = Memtable::<i32, i32>::new();
        let p = Arc::new(RefCell::new(Node::<i32, i32> {
            key: 0,
            priority: 0,
            left: None,
            right: None,
            parent: None,
        }));

        let x = Arc::new(RefCell::new(Node::<i32, i32> {
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
        let mut m = Memtable::<i32, i32>::new();
        let x = Arc::new(RefCell::new(Node::<i32, i32> {
            key: 0,
            priority: 0,
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
        let mut m = Memtable::<i32, i32>::new();
        let p = Arc::new(RefCell::new(Node::<i32, i32> {
            key: 0,
            priority: 0,
            left: None,
            right: None,
            parent: None,
        }));
        let x = Arc::new(RefCell::new(Node::<i32, i32> {
            key: 1,
            priority: 2,
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
        let mut m = Memtable::<i32, i32>::new();
        let p = Arc::new(RefCell::new(Node::<i32, i32> {
            key: 0,
            priority: 0,
            left: None,
            right: None,
            parent: None,
        }));
        let x = Arc::new(RefCell::new(Node::<i32, i32> {
            key: 1,
            priority: 2,
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
        let mut m = Memtable::<i32, i32>::new();
        let p = Arc::new(RefCell::new(Node::<i32, i32> {
            key: 0,
            priority: 0,
            left: None,
            right: None,
            parent: None,
        }));
        let y = Arc::new(RefCell::new(Node::<i32, i32> {
            key: 1,
            priority: 1,
            left: None,
            right: None,
            parent: None,
        }));
        let x = Arc::new(RefCell::new(Node::<i32, i32> {
            key: 2,
            priority: 2,
            left: None,
            right: None,
            parent: None,
        }));
        let x_right = Arc::new(RefCell::new(Node::<i32, i32> {
            key: 3,
            priority: 3,
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
        let mut m = Memtable::<i32, i32>::new();
        let p = Arc::new(RefCell::new(Node::<i32, i32> {
            key: 0,
            priority: 0,
            left: None,
            right: None,
            parent: None,
        }));
        let y = Arc::new(RefCell::new(Node::<i32, i32> {
            key: 1,
            priority: 1,
            left: None,
            right: None,
            parent: None,
        }));
        let x = Arc::new(RefCell::new(Node::<i32, i32> {
            key: 2,
            priority: 2,
            left: None,
            right: None,
            parent: None,
        }));
        let x_right = Arc::new(RefCell::new(Node::<i32, i32> {
            key: 3,
            priority: 3,
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
*/
#[derive(Debug)]
pub struct MemtableIterator {
    unvisited: Vec<Link>,
}

impl MemtableIterator {
    fn push_left_edge(&mut self, tree: &Memtable) {
        if matches!(tree.root, None) {
            return;
        }
        let mut link = Some(tree.root.as_ref().unwrap().clone());
        println!("link === {:?}", link);
        while !matches!(link, None) {
            println!("link 2 === {:?}", link);
            let node = link.as_ref().unwrap();
            self.unvisited.push(Some(node.clone()));
            println!("self == {:?}", self);
            link = node.get_left();
        }
    }
}

impl Iterator for MemtableIterator {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        println!("next {:?}", self);
        let link = self.unvisited.pop()?;

        let node = link.as_ref().unwrap();
        self.push_left_edge(&Memtable::new());
        return Some(node.read().unwrap().key.clone());
    }
}

impl IntoIterator for Memtable {
    type Item = Vec<u8>;
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
        iter.push_left_edge(self);
        iter
    }
}
