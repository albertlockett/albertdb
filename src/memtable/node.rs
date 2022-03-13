use std::sync::Arc;
use std::sync::RwLock;

pub struct Node {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
    pub priority: f64,
    pub left: Link,
    pub right: Link,
    pub parent: Link,
}

pub type Link = Option<Arc<RwLock<Node>>>;

pub trait NodeMethods {
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

    fn search(&self, key: &Vec<u8>) -> (Option<Vec<u8>>, bool);
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

impl NodeMethods for Arc<RwLock<Node>> {
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

    fn search(&self, key: &Vec<u8>) -> (Option<Vec<u8>>, bool) {
        if self.read().unwrap().key == *key {
            return (self.read().unwrap().value.clone(), true);
        }

        let has_left = !matches!(self.get_left(), None);
        if has_left && *key < self.read().unwrap().key {
            return self.get_left().unwrap().search(key);
        }

        let has_right = !matches!(self.get_right(), None);
        if has_right && *key > self.read().unwrap().key {
            return self.get_right().unwrap().search(key);
        }

        return (None, false);
    }
}
