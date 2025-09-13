use crate::comparator::Comparator;
use crate::common::{Key, Value};
use std::ptr;
use rand::Rng;
use std::cmp::Ordering;
use std::sync::Arc;
use std::marker::PhantomData;


/// The maximum number of levels in the skip list.
/// This limits the number of elements to (1/P)^MAX_LEVEL.
/// For P=0.25, this supports over 268 million items.
const MAX_LEVEL: usize = 12;

/// The probability factor for randomization of levels.
const P: f64 = 0.25;

type Link = *mut Node;

struct Node {
    key: Key,
    val: Value,
    next: Vec<Link>,
}

impl Node {

    pub fn new(key: Key, value: Value, level: usize) -> Self {
        Node { key: key, val: value , next: vec![ptr::null_mut(); level] }
    }
}

pub struct SkipList {
    head: Link,
    level: usize,
    comparator: Arc<dyn Comparator>,
    rng: rand::rngs::ThreadRng,
}

impl SkipList
{
    pub fn new(comparator: Arc<dyn Comparator>) -> Self {
        // The head node can now be a regular node with empty key/value pairs.
        let head_node = Box::new(Node::new(Vec::new(), Vec::new(), MAX_LEVEL));

        SkipList {
            head: Box::into_raw(head_node),
            level: 1,
            comparator,
            rng: rand::rng(),
        }
    }

    fn random_level(&mut self) -> usize {
        let mut level = 1;
        while self.rng.random::<f64>() < P && level < MAX_LEVEL {
            level += 1;
        }
        level
    }

    /// Inserts a key-value pair into the skip list.
    pub fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) {
        let mut update = vec![self.head; MAX_LEVEL];
        let mut current = self.head;

        unsafe {
            for i in (0..self.level).rev() {
                // Comparison is now done directly on `next_node.key`.
                while let Some(next_node) = (&(*current)).next[i].as_ref() {
                    // We compare the new key with the key in the next node.
                    if self.comparator.compare(&next_node.key, &key) == Ordering::Less {
                        current = next_node as *const _ as *mut _;
                    } else {
                        break;
                    }
                }
                update[i] = current;
            }

            let new_level = self.random_level();
            if new_level > self.level {
                self.level = new_level;
            }

            let new_node = Box::into_raw(Box::new(Node::new(key, value, new_level)));

            for i in 0..new_level {
                (&mut(*new_node)).next[i] = (&(*update[i])).next[i];
                (&mut(*update[i])).next[i] = new_node;
            }
        }
    }

    /// Searches for a key and returns a reference to its value if found.
    pub fn get(&self, key: &[u8]) -> Option<&Vec<u8>> {
        let mut current = self.head;
        unsafe {
            for i in (0..self.level).rev() {
                while let Some(next_node) = (&(*current)).next[i].as_ref() {
                    if self.comparator.compare(&next_node.key, key) == Ordering::Less {
                        current = next_node as *const _ as *mut _;
                    } else {
                        break;
                    }
                }
            }

            if let Some(next_node) = (&(*current)).next[0].as_ref() {
                if self.comparator.compare(&next_node.key, key) == Ordering::Equal {
                    // Return a reference to the value.
                    return Some(&next_node.val);
                }
            }
        }
        None
    }

    /// Returns an iterator over the key-value pairs in the skip list.
    pub fn iter<'a>(&'a self) -> SkipListIterator<'a> {
        SkipListIterator {
            current: unsafe { (&(*self.head)).next[0] },
            _phantom: PhantomData,
        }
    }
}

impl Drop for SkipList {
    fn drop(&mut self) {
        unsafe {
            let mut current = (&(*self.head)).next[0];
            while !current.is_null() {
                let node_to_free = Box::from_raw(current);
                current = node_to_free.next[0];
            }
            let _ = Box::from_raw(self.head);
        }
    }
}


pub struct SkipListIterator<'a> {
    current: Link,
    _phantom: PhantomData<&'a Node>,
}

impl<'a> Iterator for SkipListIterator<'a> {
    type Item = (&'a Key, &'a Value);
    
    fn next(&mut self) -> Option<Self::Item> {
        if self.current.is_null() {
            None
        } else {
            unsafe {
                let node = &*self.current;
                self.current = node.next[0];
                Some((&node.key, &node.val))
            }
        }
    }
}
