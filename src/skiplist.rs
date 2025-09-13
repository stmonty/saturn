use crate::common::{Key, Value};
use crate::comparator::Comparator;
use rand::Rng;
use std::cmp::Ordering;
use std::marker::PhantomData;
use std::ptr;
use std::sync::Arc;

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
        Node {
            key: key,
            val: value,
            next: vec![ptr::null_mut(); level],
        }
    }
}

pub struct SkipList {
    head: Link,
    level: usize,
    comparator: Arc<dyn Comparator>,
    rng: rand::rngs::ThreadRng,
}

impl SkipList {
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
                (&mut (*new_node)).next[i] = (&(*update[i])).next[i];
                (&mut (*update[i])).next[i] = new_node;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::comparator::BytewiseComparator;

    #[test]
    fn test_insert_and_get() {
        let comparator = Arc::new(BytewiseComparator::new());
        let mut list = SkipList::new(comparator);

        list.insert(b"apple".to_vec(), b"red".to_vec());
        list.insert(b"banana".to_vec(), b"yellow".to_vec());
        list.insert(b"cherry".to_vec(), b"dark red".to_vec());

        // Test successful gets
        let banana_val = list.get(b"banana").unwrap();
        assert_eq!(*banana_val, b"yellow".to_vec());

        let apple_val = list.get(b"apple").unwrap();
        assert_eq!(*apple_val, b"red".to_vec());

        // Test unsuccessful get
        assert!(list.get(b"grape").is_none());
    }

    #[test]
    fn test_iterator_order() {
        let comparator = Arc::new(BytewiseComparator::new());
        let mut list = SkipList::new(comparator);

        list.insert(b"zulu".to_vec(), b"4".to_vec());
        list.insert(b"alpha".to_vec(), b"1".to_vec());
        list.insert(b"bravo".to_vec(), b"2".to_vec());
        list.insert(b"x-ray".to_vec(), b"3".to_vec());

        let results: Vec<_> = list.iter().map(|(k, v)| (k.clone(), v.clone())).collect();

        assert_eq!(results.len(), 4);
        assert_eq!(results[0], (b"alpha".to_vec(), b"1".to_vec()));
        assert_eq!(results[1], (b"bravo".to_vec(), b"2".to_vec()));
        assert_eq!(results[2], (b"x-ray".to_vec(), b"3".to_vec()));
        assert_eq!(results[3], (b"zulu".to_vec(), b"4".to_vec()));
    }

    #[test]
    fn test_duplicates_are_inserted() {
        let comparator = Arc::new(BytewiseComparator::new());
        let mut list = SkipList::new(comparator);

        // Insert two duplicates: the later insert should come first among equals.
        list.insert(b"key1".to_vec(), b"value2".to_vec()); // older
        list.insert(b"key1".to_vec(), b"value1".to_vec()); // newer

        let collected: Vec<_> = list.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        assert_eq!(collected.len(), 2);

        // Among equal keys, iteration shows newest first (LIFO for duplicates).
        assert_eq!(collected[0], (b"key1".to_vec(), b"value1".to_vec()));
        assert_eq!(collected[1], (b"key1".to_vec(), b"value2".to_vec()));

        // get() returns the most recent value for the key (last-writer-wins).
        let val = list.get(b"key1").unwrap();
        assert_eq!(*val, b"value1".to_vec());
    }

    #[test]
    fn test_empty_list() {
        let comparator = Arc::new(BytewiseComparator::new());
        let list = SkipList::new(comparator);

        assert!(list.get(b"any_key").is_none());
        assert_eq!(list.iter().count(), 0);
    }

    #[test]
    fn test_drop_no_leaks() {
        // This test simply creates and drops a list with many items.
        // Running this with a tool like `valgrind` would confirm no memory is leaked.
        let comparator = Arc::new(BytewiseComparator::new());
        let mut list = SkipList::new(comparator);
        for i in 0..1000 {
            let key = format!("key{}", i);
            let val = format!("val{}", i);
            list.insert(key.into_bytes(), val.into_bytes());
        }
        // `list` is dropped here automatically, and our `Drop` implementation is called.
    }
}
