use crate::comparator::Comparator;
use rand::Rng;
use std::cell::RefCell;
use std::cmp::Ordering;
use std::rc::Rc;
use std::sync::Arc;


/// The maximum number of levels in the skip list.
/// This limits the number of elements to (1/P)^MAX_LEVEL.
/// For P=0.25, this supports over 268 million items.
const MAX_LEVEL: usize = 12;

/// The probability factor for randomization of levels.
const P: f64 = 0.25;

type Link<T> = Option<Rc<RefCell<Node<T>>>>;

struct Node<T> {
    /// The key-value pair stored in the node.
    /// The head node will have this as `None`.
    element: Option<T>,
    /// An array of forward pointers. `next[i]` points to the next node at level `i`.
    next: Vec<Link<T>>,
}

impl<T> Node<T> {

    pub fn new(element: T, level: usize) -> Self {
        Node { element: Some(element), next: vec![None; level] }
    }

    pub fn new_head() -> Self {
        Node { element: None, next: vec![None; MAX_LEVEL] }
    }
}

pub struct SkipList<T: AsRef<[u8]> + Clone> {
    head: Rc<RefCell<Node<T>>>,
    level: usize,
    comparator: Arc<dyn Comparator>,
    rng: rand::rngs::ThreadRng,
}

impl<T: AsRef<[u8]> + Clone> SkipList<T>
{
    pub fn new(comparator: Arc<dyn Comparator>) -> Self {
        SkipList { head: Rc::new(RefCell::new(Node::new_head())), level: 1, comparator: comparator, rng: rand::rng() }
    }

    fn random_level(&mut self) -> usize {
        let mut level = 1;
        while self.rng.random::<f64>() < P && level < MAX_LEVEL {
            level += 1;
        }
        level
    }
       /// Inserts an element into the skip list.
    /// If an element with an equivalent key already exists, it is not overwritten.
    /// The new element is inserted according to the comparator's ordering.
    pub fn insert(&mut self, element: T) {
        let mut update = vec![Rc::clone(&self.head); MAX_LEVEL];
        let mut current = Rc::clone(&self.head);

        // Traverse from the top level down to find the insertion point at each level.
        for i in (0..self.level).rev() {
            loop {
                let next_node_link = current.borrow().next[i].clone();

                match next_node_link {
                    Some(ref next_node) => {
                        // Bind the borrow guard to a variable. Its lifetime now extends for this entire scope.
                        let next_node_guard = next_node.borrow();
                        // This reference is now valid because `next_node_guard` is still alive.
                        let next_element = next_node_guard.element.as_ref().unwrap();
                        
                        if self.comparator.compare(next_element.as_ref(), element.as_ref()) == Ordering::Less {
                            // Advance `current`. This is safe.
                            current = Rc::clone(next_node);
                        } else {
                            // Found insertion point for this level.
                            break;
                        }
                    }
                    None => {
                        // Reached the end of the list at this level.
                        break;
                    }
                }
            }
            update[i] = Rc::clone(&current);
        }

        // Determine the level for the new node.
        let new_level = self.random_level();
        if new_level > self.level {
            // If the new node is taller than the current list, update the list's level.
            self.level = new_level;
        }

        // Create and splice the new node into the list.
        let new_node = Rc::new(RefCell::new(Node::new(element, new_level)));
        for i in 0..new_level {
            new_node.borrow_mut().next[i] = update[i].borrow_mut().next[i].take();
            update[i].borrow_mut().next[i] = Some(Rc::clone(&new_node));
        }
    }

    /// Searches for an element with the given key.
    /// Returns a clone of the element if found.
    pub fn get(&self, key: &[u8]) -> Option<T> {
        let mut current = Rc::clone(&self.head);
        // Traverse from the top level down, getting as close as possible to the key.
        for i in (0..self.level).rev() {
            loop {
                let next_node_link = current.borrow().next[i].clone();

                match next_node_link {
                    Some(ref next_node) => {
                        // Bind the borrow guard to a variable to extend its lifetime.
                        let next_node_guard = next_node.borrow();
                        // This reference is valid as long as `next_node_guard` is in scope.
                        let next_element = next_node_guard.element.as_ref().unwrap();

                        if self.comparator.compare(next_element.as_ref(), key) == Ordering::Less {
                            // Advance `current`.
                            current = Rc::clone(next_node);
                        } else {
                            // Key is >= next_element, so drop down.
                            break;
                        }
                    }
                    None => {
                        // Reached the end of the list at this level.
                        break;
                    }
                }
            }
        }

        // After the search, `current` is the predecessor of the potential match.
        // We need to check the next node at the bottom level (level 0).
        if let Some(next_node) = current.borrow().next[0].clone() {
            let next_node_guard = next_node.borrow();
            let next_element = next_node_guard.element.as_ref().unwrap();
            if self.comparator.compare(next_element.as_ref(), key) == Ordering::Equal {
                return Some(next_element.clone());
            }
        }

        None
    }

    pub fn iter(&self) -> SkipListIterator<T> {
        SkipListIterator { current: self.head.borrow().next[0].clone() }
    }

}


/// An iterator for the `SkipList`.
pub struct SkipListIterator<T> {
    current: Link<T>,
}

impl<T> Iterator for SkipListIterator<T>
where
    T: Clone,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.current.take().map(|node| {
            let node_borrow = node.borrow();
            self.current = node_borrow.next[0].clone();
            node_borrow.element.as_ref().unwrap().clone()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::{Key, SequenceNumber};
    use crate::comparator::{BytewiseComparator};

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct InternalKey {
        user_key: Key,
        sequence: SequenceNumber,
        // In a real implementation, you'd also have a `ValueType` enum here.
    }

    impl InternalKey {
        fn new(key: &[u8], seq: SequenceNumber) -> Self {
            Self {
                user_key: key.to_vec(),
                sequence: seq,
            }
        }
    }

    /// We implement `AsRef<[u8]>` so the SkipList can pass the `InternalKey`
    /// to the `BytewiseComparator`. The comparator will then only operate
    /// on the `user_key` portion, which is the correct behavior for ordering.
    impl AsRef<[u8]> for InternalKey {
        fn as_ref(&self) -> &[u8] {
            &self.user_key
        }
    }


    #[test]
    fn test_insert_and_get() {
        let comparator = Arc::new(BytewiseComparator::new());
        let mut list = SkipList::new(comparator);

        list.insert(InternalKey::new(b"apple", 1));
        list.insert(InternalKey::new(b"banana", 2));
        list.insert(InternalKey::new(b"cherry", 3));

        // We search by the user key slice.
        let result = list.get(b"banana").unwrap();
        assert_eq!(result.user_key, b"banana");
        assert_eq!(result.sequence, 2);

        let result_apple = list.get(b"apple").unwrap();
        assert_eq!(result_apple.user_key, b"apple");
        assert_eq!(result_apple.sequence, 1);

        assert!(list.get(b"grape").is_none());
    }

    #[test]
    fn test_iterator_order() {
        let comparator = Arc::new(BytewiseComparator::new());
        let mut list = SkipList::new(comparator);

        list.insert(InternalKey::new(b"zulu", 4));
        list.insert(InternalKey::new(b"alpha", 1));
        list.insert(InternalKey::new(b"bravo", 2));
        list.insert(InternalKey::new(b"x-ray", 3));

        let results: Vec<InternalKey> = list.iter().collect();

        assert_eq!(results[0].user_key, b"alpha");
        assert_eq!(results[1].user_key, b"bravo");
        assert_eq!(results[2].user_key, b"x-ray");
        assert_eq!(results[3].user_key, b"zulu");
        assert_eq!(results.len(), 4);
    }

     #[test]
    fn test_overwrite_is_not_supported_inserts_duplicates() {
        let comparator = Arc::new(BytewiseComparator::new());
        let mut list = SkipList::new(comparator);

        list.insert(InternalKey::new(b"key1", 2));
        list.insert(InternalKey::new(b"key1", 1));

        let collected: Vec<_> = list.iter().collect();
        assert_eq!(collected.len(), 2);
        // The comparator only sees "key1", so insertion order is maintained for equal keys.
        assert_eq!(collected[0].sequence, 2);
        assert_eq!(collected[1].sequence, 1);
    }
}
