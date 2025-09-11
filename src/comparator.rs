use std::cmp::Ordering;

/// A `Comparator` trait defines a total ordering over a set of keys.
///
/// LSM-style storage engines use comparators to maintain the sorted order of keys
/// within their internal data structures (like memtables and SSTables). This allows
/// for efficient searching, merging, and range scans.
///
/// Beyond simple key comparison, this trait includes methods for creating
/// compact keys, which are crucial for reducing the storage overhead of indexes
/// in SSTables.
pub trait Comparator: Send + Sync + 'static {
    /// Compares two key slices.
    ///
    /// # Returns
    /// - `Ordering::Less` if `a` is less than `b`.
    /// - `Ordering::Equal` if `a` is equal to `b`.
    /// - `Ordering::Greater` if `a` is greater than `b`.
    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering;

    /// Returns the name of the comparator.
    ///
    /// This is used to ensure that a database is not opened with a comparator
    /// different from the one it was created with. The name is stored in the
    /// MANIFEST file.
    fn name(&self) -> &'static str;

    /// Finds a short key to separate `start` and `limit`.
    ///
    /// If `start` is a prefix of `limit`, this function might not be able to find a
    /// separator. Otherwise, it attempts to find a string `s` such that
    /// `start <= s < limit`.
    ///
    /// This is used to create compact index block keys. For example, if an SSTable
    /// block has keys from "apple" to "apply", we can use "applf" as the separator
    /// in the index instead of "apply", saving space.
    ///
    /// The default implementation simply returns `start` cloned into a `Vec<u8>`.
    fn find_shortest_separator(&self, start: &[u8], limit: &[u8]) -> Vec<u8> {
        // Find the length of the common prefix
        let min_len = start.len().min(limit.len());
        let mut diff_index = 0;
        while diff_index < min_len && start[diff_index] == limit[diff_index] {
            diff_index += 1;
        }

        if diff_index >= min_len {
            // One key is a prefix of the other, no separation possible
        } else {
            let diff_byte = start[diff_index];
            // If the differing byte in `start` can be incremented and is still less than
            // the corresponding byte in `limit`, we can create a shorter separator.
            if diff_byte < 0xff && diff_byte + 1 < limit[diff_index] {
                let mut separator = start[..=diff_index].to_vec();
                separator[diff_index] += 1;
                return separator;
            }
        }

        // Default case: cannot find a shorter separator.
        start.to_vec()
    }

    /// Finds a short key that is a successor to `key`.
    ///
    /// This function finds a string `s` such that `s >= key`. It is used to define
    /// an exclusive upper bound for a range. For example, if we want to scan all
    /// keys starting with "user", the successor of "user" could be "usf", which
    /// would serve as the exclusive end of our scan range.
    ///
    /// The default implementation finds the first byte that can be incremented
    /// and truncates the rest of the key.
    fn find_short_successor(&self, key: &[u8]) -> Vec<u8> {
        for (i, &byte) in key.iter().enumerate() {
            if byte != 0xff {
                let mut successor = key[..=i].to_vec();
                successor[i] += 1;
                return successor;
            }
        }
        // If all bytes are 0xff, we can't create a shorter successor.
        key.to_vec()
    }
}

/// A comparator that provides a lexicographical (bytewise) ordering of keys.
///
/// This is the most common comparator used in key-value stores. It compares
/// byte slices directly, which is suitable for most applications where keys
/// are strings or binary data.
#[derive(Default, Clone, Copy)]
pub struct BytewiseComparator;

impl BytewiseComparator {
    pub fn new() -> Self {
        BytewiseComparator
    }
}

impl Comparator for BytewiseComparator {
    /// Compares two byte slices lexicographically.
    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
        a.cmp(b)
    }

    /// The name of this comparator.
    fn name(&self) -> &'static str {
        "BytewiseComparator"
    }

    // Note: The default implementations of `find_shortest_separator` and
    // `find_short_successor` are generally sufficient for a bytewise comparator,
    // so we don't need to override them here.
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bytewise_comparator_compare() {
        let comparator = BytewiseComparator::new();
        assert_eq!(comparator.compare(b"apple", b"apple"), Ordering::Equal);
        assert_eq!(comparator.compare(b"apple", b"banana"), Ordering::Less);
        assert_eq!(comparator.compare(b"banana", b"apple"), Ordering::Greater);
        assert_eq!(comparator.compare(b"a", b"apple"), Ordering::Less);
    }

    #[test]
    fn test_bytewise_comparator_name() {
        let comparator = BytewiseComparator::new();
        assert_eq!(comparator.name(), "BytewiseComparator");
    }

    #[test]
    fn test_shortest_separator() {
        let comparator = BytewiseComparator::new();
        assert_eq!(comparator.find_shortest_separator(b"apple", b"apply"), b"applf");
        assert_eq!(comparator.find_shortest_separator(b"a", b"c"), b"b");
        assert_eq!(comparator.find_shortest_separator(b"z", b"za"), b"z"); // Prefix case
    }

    #[test]
    fn test_short_successor() {
        let comparator = BytewiseComparator::new();
        assert_eq!(comparator.find_short_successor(b"apple"), b"applf");
        assert_eq!(comparator.find_short_successor(b"user"), b"usf");
        assert_eq!(comparator.find_short_successor(b"z"), vec![0xff]);
        assert_eq!(comparator.find_short_successor(&[0xff, 0xff]), &[0xff, 0xff]);
    }
}
