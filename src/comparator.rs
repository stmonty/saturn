use std::cmp::Ordering;

/// A `Comparator` trait defines a total ordering over a set of keys.
pub trait Comparator: Send + Sync + 'static {
    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering;

    fn name(&self) -> &'static str;

    fn find_shortest_separator(&self, start: &[u8], limit: &[u8]) -> Vec<u8> {
        let min_len = start.len().min(limit.len());
        let mut diff_index = 0;
        while diff_index < min_len && start[diff_index] == limit[diff_index] {
            diff_index += 1;
        }

        if diff_index >= min_len {
        } else {
            let diff_byte = start[diff_index];
            if diff_byte < 0xff && diff_byte + 1 < limit[diff_index] {
                let mut separator = start[..=diff_index].to_vec();
                separator[diff_index] += 1;
                return separator;
            }
        }

        // Default case: cannot find a shorter separator.
        start.to_vec()
    }

    fn find_short_successor(&self, key: &[u8]) -> Vec<u8> {
        for (i, &byte) in key.iter().enumerate() {
            if byte != 0xff {
                let mut s = key[..=i].to_vec();
                s[i] = byte + 1;
                return s;
            }
        }
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
    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
        a.cmp(b)
    }

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
        assert_eq!(
            comparator.find_shortest_separator(b"apple", b"apply"),
            b"applf"
        );
        assert_eq!(comparator.find_shortest_separator(b"a", b"c"), b"b");
        assert_eq!(comparator.find_shortest_separator(b"z", b"za"), b"z"); // Prefix case
    }

    #[test]
    fn test_short_successor() {
        let comparator = BytewiseComparator::new();
        assert_eq!(comparator.find_short_successor(b"apple"), b"b");
        assert_eq!(comparator.find_short_successor(b"user"), b"v");
        assert_eq!(comparator.find_short_successor(b"z"), b"{");
        assert_eq!(
            comparator.find_short_successor(&[0xff, 0xff]),
            &[0xff, 0xff]
        );
    }
}
