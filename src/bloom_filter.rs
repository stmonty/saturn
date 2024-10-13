use crate::common::Key;

pub struct BloomFilter {
    bit_array: Vec<bool>,
    size: usize,
}

impl Default for BloomFilter {
    fn default() -> Self {
        Self {
            bit_array: vec![false; 1024],
            size: 1024,
        }
    }
}

impl BloomFilter {
    pub fn new(size: usize) -> Self {
        Self {
            bit_array: vec![false; size],
            size,
        }
    }

    pub fn add(&mut self, key: &Key) {
        let hash1 = self.hash1(key) % self.size;
        let hash2 = self.hash2(key) % self.size;
        self.bit_array[hash1] = true;
        self.bit_array[hash2] = true;
    }

    pub fn contains(&self, key: &Key) -> bool {
        let hash1 = self.hash1(key) % self.size;
        let hash2 = self.hash2(key) % self.size;
        self.bit_array[hash1] && self.bit_array[hash2]
    }

    fn hash1(&self, key: &Key) -> usize {
        key.iter().fold(0, |acc, &b| acc.wrapping_add(b as usize))
    }

    fn hash2(&self, key: &Key) -> usize {
        key.iter().fold(0, |acc, &b| acc.wrapping_mul(31).wrapping_add(b as usize))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bloom_filter_basic() {
        let mut bf = BloomFilter::default();
        let key1 = b"alpha".to_vec();
        let key2 = b"beta".to_vec();
        let key3 = b"gamma".to_vec();

        bf.add(&key1);
        bf.add(&key2);
        bf.add(&key3);

        assert!(bf.contains(&key1));
        assert!(bf.contains(&key2));
        assert!(bf.contains(&key3));
    }

    #[test]
    fn test_bloom_filter_add_and_contains() {
        let mut bf = BloomFilter::default();
        let keys: Vec<Key> = vec![
            b"apple".to_vec(),
            b"banana".to_vec(),
            b"cherry".to_vec(),
        ];

        for key in &keys {
            bf.add(key);
        }

        for key in &keys {
            assert!(bf.contains(key), "Bloom Filter should contain {:?}", key);
        }

        let non_keys: Vec<Key> = vec![
            b"durian".to_vec(),
            b"elderberry".to_vec(),
            b"fig".to_vec(),
        ];

        for key in &non_keys {
            assert!(!bf.contains(key), "Bloom Filter should not contain {:?}", key);
        }
    }
}
