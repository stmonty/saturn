use crate::common::Key;

pub struct BloomFilter {
    bit_array: Vec<bool>,
    size: usize,
}

impl BloomFilter {
    pub fn new() -> Self {
        Self {
            bit_array: vec![false; 1024], // Size can be adjusted
            size: 1024,
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
