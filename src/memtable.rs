use std::collections::BTreeMap;

use crate::common::{Key, Value, SequenceNumber};

pub struct MemTable {
    pub map: BTreeMap<Key, (Value, SequenceNumber)>,
    pub tombstones: BTreeMap<Key, SequenceNumber>,
    pub current_sequence_number: SequenceNumber,
}

impl MemTable {
    pub fn new() -> Self {
        Self {
            map: BTreeMap::new(),
            tombstones: BTreeMap::new(),
            current_sequence_number: 0,
        }
    }

    pub fn insert(&mut self, key: Key, value: Value) {
        self.current_sequence_number += 1;
        self.map.insert(key, (value, self.current_sequence_number));
    }

    pub fn delete(&mut self, key: Key) {
        self.current_sequence_number += 1;
        self.map.remove(&key);
        self.tombstones.insert(key, self.current_sequence_number);
    }

    pub fn get(&self, key: &Key) -> Option<&(Value, SequenceNumber)> {
        self.map.get(key)
    }

    pub fn is_full(&self) -> bool {
        self.map.len() + self.tombstones.len() >= 1000
    }

    pub fn flush(&mut self) -> (BTreeMap<Key, (Value, SequenceNumber)>, BTreeMap<Key, SequenceNumber>) {
        let data = std::mem::replace(&mut self.map, BTreeMap::new());
        let tombstones = std::mem::replace(&mut self.tombstones, BTreeMap::new());
        (data, tombstones)
    }
}
