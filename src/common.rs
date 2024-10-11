pub type Key = Vec<u8>;
pub type Value = Vec<u8>;
pub type SequenceNumber = u64;

pub enum Entry {
    Put { key: Key, value: Value },
    Delete { key: Key },
}
