use crate::common::{Key, Value};

struct Node {
    skips: Vec<Option<*mut Node>>,
    next: Option<Box<Node>>,
    key: Key,
    value: Value,
}
