use crate::common::{Key, Value};
use crate::comparator::{Comp};

struct Node {
    skips: Vec<Option<*mut Node>>,
    next: Option<Box<Node>>,
    key: Key,
    value: Value,
}


impl SkipListLevel<C: Comp>
