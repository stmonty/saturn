use std::{cmp::Ordering, sync::Arc};

type WrappedCmp = Arc<dyn Cmp>;


pub trait Cmp {
    fn cmp(&self, a: &[u8], b: &[u8]) -> Ordering;

    fn shortest_sep(&self, from: &[u8], to: &[u8]) -> Vec<u8>;

    fn short_successor(&self, key: &[u8]) -> Vec<u8>;

    fn id(&self) -> &'static str;
}

#[derive(Clone)]
pub struct DefaultCmp;

impl Cmp for DefaultCmp {
    fn cmp(&self, a: &[u8], b: &[u8]) -> Ordering {
        a.cmp(b)
    }

    fn id(&self) -> &'static str {
        "saturn/BytewiseComparator"
    }

    fn shortest_sep(&self, a: &[u8], b: &[u8]) -> Vec<u8> {
        todo!()
    }

    fn short_successor(&self, a: &[u8]) -> Vec<u8> {
        todo!()
    }
}

