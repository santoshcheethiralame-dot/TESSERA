use std::cmp::Reverse;
use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq)]
pub enum Op {
    Put(Vec<u8>),
    Delete,
}

#[derive(Default)]
pub struct Memtable {
    entries: BTreeMap<(Vec<u8>, Reverse<u64>), Op>,
}

impl Memtable {
    pub fn new() -> Self {
        Memtable::default()
    }

    pub fn insert(&mut self, key: Vec<u8>, seq: u64, op: Op) {
        self.entries.insert((key, Reverse(seq)), op);
    }

    pub fn get(&self, key: &[u8], snapshot: u64) -> Option<&[u8]> {
        let lower = (key.to_vec(), Reverse(snapshot));
        let ((found, _), op) = self.entries.range(lower..).next()?;
        if found.as_slice() != key {
            return None;
        }
        match op {
            Op::Put(value) => Some(value),
            Op::Delete => None,
        }
    }
}
