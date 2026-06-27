use std::cmp::Reverse;
use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq)]
pub enum Op {
    Put(Vec<u8>),
    Delete,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Lookup {
    Found(Vec<u8>),
    Deleted,
    Absent,
}

#[derive(Default)]
pub struct Memtable {
    entries: BTreeMap<(Vec<u8>, Reverse<u64>), Op>,
    bytes: usize,
}

impl Memtable {
    pub fn new() -> Self {
        Memtable::default()
    }

    pub fn insert(&mut self, key: Vec<u8>, seq: u64, op: Op) {
        let value_len = match &op {
            Op::Put(value) => value.len(),
            Op::Delete => 0,
        };
        self.bytes += key.len() + value_len + 16;
        self.entries.insert((key, Reverse(seq)), op);
    }

    pub fn get(&self, key: &[u8], snapshot: u64) -> Lookup {
        let lower = (key.to_vec(), Reverse(snapshot));
        match self.entries.range(lower..).next() {
            Some(((found, _), op)) if found.as_slice() == key => match op {
                Op::Put(value) => Lookup::Found(value.clone()),
                Op::Delete => Lookup::Deleted,
            },
            _ => Lookup::Absent,
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = (&[u8], u64, &Op)> {
        self.entries
            .iter()
            .map(|((key, seq), op)| (key.as_slice(), seq.0, op))
    }

    pub fn into_entries(self) -> Vec<(Vec<u8>, u64, Op)> {
        self.entries
            .into_iter()
            .map(|((key, Reverse(seq)), op)| (key, seq, op))
            .collect()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn bytes(&self) -> usize {
        self.bytes
    }
}
