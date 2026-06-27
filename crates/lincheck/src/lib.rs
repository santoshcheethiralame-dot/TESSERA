use std::collections::{BTreeMap, HashSet};

#[derive(Clone, Debug, PartialEq)]
pub enum OpKind {
    Put(Vec<u8>),
    Delete,
    Get(Option<Vec<u8>>),
}

#[derive(Clone, Debug)]
pub struct Op {
    pub key: Vec<u8>,
    pub kind: OpKind,
    pub invoke: u64,
    pub response: u64,
}

pub fn linearizable(history: &[Op]) -> bool {
    let mut by_key: BTreeMap<&[u8], Vec<&Op>> = BTreeMap::new();
    for op in history {
        by_key.entry(op.key.as_slice()).or_default().push(op);
    }
    by_key.values().all(|ops| register_linearizable(ops))
}

type State = Option<Vec<u8>>;

fn register_linearizable(ops: &[&Op]) -> bool {
    let n = ops.len();
    if n == 0 {
        return true;
    }
    assert!(n <= 128, "per-key history too large for the checker: {n}");
    let full = if n == 128 {
        u128::MAX
    } else {
        (1u128 << n) - 1
    };
    let mut memo: HashSet<(u128, State)> = HashSet::new();
    dfs(ops, full, &None, &mut memo)
}

fn dfs(ops: &[&Op], remaining: u128, state: &State, memo: &mut HashSet<(u128, State)>) -> bool {
    if remaining == 0 {
        return true;
    }
    let key = (remaining, state.clone());
    if memo.contains(&key) {
        return false;
    }
    let min_response = (0..ops.len())
        .filter(|&i| remaining & (1u128 << i) != 0)
        .map(|i| ops[i].response)
        .min()
        .unwrap();
    for i in 0..ops.len() {
        if remaining & (1u128 << i) == 0 || ops[i].invoke > min_response {
            continue;
        }
        let next = remaining & !(1u128 << i);
        let ok = match &ops[i].kind {
            OpKind::Put(value) => dfs(ops, next, &Some(value.clone()), memo),
            OpKind::Delete => dfs(ops, next, &None, memo),
            OpKind::Get(returned) => returned == state && dfs(ops, next, state, memo),
        };
        if ok {
            return true;
        }
    }
    memo.insert(key);
    false
}
