use lincheck::{linearizable, Op, OpKind};

fn op(key: &[u8], kind: OpKind, invoke: u64, response: u64) -> Op {
    Op {
        key: key.to_vec(),
        kind,
        invoke,
        response,
    }
}

fn put(value: &[u8]) -> OpKind {
    OpKind::Put(value.to_vec())
}

fn got(value: &[u8]) -> OpKind {
    OpKind::Get(Some(value.to_vec()))
}

#[test]
fn empty_history_is_linearizable() {
    assert!(linearizable(&[]));
}

#[test]
fn put_then_get_is_linearizable() {
    let h = vec![op(b"x", put(b"1"), 0, 1), op(b"x", got(b"1"), 2, 3)];
    assert!(linearizable(&h));
}

#[test]
fn stale_read_is_not_linearizable() {
    let h = vec![op(b"x", put(b"1"), 0, 1), op(b"x", OpKind::Get(None), 2, 3)];
    assert!(!linearizable(&h));
}

#[test]
fn concurrent_read_may_see_the_old_value() {
    let h = vec![
        op(b"x", put(b"1"), 0, 10),
        op(b"x", OpKind::Get(None), 1, 2),
    ];
    assert!(linearizable(&h));
}

#[test]
fn reading_an_older_value_after_two_writes_is_not_linearizable() {
    let h = vec![
        op(b"x", put(b"1"), 0, 1),
        op(b"x", put(b"2"), 2, 3),
        op(b"x", got(b"1"), 4, 5),
    ];
    assert!(!linearizable(&h));
}

#[test]
fn reading_a_value_never_written_is_not_linearizable() {
    let h = vec![op(b"x", got(b"1"), 0, 1)];
    assert!(!linearizable(&h));
}

#[test]
fn delete_then_get_absent_is_linearizable() {
    let h = vec![
        op(b"x", put(b"1"), 0, 1),
        op(b"x", OpKind::Delete, 2, 3),
        op(b"x", OpKind::Get(None), 4, 5),
    ];
    assert!(linearizable(&h));
}

#[test]
fn independent_keys_are_checked_separately() {
    let h = vec![
        op(b"a", put(b"1"), 0, 1),
        op(b"b", put(b"2"), 0, 1),
        op(b"a", got(b"1"), 2, 3),
        op(b"b", got(b"2"), 2, 3),
    ];
    assert!(linearizable(&h));
}

#[test]
fn concurrent_writes_with_a_consistent_read_is_linearizable() {
    let h = vec![
        op(b"x", put(b"1"), 0, 5),
        op(b"x", put(b"2"), 0, 5),
        op(b"x", got(b"2"), 6, 7),
    ];
    assert!(linearizable(&h));
}

#[test]
fn read_inconsistent_with_both_concurrent_writes_is_not_linearizable() {
    let h = vec![
        op(b"x", put(b"1"), 0, 5),
        op(b"x", put(b"2"), 0, 5),
        op(b"x", got(b"3"), 6, 7),
    ];
    assert!(!linearizable(&h));
}
