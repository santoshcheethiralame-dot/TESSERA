use storage::{Db, Disk, MemDisk};

fn sst_count(disk: &MemDisk) -> usize {
    disk.list()
        .into_iter()
        .filter(|n| n.starts_with("sst-"))
        .count()
}

#[test]
fn compaction_merges_tables_into_one() {
    let disk = MemDisk::new();
    let mut db = Db::open(disk.clone()).unwrap();
    db.put(b"a", b"1").unwrap();
    db.flush().unwrap();
    db.put(b"b", b"2").unwrap();
    db.flush().unwrap();
    db.put(b"c", b"3").unwrap();
    db.flush().unwrap();
    assert_eq!(sst_count(&disk), 3);

    db.compact().unwrap();
    assert_eq!(sst_count(&disk), 1);
    assert_eq!(db.get(b"a").unwrap(), Some(b"1".to_vec()));
    assert_eq!(db.get(b"b").unwrap(), Some(b"2".to_vec()));
    assert_eq!(db.get(b"c").unwrap(), Some(b"3".to_vec()));
}

#[test]
fn compaction_drops_overwritten_versions() {
    let disk = MemDisk::new();
    let mut db = Db::open(disk.clone()).unwrap();
    db.put(b"k", b"v1").unwrap();
    db.flush().unwrap();
    db.put(b"k", b"v2").unwrap();
    db.flush().unwrap();
    db.put(b"k", b"v3").unwrap();
    db.flush().unwrap();

    db.compact().unwrap();
    assert_eq!(sst_count(&disk), 1);
    assert_eq!(db.get(b"k").unwrap(), Some(b"v3".to_vec()));
}

#[test]
fn compaction_drops_tombstones() {
    let disk = MemDisk::new();
    let mut db = Db::open(disk.clone()).unwrap();
    db.put(b"k", b"v").unwrap();
    db.flush().unwrap();
    db.delete(b"k").unwrap();
    db.flush().unwrap();

    db.compact().unwrap();
    assert_eq!(db.get(b"k").unwrap(), None);

    let reopened = Db::open(disk.clone()).unwrap();
    assert_eq!(reopened.get(b"k").unwrap(), None);
}

#[test]
fn auto_compaction_keeps_table_count_bounded() {
    let disk = MemDisk::new();
    let mut db = Db::open(disk.clone()).unwrap();
    db.set_flush_threshold(64);
    db.set_compaction_threshold(3);
    for i in 0..300u32 {
        let key = format!("key{i:04}");
        db.put(key.as_bytes(), b"v").unwrap();
    }
    db.flush().unwrap();

    assert!(sst_count(&disk) <= 3);
    assert_eq!(db.get(b"key0000").unwrap(), Some(b"v".to_vec()));
    assert_eq!(db.get(b"key0150").unwrap(), Some(b"v".to_vec()));
    assert_eq!(db.get(b"key0299").unwrap(), Some(b"v".to_vec()));
    assert_eq!(db.get(b"key0300").unwrap(), None);
}

#[test]
fn data_survives_compaction_and_reopen() {
    let disk = MemDisk::new();
    {
        let mut db = Db::open(disk.clone()).unwrap();
        for i in 0..50u32 {
            let key = format!("k{i:03}");
            db.put(key.as_bytes(), key.as_bytes()).unwrap();
            db.flush().unwrap();
        }
        db.compact().unwrap();
        assert_eq!(sst_count(&disk), 1);
    }
    let db = Db::open(disk.clone()).unwrap();
    assert_eq!(db.get(b"k000").unwrap(), Some(b"k000".to_vec()));
    assert_eq!(db.get(b"k025").unwrap(), Some(b"k025".to_vec()));
    assert_eq!(db.get(b"k049").unwrap(), Some(b"k049".to_vec()));
}
