use storage::{Db, Disk, MemDisk};

#[test]
fn reads_served_from_flushed_sstable() {
    let mut db = Db::open(MemDisk::new()).unwrap();
    db.put(b"a", b"1").unwrap();
    db.put(b"b", b"2").unwrap();
    db.put(b"c", b"3").unwrap();
    db.flush().unwrap();
    assert_eq!(db.get(b"a").unwrap(), Some(b"1".to_vec()));
    assert_eq!(db.get(b"b").unwrap(), Some(b"2".to_vec()));
    assert_eq!(db.get(b"c").unwrap(), Some(b"3".to_vec()));
    assert_eq!(db.get(b"missing").unwrap(), None);
}

#[test]
fn memtable_shadows_older_sstable() {
    let mut db = Db::open(MemDisk::new()).unwrap();
    db.put(b"k", b"old").unwrap();
    db.flush().unwrap();
    db.put(b"k", b"new").unwrap();
    assert_eq!(db.get(b"k").unwrap(), Some(b"new".to_vec()));
}

#[test]
fn delete_shadows_value_in_sstable() {
    let mut db = Db::open(MemDisk::new()).unwrap();
    db.put(b"k", b"v").unwrap();
    db.flush().unwrap();
    db.delete(b"k").unwrap();
    assert_eq!(db.get(b"k").unwrap(), None);
    db.flush().unwrap();
    assert_eq!(db.get(b"k").unwrap(), None);
}

#[test]
fn newest_sstable_wins() {
    let mut db = Db::open(MemDisk::new()).unwrap();
    db.put(b"k", b"1").unwrap();
    db.flush().unwrap();
    db.put(b"k", b"2").unwrap();
    db.flush().unwrap();
    assert_eq!(db.get(b"k").unwrap(), Some(b"2".to_vec()));
}

#[test]
fn sstables_persist_across_reopen() {
    let disk = MemDisk::new();
    {
        let mut db = Db::open(disk.clone()).unwrap();
        db.put(b"a", b"1").unwrap();
        db.put(b"b", b"2").unwrap();
        db.flush().unwrap();
        db.put(b"c", b"3").unwrap();
        db.sync().unwrap();
    }
    let db = Db::open(disk.clone()).unwrap();
    assert_eq!(db.get(b"a").unwrap(), Some(b"1".to_vec()));
    assert_eq!(db.get(b"b").unwrap(), Some(b"2".to_vec()));
    assert_eq!(db.get(b"c").unwrap(), Some(b"3".to_vec()));
}

#[test]
fn large_sstable_spans_multiple_blocks() {
    let mut db = Db::open(MemDisk::new()).unwrap();
    for i in 0..500u32 {
        let key = format!("k{i:05}");
        db.put(key.as_bytes(), &[b'x'; 50]).unwrap();
    }
    db.flush().unwrap();
    assert_eq!(db.get(b"k00000").unwrap(), Some(vec![b'x'; 50]));
    assert_eq!(db.get(b"k00250").unwrap(), Some(vec![b'x'; 50]));
    assert_eq!(db.get(b"k00499").unwrap(), Some(vec![b'x'; 50]));
    assert_eq!(db.get(b"k00500").unwrap(), None);
}

#[test]
fn auto_flush_produces_multiple_sstables() {
    let disk = MemDisk::new();
    let mut db = Db::open(disk.clone()).unwrap();
    db.set_flush_threshold(64);
    for i in 0..200u32 {
        let key = format!("key{i:04}");
        let value = format!("val{i:04}");
        db.put(key.as_bytes(), value.as_bytes()).unwrap();
    }
    db.flush().unwrap();

    let count = disk
        .list()
        .into_iter()
        .filter(|n| n.starts_with("sst-"))
        .count();
    assert!(count > 1);
    assert_eq!(db.get(b"key0000").unwrap(), Some(b"val0000".to_vec()));
    assert_eq!(db.get(b"key0100").unwrap(), Some(b"val0100".to_vec()));
    assert_eq!(db.get(b"key0199").unwrap(), Some(b"val0199".to_vec()));
}

#[test]
fn orphan_tmp_sstable_is_ignored_on_open() {
    let disk = MemDisk::new();
    {
        let mut db = Db::open(disk.clone()).unwrap();
        db.put(b"a", b"1").unwrap();
        db.sync().unwrap();
    }
    disk.create("sst-000099.tmp").unwrap();
    disk.append("sst-000099.tmp", b"garbage").unwrap();
    disk.sync("sst-000099.tmp").unwrap();

    let db = Db::open(disk.clone()).unwrap();
    assert_eq!(db.get(b"a").unwrap(), Some(b"1".to_vec()));
}
