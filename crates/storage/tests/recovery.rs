use storage::{Db, Disk, MemDisk};

#[test]
fn put_get_delete_with_overwrite() {
    let mut db = Db::open(MemDisk::new()).unwrap();
    db.put(b"a", b"1").unwrap();
    db.put(b"b", b"2").unwrap();
    assert_eq!(db.get(b"a"), Some(b"1".to_vec()));
    assert_eq!(db.get(b"b"), Some(b"2".to_vec()));

    db.put(b"a", b"3").unwrap();
    assert_eq!(db.get(b"a"), Some(b"3".to_vec()));

    db.delete(b"a").unwrap();
    assert_eq!(db.get(b"a"), None);
    assert_eq!(db.get(b"missing"), None);
}

#[test]
fn synced_writes_survive_crash_unsynced_are_lost() {
    let disk = MemDisk::new();
    {
        let mut db = Db::open(disk.clone()).unwrap();
        db.put(b"durable", b"yes").unwrap();
        db.sync().unwrap();
        db.put(b"lost", b"maybe").unwrap();
    }
    disk.crash();

    let db = Db::open(disk.clone()).unwrap();
    assert_eq!(db.get(b"durable"), Some(b"yes".to_vec()));
    assert_eq!(db.get(b"lost"), None);
}

#[test]
fn torn_tail_record_is_discarded() {
    let disk = MemDisk::new();
    {
        let mut db = Db::open(disk.clone()).unwrap();
        db.put(b"k1", b"v1").unwrap();
        db.put(b"k2", b"v2").unwrap();
        db.sync().unwrap();
    }
    let size = disk.size("wal").unwrap();
    disk.truncate("wal", size - 1);

    let db = Db::open(disk.clone()).unwrap();
    assert_eq!(db.get(b"k1"), Some(b"v1".to_vec()));
    assert_eq!(db.get(b"k2"), None);
}

#[test]
fn corruption_stops_replay_at_the_bad_record() {
    let disk = MemDisk::new();
    {
        let mut db = Db::open(disk.clone()).unwrap();
        db.put(b"k1", b"v1").unwrap();
        db.put(b"k2", b"v2").unwrap();
        db.sync().unwrap();
    }
    disk.corrupt("wal", 9, 0xff);

    let db = Db::open(disk.clone()).unwrap();
    assert_eq!(db.get(b"k1"), None);
    assert_eq!(db.get(b"k2"), None);
}
