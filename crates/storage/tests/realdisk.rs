use storage::{Db, RealDisk};

#[test]
fn realdisk_put_flush_overwrite_reopen() {
    let dir = std::env::temp_dir().join("tessera-test-realdisk");
    let _ = std::fs::remove_dir_all(&dir);

    {
        let mut db = Db::open(RealDisk::open(&dir).unwrap()).unwrap();
        for i in 0..500u32 {
            db.put(format!("k{i}").as_bytes(), format!("v{i}").as_bytes())
                .unwrap();
        }
        db.flush().unwrap();
        for i in 0..500u32 {
            db.put(format!("k{i}").as_bytes(), format!("w{i}").as_bytes())
                .unwrap();
        }
        db.sync().unwrap();
    }

    let db = Db::open(RealDisk::open(&dir).unwrap()).unwrap();
    for i in 0..500u32 {
        assert_eq!(
            db.get(format!("k{i}").as_bytes()).unwrap(),
            Some(format!("w{i}").into_bytes())
        );
    }
    let _ = std::fs::remove_dir_all(&dir);
}
