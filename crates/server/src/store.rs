use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use consensus::RaftStore;

pub struct DiskStore {
    path: PathBuf,
    tmp: PathBuf,
    cached: Option<Vec<u8>>,
}

impl DiskStore {
    pub fn open(dir: impl AsRef<Path>) -> io::Result<Self> {
        let dir = dir.as_ref();
        fs::create_dir_all(dir)?;
        let path = dir.join("raft.state");
        let tmp = dir.join("raft.state.tmp");
        let cached = fs::read(&path).ok();
        Ok(DiskStore { path, tmp, cached })
    }
}

impl RaftStore for DiskStore {
    fn save(&mut self, bytes: &[u8]) {
        if self.cached.as_deref() == Some(bytes) {
            return;
        }
        let mut file = File::create(&self.tmp).expect("create raft state file");
        file.write_all(bytes).expect("write raft state");
        file.sync_all().expect("fsync raft state");
        fs::rename(&self.tmp, &self.path).expect("commit raft state");
        self.cached = Some(bytes.to_vec());
    }

    fn load(&self) -> Option<Vec<u8>> {
        self.cached.clone()
    }
}
