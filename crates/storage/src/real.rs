use std::cell::RefCell;
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::disk::Disk;

pub struct RealDisk {
    dir: PathBuf,
    files: RefCell<BTreeMap<String, File>>,
}

impl RealDisk {
    pub fn open(dir: impl AsRef<Path>) -> io::Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;
        Ok(RealDisk {
            dir,
            files: RefCell::new(BTreeMap::new()),
        })
    }

    fn ensure(&self, name: &str) -> io::Result<()> {
        if self.files.borrow().contains_key(name) {
            return Ok(());
        }
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(self.dir.join(name))?;
        self.files.borrow_mut().insert(name.to_string(), file);
        Ok(())
    }
}

impl Disk for RealDisk {
    fn create(&self, name: &str) -> io::Result<()> {
        self.files.borrow_mut().remove(name);
        File::create(self.dir.join(name))?;
        self.ensure(name)
    }

    fn append(&self, name: &str, data: &[u8]) -> io::Result<()> {
        self.ensure(name)?;
        self.files
            .borrow_mut()
            .get_mut(name)
            .unwrap()
            .write_all(data)
    }

    fn read_at(&self, name: &str, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        self.ensure(name)?;
        let mut files = self.files.borrow_mut();
        let file = files.get_mut(name).unwrap();
        file.seek(SeekFrom::Start(offset))?;
        let mut buf = vec![0u8; len];
        let mut got = 0;
        while got < len {
            let n = file.read(&mut buf[got..])?;
            if n == 0 {
                break;
            }
            got += n;
        }
        buf.truncate(got);
        Ok(buf)
    }

    fn sync(&self, name: &str) -> io::Result<()> {
        if let Some(file) = self.files.borrow().get(name) {
            file.sync_all()?;
        }
        Ok(())
    }

    fn size(&self, name: &str) -> io::Result<u64> {
        if let Some(file) = self.files.borrow().get(name) {
            return Ok(file.metadata()?.len());
        }
        Ok(fs::metadata(self.dir.join(name))
            .map(|m| m.len())
            .unwrap_or(0))
    }

    fn exists(&self, name: &str) -> bool {
        self.files.borrow().contains_key(name) || self.dir.join(name).exists()
    }

    fn list(&self) -> Vec<String> {
        let mut out = Vec::new();
        if let Ok(entries) = fs::read_dir(&self.dir) {
            for entry in entries.flatten() {
                if let Ok(name) = entry.file_name().into_string() {
                    out.push(name);
                }
            }
        }
        out.sort();
        out
    }

    fn remove(&self, name: &str) -> io::Result<()> {
        self.files.borrow_mut().remove(name);
        let _ = fs::remove_file(self.dir.join(name));
        Ok(())
    }

    fn rename(&self, from: &str, to: &str) -> io::Result<()> {
        self.files.borrow_mut().remove(from);
        self.files.borrow_mut().remove(to);
        fs::rename(self.dir.join(from), self.dir.join(to))
    }
}
