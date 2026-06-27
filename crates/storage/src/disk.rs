use std::cell::RefCell;
use std::collections::BTreeMap;
use std::io;
use std::rc::Rc;

pub trait Disk {
    fn create(&self, name: &str) -> io::Result<()>;
    fn append(&self, name: &str, data: &[u8]) -> io::Result<()>;
    fn read_at(&self, name: &str, offset: u64, len: usize) -> io::Result<Vec<u8>>;
    fn sync(&self, name: &str) -> io::Result<()>;
    fn size(&self, name: &str) -> io::Result<u64>;
    fn exists(&self, name: &str) -> bool;
    fn list(&self) -> Vec<String>;
    fn remove(&self, name: &str) -> io::Result<()>;
    fn rename(&self, from: &str, to: &str) -> io::Result<()>;
}

#[derive(Default)]
struct MemFile {
    committed: Vec<u8>,
    pending: Vec<u8>,
}

#[derive(Default)]
struct MemState {
    files: BTreeMap<String, MemFile>,
}

#[derive(Clone, Default)]
pub struct MemDisk {
    state: Rc<RefCell<MemState>>,
}

impl MemDisk {
    pub fn new() -> Self {
        MemDisk::default()
    }

    pub fn crash(&self) {
        for file in self.state.borrow_mut().files.values_mut() {
            file.pending.clear();
        }
    }

    pub fn truncate(&self, name: &str, len: u64) {
        if let Some(file) = self.state.borrow_mut().files.get_mut(name) {
            file.pending.clear();
            file.committed.truncate(len as usize);
        }
    }

    pub fn corrupt(&self, name: &str, offset: u64, value: u8) {
        if let Some(file) = self.state.borrow_mut().files.get_mut(name) {
            let at = offset as usize;
            if at < file.committed.len() {
                file.committed[at] = value;
            }
        }
    }
}

fn not_found(name: &str) -> io::Error {
    io::Error::new(io::ErrorKind::NotFound, name.to_string())
}

impl Disk for MemDisk {
    fn create(&self, name: &str) -> io::Result<()> {
        self.state
            .borrow_mut()
            .files
            .insert(name.to_string(), MemFile::default());
        Ok(())
    }

    fn append(&self, name: &str, data: &[u8]) -> io::Result<()> {
        let mut state = self.state.borrow_mut();
        let file = state.files.entry(name.to_string()).or_default();
        file.pending.extend_from_slice(data);
        Ok(())
    }

    fn read_at(&self, name: &str, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        let state = self.state.borrow();
        let file = state.files.get(name).ok_or_else(|| not_found(name))?;
        let mut bytes = file.committed.clone();
        bytes.extend_from_slice(&file.pending);
        let start = (offset as usize).min(bytes.len());
        let end = start.saturating_add(len).min(bytes.len());
        Ok(bytes[start..end].to_vec())
    }

    fn sync(&self, name: &str) -> io::Result<()> {
        let mut state = self.state.borrow_mut();
        let file = state.files.get_mut(name).ok_or_else(|| not_found(name))?;
        let pending = std::mem::take(&mut file.pending);
        file.committed.extend_from_slice(&pending);
        Ok(())
    }

    fn size(&self, name: &str) -> io::Result<u64> {
        let state = self.state.borrow();
        let file = state.files.get(name).ok_or_else(|| not_found(name))?;
        Ok((file.committed.len() + file.pending.len()) as u64)
    }

    fn exists(&self, name: &str) -> bool {
        self.state.borrow().files.contains_key(name)
    }

    fn list(&self) -> Vec<String> {
        self.state.borrow().files.keys().cloned().collect()
    }

    fn remove(&self, name: &str) -> io::Result<()> {
        self.state.borrow_mut().files.remove(name);
        Ok(())
    }

    fn rename(&self, from: &str, to: &str) -> io::Result<()> {
        let mut state = self.state.borrow_mut();
        let file = state.files.remove(from).ok_or_else(|| not_found(from))?;
        state.files.insert(to.to_string(), file);
        Ok(())
    }
}
