pub trait RaftStore {
    fn save(&mut self, bytes: &[u8]);
    fn load(&self) -> Option<Vec<u8>>;
}

#[derive(Default)]
pub struct MemStore {
    data: Option<Vec<u8>>,
}

impl MemStore {
    pub fn new() -> Self {
        MemStore { data: None }
    }
}

impl RaftStore for MemStore {
    fn save(&mut self, bytes: &[u8]) {
        self.data = Some(bytes.to_vec());
    }

    fn load(&self) -> Option<Vec<u8>> {
        self.data.clone()
    }
}
