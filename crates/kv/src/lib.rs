use consensus::{decode_command, KvCommand, StateMachine};
use storage::{Db, Disk};

pub struct LsmKv<D: Disk> {
    db: Db<D>,
}

impl<D: Disk> LsmKv<D> {
    pub fn new(db: Db<D>) -> Self {
        LsmKv { db }
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.db.get(key).ok().flatten()
    }
}

impl<D: Disk> StateMachine for LsmKv<D> {
    fn apply(&mut self, command: &[u8]) -> Vec<u8> {
        match decode_command(command) {
            Some(KvCommand::Put(key, value)) => {
                let _ = self.db.put(&key, &value);
            }
            Some(KvCommand::Delete(key)) => {
                let _ = self.db.delete(&key);
            }
            None => {}
        }
        Vec::new()
    }
}
