use std::path::PathBuf;

use consensus::{encode_put, KvStore, Message, Raft, RaftStore};
use server::DiskStore;
use sim::{millis, secs, Simulator};

fn temp_dir(tag: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!("tessera-diskstore-{tag}"));
    let _ = std::fs::remove_dir_all(&path);
    path
}

#[test]
fn raft_state_survives_process_restart() {
    let dir = temp_dir("restart");

    {
        let store = DiskStore::open(&dir).unwrap();
        let raft = Raft::with_store(0, &[0], KvStore::new(), Box::new(store));
        let mut sim = Simulator::new(1, vec![raft]);
        while !sim.process(0).is_leader() {
            sim.run_for(millis(1));
        }
        let before = sim.process(0).commit_index();
        sim.inject(
            0,
            Message::ClientRequest {
                request_id: 1,
                command: encode_put(b"key", b"durable"),
            },
        );
        let deadline = sim.now() + secs(5);
        while sim.process(0).commit_index() <= before && sim.now() < deadline {
            sim.run_for(millis(1));
        }
        assert!(
            sim.process(0).commit_index() > before,
            "write did not commit"
        );
    }

    {
        let store = DiskStore::open(&dir).unwrap();
        assert!(store.load().is_some(), "nothing was persisted to disk");
        let raft = Raft::with_store(0, &[0], KvStore::new(), Box::new(store));
        let mut sim = Simulator::new(2, vec![raft]);
        while !sim.process(0).is_leader() {
            sim.run_for(millis(1));
        }
        sim.inject(
            0,
            Message::ClientRequest {
                request_id: 2,
                command: encode_put(b"trigger", b"1"),
            },
        );
        sim.run_for(secs(1));
        assert_eq!(
            sim.process(0).state_machine().get(b"key"),
            Some(b"durable".to_vec()),
            "committed write did not survive restart from disk"
        );
    }

    let _ = std::fs::remove_dir_all(&dir);
}
