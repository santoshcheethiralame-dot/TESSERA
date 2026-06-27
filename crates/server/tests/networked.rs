use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use consensus::Raft;
use kv::LsmKv;
use server::{run_node, Client};
use sim::NodeId;
use storage::{Db, RealDisk};

fn temp_root(tag: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!("tessera-net-{tag}"));
    let _ = std::fs::remove_dir_all(&path);
    std::fs::create_dir_all(&path).unwrap();
    path
}

fn spawn_cluster(
    base: u16,
    ids: &[NodeId],
    root: &Path,
) -> (BTreeMap<NodeId, SocketAddr>, Vec<Arc<AtomicBool>>) {
    let addrs: BTreeMap<NodeId, SocketAddr> = ids
        .iter()
        .map(|&i| (i, format!("127.0.0.1:{}", base + i as u16).parse().unwrap()))
        .collect();
    let mut shutdowns = Vec::new();
    for &id in ids {
        let addr = addrs[&id];
        let peers: BTreeMap<NodeId, SocketAddr> = addrs
            .iter()
            .filter(|&(&k, _)| k != id)
            .map(|(&k, &v)| (k, v))
            .collect();
        let cluster = ids.to_vec();
        let dir = root.join(format!("n{id}"));
        let shutdown = Arc::new(AtomicBool::new(false));
        shutdowns.push(shutdown.clone());
        thread::spawn(move || {
            let db = Db::open(RealDisk::open(&dir).unwrap()).unwrap();
            let raft = Raft::new(id, &cluster, LsmKv::new(db));
            run_node(id, addr, peers, raft, shutdown);
        });
    }
    (addrs, shutdowns)
}

#[test]
fn cluster_elects_replicates_and_recovers_over_tcp() {
    let ids = vec![0, 1, 2];
    let root = temp_root("recover");
    let (addrs, shutdowns) = spawn_cluster(19200, &ids, &root);
    let nodes: Vec<SocketAddr> = ids.iter().map(|&i| addrs[&i]).collect();

    thread::sleep(Duration::from_millis(1500));

    let mut client = Client::new(nodes, 1_000_001);
    client.put(b"alpha", b"one");
    client.put(b"beta", b"two");
    assert_eq!(client.get(b"alpha"), Some(b"one".to_vec()));
    assert_eq!(client.get(b"beta"), Some(b"two".to_vec()));

    shutdowns[0].store(true, Ordering::Relaxed);
    thread::sleep(Duration::from_millis(2000));

    client.put(b"gamma", b"three");
    assert_eq!(client.get(b"gamma"), Some(b"three".to_vec()));
    assert_eq!(client.get(b"alpha"), Some(b"one".to_vec()));
    assert_eq!(client.get(b"missing"), None);

    for shutdown in &shutdowns {
        shutdown.store(true, Ordering::Relaxed);
    }
    thread::sleep(Duration::from_millis(300));
    let _ = std::fs::remove_dir_all(&root);
}
