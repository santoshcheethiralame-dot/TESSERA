use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use consensus::Raft;
use kv::LsmKv;
use server::{run_node, DiskStore};
use sim::NodeId;
use storage::{Db, RealDisk};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 4 {
        eprintln!("usage: tessera-server <id> <listen_addr> <data_dir> [peer_id=addr ...]");
        std::process::exit(2);
    }

    let id: NodeId = args[1].parse().expect("node id");
    let addr: SocketAddr = args[2].parse().expect("listen address");
    let data_dir = args[3].clone();

    let mut peers = BTreeMap::new();
    let mut cluster = vec![id];
    for spec in &args[4..] {
        let (pid, paddr) = spec.split_once('=').expect("peer must be id=addr");
        let pid: NodeId = pid.parse().expect("peer id");
        let paddr: SocketAddr = paddr.parse().expect("peer address");
        peers.insert(pid, paddr);
        cluster.push(pid);
    }
    cluster.sort_unstable();

    let disk = RealDisk::open(&data_dir).expect("open data directory");
    let db = Db::open(disk).expect("open database");
    let store = DiskStore::open(&data_dir).expect("open raft store");
    let raft = Raft::with_store(id, &cluster, LsmKv::new(db), Box::new(store));

    println!("tessera node {id} on {addr}, cluster {cluster:?}, data {data_dir}");
    run_node(id, addr, peers, raft, Arc::new(AtomicBool::new(false)));
}
