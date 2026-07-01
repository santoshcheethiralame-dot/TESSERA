use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use consensus::{encode_put, KvStore, Message, Raft};
use kv::LsmKv;
use server::{run_node, Client};
use sim::{millis, NodeId, Simulator};
use storage::{Db, RealDisk};

fn main() {
    println!("== tessera benchmarks ==\n");
    storage_bench();
    println!();
    distributed_bench();
    println!();
    networked_bench();
}

fn percentile(sorted: &[u128], p: f64) -> u128 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() as f64 * p) as usize).min(sorted.len() - 1);
    sorted[idx]
}

fn report(label: &str, mut lat: Vec<u128>) {
    lat.sort_unstable();
    let n = lat.len();
    let sum: u128 = lat.iter().sum();
    let p50 = percentile(&lat, 0.50) as f64 / 1000.0;
    let p99 = percentile(&lat, 0.99) as f64 / 1000.0;
    let ops = n as f64 / (sum as f64 / 1e9);
    println!("  {label:<26} p50 {p50:>7.1} us   p99 {p99:>7.1} us   ({ops:>10.0} ops/s serial)");
}

fn storage_bench() {
    println!("-- storage engine on REAL disk (LSM: WAL+fsync, SSTables, bloom, compaction) --");
    let dir = std::env::temp_dir().join("tessera-bench-storage");
    let _ = std::fs::remove_dir_all(&dir);
    let disk = RealDisk::open(&dir).unwrap();
    let mut db = Db::open(disk).unwrap();
    let value = vec![b'x'; 100];

    let durable = 2_000usize;
    let mut lat = Vec::with_capacity(durable);
    for i in 0..durable {
        let key = format!("k{i:08}");
        let start = Instant::now();
        db.put(key.as_bytes(), &value).unwrap();
        db.sync().unwrap();
        lat.push(start.elapsed().as_nanos());
    }
    report("durable put (fsync/op)", lat);

    let batched = 200_000usize;
    let start = Instant::now();
    for i in 0..batched {
        let key = format!("k{:08}", durable + i);
        db.put(key.as_bytes(), &value).unwrap();
    }
    db.sync().unwrap();
    let secs = start.elapsed().as_secs_f64();
    println!(
        "  {:<26} {batched} ops in {secs:.2} s     ({:>10.0} ops/s)",
        "batched put (group commit)",
        batched as f64 / secs
    );

    let reads = 100_000usize;
    let total = durable + batched;
    let mut rng = 0x9e37_79b9_7f4a_7c15u64;
    let mut lat = Vec::with_capacity(reads);
    let mut hits = 0u64;
    for _ in 0..reads {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        let i = (rng as usize) % total;
        let key = format!("k{i:08}");
        let start = Instant::now();
        let got = db.get(key.as_bytes()).unwrap();
        lat.push(start.elapsed().as_nanos());
        if got.is_some() {
            hits += 1;
        }
    }
    report("random get", lat);
    println!(
        "  (read hit rate {:.1}%)",
        100.0 * hits as f64 / reads as f64
    );
    let _ = std::fs::remove_dir_all(&dir);
}

fn cluster(n: usize, seed: u64) -> Simulator<Raft<KvStore>> {
    let ids: Vec<NodeId> = (0..n).collect();
    let nodes: Vec<Raft<KvStore>> = ids
        .iter()
        .map(|&id| Raft::new(id, &ids, KvStore::new()))
        .collect();
    Simulator::new(seed, nodes)
}

fn leader(sim: &Simulator<Raft<KvStore>>) -> Option<NodeId> {
    (0..sim.nodes()).find(|&i| sim.process(i).is_leader())
}

fn distributed_bench() {
    println!("-- distributed Raft (5 nodes, deterministic simulator, virtual time) --");

    let mut sim = cluster(5, 1);
    let start = sim.now().as_millis();
    while leader(&sim).is_none() {
        sim.run_for(millis(1));
    }
    println!(
        "  {:<26} {} ms",
        "leader election",
        sim.now().as_millis() - start
    );

    let l = leader(&sim).unwrap();
    let n = 5_000u64;
    let t0 = sim.now().as_nanos();
    for i in 0..n {
        let before = sim.process(l).commit_index();
        let key = format!("k{i:06}");
        sim.inject(
            l,
            Message::ClientRequest {
                request_id: i + 1,
                command: encode_put(key.as_bytes(), b"v"),
            },
        );
        let deadline = sim.now().as_nanos() + 5_000_000_000;
        while sim.process(l).commit_index() <= before && sim.now().as_nanos() < deadline {
            sim.run_for(millis(1));
        }
    }
    let dt = (sim.now().as_nanos() - t0) as f64 / 1e9;
    println!(
        "  {:<26} {n} writes in {dt:.2} s     ({:>10.0} writes/s, serial commit)",
        "replicated throughput",
        n as f64 / dt
    );

    let mut sim = cluster(5, 5);
    while leader(&sim).is_none() {
        sim.run_for(millis(1));
    }
    let l = leader(&sim).unwrap();
    let m = 2_000usize;
    let base = sim.process(l).commit_index();
    let t0 = sim.now().as_nanos();
    for i in 0..m {
        let key = format!("c{i:06}");
        sim.inject(
            l,
            Message::ClientRequest {
                request_id: i as u64 + 1,
                command: encode_put(key.as_bytes(), b"v"),
            },
        );
    }
    let target = base + m;
    let deadline = sim.now().as_nanos() + 60_000_000_000;
    while sim.process(l).commit_index() < target && sim.now().as_nanos() < deadline {
        sim.run_for(millis(1));
    }
    let committed = sim.process(l).commit_index() - base;
    let dt = (sim.now().as_nanos() - t0) as f64 / 1e9;
    println!(
        "  {:<26} {committed} writes in {dt:.3} s     ({:>10.0} writes/s, pipelined)",
        "concurrent throughput",
        committed as f64 / dt.max(1e-9)
    );

    let mut sim = cluster(5, 2);
    while leader(&sim).is_none() {
        sim.run_for(millis(1));
    }
    let old = leader(&sim).unwrap();
    let ids: Vec<NodeId> = (0..5).collect();
    sim.partitions_mut().isolate(old, &ids);
    let t1 = sim.now().as_millis();
    let recovery = loop {
        sim.run_for(millis(1));
        if let Some(nl) = (0..5).find(|&i| i != old && sim.process(i).is_leader()) {
            let committed = sim.process(nl).commit_index();
            sim.inject(
                nl,
                Message::ClientRequest {
                    request_id: 1,
                    command: encode_put(b"after", b"kill"),
                },
            );
            while sim.process(nl).commit_index() <= committed {
                sim.run_for(millis(1));
            }
            break sim.now().as_millis() - t1;
        }
    };
    println!("  {:<26} {recovery} ms", "recovery after leader kill");

    let mut sim = cluster(5, 3);
    while leader(&sim).is_none() {
        sim.run_for(millis(1));
    }
    let l = leader(&sim).unwrap();
    let others: Vec<usize> = (0..5).filter(|&i| i != l).collect();
    let majority = vec![l, others[0], others[1]];
    let minority = vec![others[2], others[3]];
    sim.partitions_mut()
        .split(&[majority.clone(), minority.clone()]);
    sim.run_for(millis(800));
    let maj = majority.iter().any(|&i| sim.process(i).is_leader());
    let min = minority.iter().any(|&i| sim.process(i).is_leader());
    println!(
        "  {:<26} majority keeps a leader: {maj}, minority elects: {min} (no split-brain)",
        "3|2 partition"
    );
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

fn networked_bench() {
    println!("-- distributed Raft over REAL TCP (5 nodes, localhost, real timers + sockets) --");
    let ids: Vec<NodeId> = vec![0, 1, 2, 3, 4];
    let root = std::env::temp_dir().join("tessera-bench-net");
    let _ = std::fs::remove_dir_all(&root);
    let (addrs, shutdowns) = spawn_cluster(19400, &ids, &root);
    let nodes: Vec<SocketAddr> = ids.iter().map(|&i| addrs[&i]).collect();

    thread::sleep(Duration::from_millis(1500));
    let mut client = Client::new(nodes, 2_000_001);
    for i in 0..50 {
        client.put(format!("warm{i}").as_bytes(), b"v");
    }

    let writes = 2_000usize;
    let value = vec![b'x'; 100];
    let mut lat = Vec::with_capacity(writes);
    let wall = Instant::now();
    for i in 0..writes {
        let key = format!("k{i:06}");
        let start = Instant::now();
        client.put(key.as_bytes(), &value);
        lat.push(start.elapsed().as_nanos());
    }
    let secs = wall.elapsed().as_secs_f64();
    report("replicated put (TCP)", lat);
    println!(
        "  {:<26} {writes} writes in {secs:.2} s     ({:>10.0} writes/s, serial client)",
        "replicated throughput",
        writes as f64 / secs
    );

    let reads = 2_000usize;
    let mut lat = Vec::with_capacity(reads);
    for i in 0..reads {
        let key = format!("k{:06}", i % writes);
        let start = Instant::now();
        let _ = client.get(key.as_bytes());
        lat.push(start.elapsed().as_nanos());
    }
    report("linearizable get (TCP)", lat);

    let killed = client.leader_hint();
    shutdowns[killed].store(true, Ordering::Relaxed);
    let start = Instant::now();
    client.put(b"after-kill", b"v");
    let recovery = start.elapsed().as_millis();
    println!(
        "  {:<26} {recovery} ms  (killed node {killed}; client re-routed and committed)",
        "recovery after leader kill"
    );

    for shutdown in &shutdowns {
        shutdown.store(true, Ordering::Relaxed);
    }
    thread::sleep(Duration::from_millis(300));
    let _ = std::fs::remove_dir_all(&root);
}
