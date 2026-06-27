use consensus::{encode_put, KvStore, Message, Raft};
use sim::{secs, Simulator};

fn cluster(n: usize, seed: u64) -> Simulator<Raft<KvStore>> {
    let ids: Vec<usize> = (0..n).collect();
    let nodes: Vec<Raft<KvStore>> = ids
        .iter()
        .map(|&id| Raft::new(id, &ids, KvStore::new()))
        .collect();
    Simulator::new(seed, nodes)
}

fn leader(sim: &Simulator<Raft<KvStore>>) -> usize {
    (0..sim.nodes())
        .find(|&i| sim.process(i).is_leader())
        .expect("a leader should exist")
}

fn put(sim: &mut Simulator<Raft<KvStore>>, to: usize, id: u64, key: &[u8], value: &[u8]) {
    sim.inject(
        to,
        Message::ClientRequest {
            request_id: id,
            command: encode_put(key, value),
        },
    );
}

#[test]
fn log_compacts_after_threshold() {
    let mut sim = cluster(3, 1);
    sim.run_for(secs(2));
    let l = leader(&sim);
    for i in 0..200u32 {
        let key = format!("k{i:03}");
        put(&mut sim, l, u64::from(i + 1), key.as_bytes(), b"v");
    }
    sim.run_for(secs(5));

    for node in 0..sim.nodes() {
        assert!(
            sim.process(node).snapshot_index() > 0,
            "node {node} never compacted"
        );
        assert!(
            sim.process(node).log_entry_count() <= 100,
            "node {node} log is not bounded"
        );
        assert_eq!(
            sim.process(node).state_machine().get(b"k000"),
            Some(b"v".to_vec())
        );
        assert_eq!(
            sim.process(node).state_machine().get(b"k199"),
            Some(b"v".to_vec())
        );
    }
}

#[test]
fn lagging_follower_catches_up_via_snapshot() {
    let mut sim = cluster(3, 2);
    sim.run_for(secs(2));
    let l = leader(&sim);
    let victim = (0..3).find(|&i| i != l).unwrap();

    let ids: Vec<usize> = (0..3).collect();
    sim.partitions_mut().isolate(victim, &ids);
    for i in 0..200u32 {
        let key = format!("k{i:03}");
        put(&mut sim, l, u64::from(i + 1), key.as_bytes(), b"v");
    }
    sim.run_for(secs(5));
    assert!(sim.process(l).snapshot_index() > 0);

    sim.partitions_mut().heal_all();
    sim.run_for(secs(5));

    for i in [0u32, 100, 199] {
        let key = format!("k{i:03}");
        assert_eq!(
            sim.process(victim).state_machine().get(key.as_bytes()),
            Some(b"v".to_vec()),
            "victim missing {key}"
        );
    }
}
