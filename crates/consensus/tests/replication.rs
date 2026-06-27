use consensus::{encode_delete, encode_put, KvStore, Message, Raft};
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

fn put(sim: &mut Simulator<Raft<KvStore>>, to: usize, key: &[u8], value: &[u8]) {
    sim.inject(
        to,
        Message::ClientRequest {
            command: encode_put(key, value),
        },
    );
}

#[test]
fn replicates_a_write_to_all_replicas() {
    let mut sim = cluster(5, 1);
    sim.run_for(secs(2));
    let l = leader(&sim);
    put(&mut sim, l, b"k", b"v");
    sim.run_for(secs(2));

    for i in 0..sim.nodes() {
        assert_eq!(
            sim.process(i).state_machine().get(b"k"),
            Some(b"v".to_vec())
        );
    }
}

#[test]
fn write_commits_with_one_follower_partitioned() {
    let mut sim = cluster(5, 2);
    sim.run_for(secs(2));
    let l = leader(&sim);
    let victim = (0..5).find(|&i| i != l).unwrap();

    let ids: Vec<usize> = (0..5).collect();
    sim.partitions_mut().isolate(victim, &ids);
    put(&mut sim, l, b"k", b"v");
    sim.run_for(secs(2));

    for i in (0..5).filter(|&i| i != victim) {
        assert_eq!(
            sim.process(i).state_machine().get(b"k"),
            Some(b"v".to_vec())
        );
    }

    sim.partitions_mut().heal_all();
    sim.run_for(secs(2));
    assert_eq!(
        sim.process(victim).state_machine().get(b"k"),
        Some(b"v".to_vec())
    );
}

#[test]
fn log_converges_after_leader_change() {
    let mut sim = cluster(5, 3);
    sim.run_for(secs(2));
    let l1 = leader(&sim);
    put(&mut sim, l1, b"a", b"1");
    sim.run_for(secs(2));

    let ids: Vec<usize> = (0..5).collect();
    sim.partitions_mut().isolate(l1, &ids);
    sim.run_for(secs(3));

    let l2 = (0..5)
        .find(|&i| i != l1 && sim.process(i).is_leader())
        .unwrap();
    put(&mut sim, l2, b"b", b"2");
    sim.run_for(secs(2));

    for i in (0..5).filter(|&i| i != l1) {
        assert_eq!(
            sim.process(i).state_machine().get(b"a"),
            Some(b"1".to_vec())
        );
        assert_eq!(
            sim.process(i).state_machine().get(b"b"),
            Some(b"2".to_vec())
        );
    }
}

#[test]
fn many_writes_replicate_in_order() {
    let mut sim = cluster(3, 4);
    sim.run_for(secs(2));
    let l = leader(&sim);
    for i in 0..50u32 {
        let key = format!("k{i:03}");
        put(&mut sim, l, key.as_bytes(), b"v");
    }
    sim.run_for(secs(3));

    for node in 0..3 {
        for i in 0..50u32 {
            let key = format!("k{i:03}");
            assert_eq!(
                sim.process(node).state_machine().get(key.as_bytes()),
                Some(b"v".to_vec())
            );
        }
    }
}

#[test]
fn replicates_a_delete() {
    let mut sim = cluster(3, 5);
    sim.run_for(secs(2));
    let l = leader(&sim);
    put(&mut sim, l, b"k", b"v");
    sim.run_for(secs(1));

    sim.inject(
        l,
        Message::ClientRequest {
            command: encode_delete(b"k"),
        },
    );
    sim.run_for(secs(2));

    for i in 0..3 {
        assert_eq!(sim.process(i).state_machine().get(b"k"), None);
    }
}
