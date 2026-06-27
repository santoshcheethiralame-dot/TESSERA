use consensus::{encode_put, KvStore, Message, Raft};
use sim::{secs, Simulator};

fn build(processes: usize, members: &[usize], seed: u64) -> Simulator<Raft<KvStore>> {
    let nodes: Vec<Raft<KvStore>> = (0..processes)
        .map(|id| Raft::new(id, members, KvStore::new()))
        .collect();
    Simulator::new(seed, nodes)
}

fn leader_in(sim: &Simulator<Raft<KvStore>>, ids: &[usize]) -> usize {
    ids.iter()
        .copied()
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
fn adds_a_server_via_joint_consensus() {
    let mut sim = build(4, &[0, 1, 2], 1);
    sim.run_for(secs(2));
    let l = leader_in(&sim, &[0, 1, 2]);
    put(&mut sim, l, 1, b"k", b"v");
    sim.run_for(secs(1));

    sim.inject(
        l,
        Message::ChangeConfig {
            members: vec![0, 1, 2, 3],
        },
    );
    sim.run_for(secs(5));

    assert_eq!(sim.process(3).config_members(), vec![0, 1, 2, 3]);
    assert_eq!(
        sim.process(3).state_machine().get(b"k"),
        Some(b"v".to_vec())
    );

    let l2 = leader_in(&sim, &[0, 1, 2, 3]);
    put(&mut sim, l2, 2, b"k2", b"v2");
    sim.run_for(secs(2));
    for node in 0..4 {
        assert_eq!(
            sim.process(node).state_machine().get(b"k2"),
            Some(b"v2".to_vec())
        );
    }
}

#[test]
fn removes_a_server_via_joint_consensus() {
    let mut sim = build(5, &[0, 1, 2, 3, 4], 2);
    sim.run_for(secs(2));
    let l = leader_in(&sim, &[0, 1, 2, 3, 4]);
    put(&mut sim, l, 1, b"k", b"v");
    sim.run_for(secs(1));

    sim.inject(
        l,
        Message::ChangeConfig {
            members: vec![0, 1, 2, 3],
        },
    );
    sim.run_for(secs(8));

    for node in [0, 1, 2, 3] {
        assert_eq!(sim.process(node).config_members(), vec![0, 1, 2, 3]);
    }

    let l2 = leader_in(&sim, &[0, 1, 2, 3]);
    put(&mut sim, l2, 2, b"k2", b"v2");
    sim.run_for(secs(2));
    for node in [0, 1, 2, 3] {
        assert_eq!(
            sim.process(node).state_machine().get(b"k2"),
            Some(b"v2".to_vec())
        );
    }
}
