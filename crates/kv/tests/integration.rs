use consensus::{encode_delete, encode_put, Message, Raft};
use kv::LsmKv;
use sim::{secs, Simulator};
use storage::{Db, MemDisk};

type Cluster = Simulator<Raft<LsmKv<MemDisk>>>;

fn cluster(n: usize, seed: u64) -> Cluster {
    let ids: Vec<usize> = (0..n).collect();
    let nodes: Vec<Raft<LsmKv<MemDisk>>> = ids
        .iter()
        .map(|&id| {
            let db = Db::open(MemDisk::new()).unwrap();
            Raft::new(id, &ids, LsmKv::new(db))
        })
        .collect();
    Simulator::new(seed, nodes)
}

fn leader(sim: &Cluster) -> usize {
    (0..sim.nodes())
        .find(|&i| sim.process(i).is_leader())
        .expect("a leader should exist")
}

#[test]
fn replicated_writes_land_in_the_lsm_on_every_node() {
    let mut sim = cluster(5, 1);
    sim.run_for(secs(2));
    let l = leader(&sim);

    for i in 0..20u32 {
        let key = format!("k{i:03}");
        sim.inject(
            l,
            Message::ClientRequest {
                command: encode_put(key.as_bytes(), b"v"),
            },
        );
    }
    sim.run_for(secs(3));

    for node in 0..sim.nodes() {
        for i in 0..20u32 {
            let key = format!("k{i:03}");
            assert_eq!(
                sim.process(node).state_machine().get(key.as_bytes()),
                Some(b"v".to_vec())
            );
        }
    }
}

#[test]
fn replicated_delete_lands_in_the_lsm() {
    let mut sim = cluster(3, 2);
    sim.run_for(secs(2));
    let l = leader(&sim);

    sim.inject(
        l,
        Message::ClientRequest {
            command: encode_put(b"k", b"v"),
        },
    );
    sim.run_for(secs(1));
    sim.inject(
        l,
        Message::ClientRequest {
            command: encode_delete(b"k"),
        },
    );
    sim.run_for(secs(2));

    for node in 0..sim.nodes() {
        assert_eq!(sim.process(node).state_machine().get(b"k"), None);
    }
}
