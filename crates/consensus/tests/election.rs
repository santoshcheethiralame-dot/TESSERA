use consensus::Raft;
use sim::{secs, Simulator};

fn cluster(n: usize, seed: u64) -> Simulator<Raft> {
    let ids: Vec<usize> = (0..n).collect();
    let nodes: Vec<Raft> = ids.iter().map(|&id| Raft::new(id, &ids)).collect();
    Simulator::new(seed, nodes)
}

fn leaders(sim: &Simulator<Raft>) -> Vec<usize> {
    (0..sim.nodes())
        .filter(|&i| sim.process(i).is_leader())
        .collect()
}

#[test]
fn elects_a_single_leader() {
    let mut sim = cluster(5, 1);
    sim.run_for(secs(5));

    let ls = leaders(&sim);
    assert_eq!(ls.len(), 1);
    let leader = ls[0];
    let term = sim.process(leader).current_term();
    for i in 0..sim.nodes() {
        assert_eq!(sim.process(i).current_term(), term);
        assert_eq!(sim.process(i).leader(), Some(leader));
    }
}

#[test]
fn re_elects_after_leader_is_isolated() {
    let mut sim = cluster(5, 2);
    sim.run_for(secs(5));
    let old = leaders(&sim);
    assert_eq!(old.len(), 1);
    let old_leader = old[0];
    let old_term = sim.process(old_leader).current_term();

    let ids: Vec<usize> = (0..5).collect();
    sim.partitions_mut().isolate(old_leader, &ids);
    sim.run_for(secs(5));

    let new_leaders: Vec<usize> = (0..5)
        .filter(|&i| i != old_leader && sim.process(i).is_leader())
        .collect();
    assert_eq!(new_leaders.len(), 1);
    assert!(sim.process(new_leaders[0]).current_term() > old_term);
}

#[test]
fn minority_partition_cannot_elect() {
    let mut sim = cluster(5, 3);
    sim.run_for(secs(5));

    let leader = leaders(&sim)[0];
    let others: Vec<usize> = (0..5).filter(|&i| i != leader).collect();
    let majority = vec![leader, others[0], others[1]];
    let minority = vec![others[2], others[3]];
    sim.partitions_mut()
        .split(&[majority.clone(), minority.clone()]);
    sim.run_for(secs(5));

    let majority_leaders = majority
        .iter()
        .filter(|&&i| sim.process(i).is_leader())
        .count();
    let minority_leaders = minority
        .iter()
        .filter(|&&i| sim.process(i).is_leader())
        .count();
    assert_eq!(majority_leaders, 1);
    assert_eq!(minority_leaders, 0);
}

#[test]
fn no_leader_without_quorum() {
    let mut sim = cluster(3, 4);
    let ids: Vec<usize> = (0..3).collect();
    for &node in &ids {
        sim.partitions_mut().isolate(node, &ids);
    }
    sim.run_for(secs(5));
    assert_eq!(leaders(&sim).len(), 0);
}
