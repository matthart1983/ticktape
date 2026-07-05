//! The M4 deliverable, as a readable script: kill the leader mid-stream,
//! elect a standby, and keep serving — with continuity across the fence.

use ticktape_cluster::{Acceptor, Election, ElectionOutcome, EpochChange};
use ticktape_core::{encode_to_vec, Frame, FrameKind, Seq, Service, Timestamp};
use ticktape_sim::demo::{gen_transfer, Bank};
use ticktape_sim::{Invariants, Rng};
use ticktape_transport::Replica;

fn input_frame(seq: u64, rng: &mut Rng) -> Frame {
    Frame::new(
        Seq(seq),
        Timestamp(seq * 1_000),
        1,
        FrameKind::Input,
        encode_to_vec(&gen_transfer(rng)),
    )
}

#[test]
fn kill_the_leader_and_continue() {
    let mut rng = Rng::new(1);

    // A three-node cluster: node 0 leads epoch 1; 1 and 2 are standbys
    // (each co-hosting an acceptor).
    let mut acceptors: Vec<Acceptor> = (0..3).map(|_| Acceptor::new()).collect();
    let mut standby_1: Replica<Bank> = Replica::new(&());
    let mut standby_2: Replica<Bank> = Replica::new(&());

    // Epoch 1: the leader sequences 100 inputs. Standby 1 receives all of
    // them; standby 2 lags at 60 (slow link).
    let mut stream: Vec<Frame> = (1..=100).map(|seq| input_frame(seq, &mut rng)).collect();
    for frame in &stream {
        standby_1.apply(frame).unwrap();
        acceptors[1].observe_seq(frame.seq);
        if frame.seq.0 <= 60 {
            standby_2.apply(frame).unwrap();
            acceptors[2].observe_seq(frame.seq);
        }
    }

    // 💀 The leader dies. Standby 1 suspects it and bids for epoch 2.
    let mut election = Election::new(2, 3);
    for id in [1usize, 2] {
        // Acceptor 0 is dead and never replies; 1 and 2 form the majority.
        let reply = acceptors[id].handle(election.request());
        election.on_reply(id as u32, reply);
    }
    let ElectionOutcome::Won { epoch, sync_to } = election.outcome() else {
        panic!("standby 1 must win with a 2/3 majority");
    };
    assert_eq!(epoch, 2);
    assert_eq!(
        sync_to,
        Seq(100),
        "grants carry high-waters; the winner knows the majority's best"
    );
    // Standby 1 already holds everything up to sync_to: it can lead with
    // zero loss (had standby 2 won, Tier 2 would sync it from standby 1
    // first). It seals the new epoch with the fence frame.
    assert_eq!(standby_1.seq(), sync_to);
    let fence = EpochChange {
        epoch,
        first_seq: sync_to.next(),
        schema_version: 0,
    }
    .to_frame(Timestamp(200_000), 1);
    stream.push(fence.clone());
    standby_1.apply(&fence).unwrap();

    // A deposed twin of the old leader trying to sequence at stale seqs is
    // harmless: the fence means replicas reject epoch-1 traffic from here.
    // (The transport carries the epoch; the cluster sim fuzzes this — here
    // we just assert the fence parses back to what consumers enforce.)
    let parsed = EpochChange::from_frame(&fence).unwrap();
    assert_eq!(parsed.epoch, 2);
    assert_eq!(parsed.first_seq, Seq(101));

    // Epoch 2: the promoted leader sequences 100 more inputs; the lagging
    // standby 2 catches up over the whole history (retransmitter in
    // production) and lands bit-identical.
    for seq in 102..=201 {
        let frame = input_frame(seq, &mut rng);
        stream.push(frame.clone());
        standby_1.apply(&frame).unwrap();
    }
    for frame in &stream[60..] {
        standby_2.apply(frame).unwrap();
    }

    assert_eq!(standby_1.seq(), Seq(201));
    assert_eq!(standby_2.seq(), Seq(201));
    assert_eq!(
        encode_to_vec(&standby_1.service().snapshot()),
        encode_to_vec(&standby_2.service().snapshot()),
        "continuity: every survivor computes the identical state"
    );
    standby_1.service().check().unwrap();
}
