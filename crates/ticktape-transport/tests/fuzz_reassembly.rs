//! Deterministic fuzz of the reliability core: for many seeds, subject the
//! Reassembler to loss, duplication, reordering, and repackaging, serving
//! gap-fills from an oracle — delivery must always be the exact in-order
//! sequence, every frame, no duplicates.

use ticktape_core::{Frame, FrameKind, Seq, Timestamp};
use ticktape_sim::Rng;
use ticktape_transport::wire::Packet;
use ticktape_transport::Reassembler;

const SESSION: u64 = 1;

fn frame(seq: u64) -> Frame {
    Frame::new(
        Seq(seq),
        Timestamp(seq * 3),
        1,
        FrameKind::Input,
        seq.to_le_bytes().to_vec(),
    )
}

#[test]
fn reassembly_is_exact_under_loss_dup_and_reorder() {
    for seed in 0..200u64 {
        let mut rng = Rng::new(seed);
        let total = 200 + rng.below(200);

        // Package the stream into packets of 1..=3 frames.
        let mut packets = Vec::new();
        let mut seq = 1u64;
        while seq <= total {
            let n = (1 + rng.below(3)).min(total - seq + 1);
            packets.push(Packet::Data {
                session: SESSION,
                frames: (seq..seq + n).map(frame).collect(),
            });
            seq += n;
        }

        // The hostile network: drop 20%, duplicate 20%, then shuffle.
        let mut wire: Vec<Packet> = Vec::new();
        for packet in packets {
            if rng.chance(1, 5) {
                continue; // lost on both channels
            }
            if rng.chance(1, 5) {
                wire.push(packet.clone()); // A/B duplicate
            }
            wire.push(packet);
        }
        for i in (1..wire.len()).rev() {
            let j = rng.below(i as u64 + 1) as usize;
            wire.swap(i, j);
        }
        // Trailing heartbeat so tail loss is provable.
        wire.push(Packet::Heartbeat {
            session: SESSION,
            next_seq: Seq(total + 1),
        });

        // Feed the wire; whenever a gap is reported, serve it from the
        // oracle (the full original stream) like a retransmitter would.
        let mut reassembler = Reassembler::new(Seq(1));
        let mut delivered = Vec::new();
        for packet in wire {
            reassembler.ingest(packet).unwrap();
            while let Some(frame) = reassembler.next_frame() {
                delivered.push(frame.seq.0);
            }
        }
        while let Some((from, count)) = reassembler.gap() {
            let fill = Packet::Data {
                session: SESSION,
                frames: (from.0..from.0 + count).map(frame).collect(),
            };
            reassembler.ingest(fill).unwrap();
            while let Some(frame) = reassembler.next_frame() {
                delivered.push(frame.seq.0);
            }
        }

        let expected: Vec<u64> = (1..=total).collect();
        assert_eq!(
            delivered, expected,
            "seed {seed}: stream not reconstructed exactly"
        );
    }
}
