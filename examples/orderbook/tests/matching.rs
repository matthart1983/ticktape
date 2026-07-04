//! Matching-semantics tests, driven at the Service level (no journal).

use orderbook::{Cmd, Evt, OrderBook, Reject, Side};
use ticktape::{Ctx, OutBuf, Seq, Service, Timestamp};
use ticktape_sim::Invariants;

/// Apply one command and return its events, checking invariants after.
fn apply(book: &mut OrderBook, seq: &mut u64, cmd: Cmd) -> Vec<Evt> {
    *seq += 1;
    let mut out = OutBuf::new();
    let mut ctx = Ctx::new(Seq(*seq), Timestamp(*seq * 1_000), &mut out);
    book.apply(Seq(*seq), &cmd, &mut ctx);
    book.check().expect("invariants after every apply");
    out.drain()
}

fn submit(
    book: &mut OrderBook,
    seq: &mut u64,
    id: u64,
    side: Side,
    price: u32,
    qty: u32,
) -> Vec<Evt> {
    apply(
        book,
        seq,
        Cmd::Submit {
            id,
            side,
            price,
            qty,
        },
    )
}

#[test]
fn resting_then_partial_fill_at_maker_price() {
    let mut book = OrderBook::genesis(&());
    let mut seq = 0;
    submit(&mut book, &mut seq, 1, Side::Sell, 101, 50);
    // Taker bids 105 for 120: fills 50 at the MAKER's 101 (price improvement),
    // remainder rests at 105.
    let events = submit(&mut book, &mut seq, 2, Side::Buy, 105, 120);
    assert_eq!(
        events,
        vec![
            Evt::Accepted { id: 2 },
            Evt::Trade {
                taker: 2,
                maker: 1,
                price: 101,
                qty: 50
            },
        ]
    );
    assert_eq!(book.best_bid(), Some(105));
    assert_eq!(book.best_ask(), None);
    assert_eq!(book.resting_shares(), 70);
}

#[test]
fn fifo_time_priority_within_a_level() {
    let mut book = OrderBook::genesis(&());
    let mut seq = 0;
    submit(&mut book, &mut seq, 10, Side::Sell, 100, 30); // first in line
    submit(&mut book, &mut seq, 11, Side::Sell, 100, 30); // second
    let events = submit(&mut book, &mut seq, 12, Side::Buy, 100, 40);
    assert_eq!(
        events,
        vec![
            Evt::Accepted { id: 12 },
            Evt::Trade {
                taker: 12,
                maker: 10,
                price: 100,
                qty: 30
            },
            Evt::Trade {
                taker: 12,
                maker: 11,
                price: 100,
                qty: 10
            },
        ],
        "first-arrived maker must fill first"
    );
}

#[test]
fn walks_price_levels_best_first() {
    let mut book = OrderBook::genesis(&());
    let mut seq = 0;
    submit(&mut book, &mut seq, 1, Side::Sell, 102, 10);
    submit(&mut book, &mut seq, 2, Side::Sell, 100, 10);
    submit(&mut book, &mut seq, 3, Side::Sell, 101, 10);
    let events = submit(&mut book, &mut seq, 4, Side::Buy, 102, 30);
    let trades: Vec<_> = events
        .iter()
        .filter_map(|e| match e {
            Evt::Trade { maker, price, .. } => Some((*maker, *price)),
            _ => None,
        })
        .collect();
    assert_eq!(trades, vec![(2, 100), (3, 101), (1, 102)], "best ask first");
    assert_eq!(book.resting_orders(), 0);
}

#[test]
fn taker_never_trades_through_its_limit() {
    let mut book = OrderBook::genesis(&());
    let mut seq = 0;
    submit(&mut book, &mut seq, 1, Side::Sell, 100, 10);
    submit(&mut book, &mut seq, 2, Side::Sell, 104, 10);
    let events = submit(&mut book, &mut seq, 3, Side::Buy, 102, 30);
    assert_eq!(
        events,
        vec![
            Evt::Accepted { id: 3 },
            Evt::Trade {
                taker: 3,
                maker: 1,
                price: 100,
                qty: 10
            },
        ],
        "the 104 ask is past the taker's 102 limit"
    );
    // Remainder rests at 102; the 104 ask stays. Book not crossed.
    assert_eq!(book.best_bid(), Some(102));
    assert_eq!(book.best_ask(), Some(104));
}

#[test]
fn cancel_and_rejects() {
    let mut book = OrderBook::genesis(&());
    let mut seq = 0;
    submit(&mut book, &mut seq, 7, Side::Buy, 99, 25);

    let events = apply(&mut book, &mut seq, Cmd::Cancel { id: 7 });
    assert_eq!(
        events,
        vec![Evt::Canceled {
            id: 7,
            remaining: 25
        }]
    );

    let events = apply(&mut book, &mut seq, Cmd::Cancel { id: 7 });
    assert_eq!(
        events,
        vec![Evt::Rejected {
            id: 7,
            reason: Reject::UnknownOrder
        }]
    );

    let events = submit(&mut book, &mut seq, 8, Side::Buy, 99, 0);
    assert_eq!(
        events,
        vec![Evt::Rejected {
            id: 8,
            reason: Reject::ZeroQty
        }]
    );

    submit(&mut book, &mut seq, 9, Side::Buy, 99, 5);
    let events = submit(&mut book, &mut seq, 9, Side::Sell, 101, 5);
    assert_eq!(
        events,
        vec![Evt::Rejected {
            id: 9,
            reason: Reject::DuplicateId
        }],
        "id 9 is still resting"
    );
}

#[test]
fn snapshot_restore_roundtrip_is_exact() {
    use ticktape::{decode_all, encode_to_vec};
    let mut book = OrderBook::genesis(&());
    let mut seq = 0;
    for i in 0..40u64 {
        let side = if i % 2 == 0 { Side::Buy } else { Side::Sell };
        let price = 95 + (i % 11) as u32;
        submit(&mut book, &mut seq, i, side, price, 10 + (i % 7) as u32);
    }
    apply(&mut book, &mut seq, Cmd::Cancel { id: 4 });

    let snap_bytes = encode_to_vec(&book.snapshot());
    let restored = OrderBook::restore(decode_all(&snap_bytes).unwrap(), &());
    restored.check().expect("restored book invariants");
    assert_eq!(
        encode_to_vec(&restored.snapshot()),
        snap_bytes,
        "restore(snapshot()) must be byte-identical"
    );

    // And the restored book behaves identically going forward.
    let mut a = book;
    let mut b = restored;
    let (mut sa, mut sb) = (seq, seq);
    let ea = submit(&mut a, &mut sa, 1000, Side::Buy, 120, 500);
    let eb = submit(&mut b, &mut sb, 1000, Side::Buy, 120, 500);
    assert_eq!(ea, eb);
}
