// Test helper for tests/crash.rs, not a user-facing tool.
//   crash_writer write  <dir> <first_batch> [del]  append batches forever, print "ack N" after each
//   crash_writer verify <dir> <acked> [del]        check the db against the model for N acked batches
//
// With `del`, every 7th step also deletes a range (derived from the step number) before it is
// acked. Some of those ranges reach past the newest timestamp, so the points of the following
// batches are written in-order *inside* an already deleted range.
//
// Batch b = 50 in-order points plus 3 late overwrites of earlier timestamps, all derived from b,
// so the expected contents after any number of batches can be recomputed from scratch.
use std::collections::BTreeMap;
use std::time::Duration;

use columnar_tsdb::{Config, Engine, Point, SeriesKey};

const PER_BATCH: u64 = 50;

fn key() -> SeriesKey {
    SeriesKey::new("crash").label("t", "1")
}

fn config(dir: &str) -> Config {
    let mut c = Config::new(dir);
    // small everything, so sealing / merging / checkpoints are always in flight
    c.bucket_width = 2_000;
    c.max_points_per_chunk = 200;
    c.max_ooo_points = 20;
    c.maintenance_interval = Duration::from_millis(5);
    c
}

fn batch(b: u64) -> Vec<Point> {
    let mut v = Vec::new();
    for i in 0..PER_BATCH {
        let idx = b * PER_BATCH + i;
        v.push(Point::new(idx as i64 * 10, (idx % 1000) as f64 * 0.25));
    }
    let total = b * PER_BATCH + PER_BATCH;
    for k in 0..3 {
        let j = (b * 7919 + k * 104_729) % total;
        v.push(Point::new(j as i64 * 10, -(b as f64) - k as f64 - 1.0));
    }
    v
}

// the delete that follows batch b, if any: [lo, hi)
fn delete_of(b: u64) -> Option<(i64, i64)> {
    if (b + 1) % 7 != 0 {
        return None;
    }
    let newest = ((b + 1) * PER_BATCH) as i64 * 10;
    let lo = (((b * 37) % (b * PER_BATCH + PER_BATCH)) as i64) * 10;
    let hi = if b % 5 == 4 {
        newest + 5_000 // reaches past everything written so far
    } else {
        lo + 10 * (30 + (b % 40) as i64)
    };
    Some((lo.min(newest - 200), hi))
}

fn model(batches: u64, with_deletes: bool) -> BTreeMap<i64, f64> {
    let mut m = BTreeMap::new();
    for b in 0..batches {
        for p in batch(b) {
            m.insert(p.ts, p.value);
        }
        if let Some((lo, hi)) = with_deletes.then(|| delete_of(b)).flatten() {
            m.retain(|&t, _| t < lo || t >= hi);
        }
    }
    m
}

#[allow(clippy::manual_is_multiple_of)] // is_multiple_of needs a newer rustc than our MSRV
fn write(dir: &str, first: u64, with_deletes: bool) {
    let e = Engine::open(config(dir)).unwrap();
    let k = key();
    let mut b = first;
    loop {
        e.append_batch(&k, &batch(b)).unwrap();
        if let Some((lo, hi)) = with_deletes.then(|| delete_of(b)).flatten() {
            e.delete_range(&k, lo, hi).unwrap();
        }
        println!("ack {}", b + 1);
        if (b + 1) % 40 == 0 {
            e.flush().unwrap();
        }
        if (b + 1) % 25 == 0 {
            e.compact().unwrap();
        }
        b += 1;
    }
}

// A batch is validated atomically but not made durable atomically: after a crash the batch that
// was in flight may be missing, complete, or a prefix, and its delete (if it has one) may have
// reached only some chunks. So every point that is in both the acked state and the acked+1 state
// must be present, and every value must come from one of the two states.
fn verify(dir: &str, acked: u64, with_deletes: bool) {
    let e = Engine::open(config(dir)).unwrap();
    let got = e.query_range(&key(), i64::MIN, i64::MAX).unwrap();
    let (a, n) = (model(acked, with_deletes), model(acked + 1, with_deletes));
    // batch applied, its delete not yet
    let mut mid = model(acked, with_deletes);
    for p in batch(acked) {
        mid.insert(p.ts, p.value);
    }

    let present: BTreeMap<i64, f64> = got.iter().map(|p| (p.ts, p.value)).collect();
    assert_eq!(present.len(), got.len(), "duplicate timestamps in result");
    assert!(
        got.windows(2).all(|w| w[0].ts < w[1].ts),
        "result not ascending"
    );
    for (ts, v) in &a {
        match present.get(ts) {
            // the in-flight step's delete may already have removed it
            None if !n.contains_key(ts) => {}
            None => panic!("acked point ts={ts} lost (acked batches: {acked})"),
            Some(g) if g == v || n.get(ts) == Some(g) || mid.get(ts) == Some(g) => {}
            Some(g) => panic!(
                "ts={ts}: got {g}, acked state has {v}, next has {:?}",
                n.get(ts)
            ),
        }
    }
    for (ts, g) in &present {
        assert!(
            a.get(ts) == Some(g) || n.get(ts) == Some(g) || mid.get(ts) == Some(g),
            "unexpected point ts={ts} value={g}"
        );
    }

    let st = e.stats();
    assert!(
        st.last_maintenance_error.is_none(),
        "{:?}",
        st.last_maintenance_error
    );
    println!("ok {}", got.len());
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("write") => write(&args[2], args[3].parse().unwrap(), args.get(4).is_some()),
        Some("verify") => verify(&args[2], args[3].parse().unwrap(), args.get(4).is_some()),
        _ => {
            eprintln!("usage: crash_writer write|verify <dir> <n> [del]");
            std::process::exit(2);
        }
    }
}
