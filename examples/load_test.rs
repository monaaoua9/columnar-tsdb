// cargo run --release --example load_test [series] [points_per_series]
// bigger than the unit tests: ingest, kill-style reopen (no flush, wal replay), query, reopen
// after flush. prints sizes and times; not a pass/fail test
use std::time::Instant;

use columnar_tsdb::{Config, Engine, Point, Selector, SeriesKey};

fn dir_size(p: &std::path::Path) -> u64 {
    let mut n = 0;
    for e in std::fs::read_dir(p).unwrap().flatten() {
        let m = e.metadata().unwrap();
        n += if m.is_dir() {
            dir_size(&e.path())
        } else {
            m.len()
        };
    }
    n
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let series: usize = a.get(1).and_then(|s| s.parse().ok()).unwrap_or(50);
    let pts: i64 = a.get(2).and_then(|s| s.parse().ok()).unwrap_or(400_000);
    let dir = std::env::temp_dir().join("ctsdb-load");
    let _ = std::fs::remove_dir_all(&dir);
    let cfg = || {
        let mut c = Config::new(&dir);
        c.bucket_width = 24 * 3600 * 1000;
        c
    };

    let t = Instant::now();
    let e = Engine::open(cfg()).unwrap();
    for s in 0..series {
        let k = SeriesKey::new("load").label("id", format!("{s:04}"));
        let mut i = 0;
        while i < pts {
            let n = 5_000.min(pts - i);
            let b: Vec<Point> = (i..i + n)
                .map(|j| Point::new(j * 10_000, ((j * 31 + s as i64) % 1013) as f64 * 0.25))
                .collect();
            e.append_batch(&k, &b).unwrap();
            i += n;
        }
    }
    let total = series as i64 * pts;
    println!(
        "ingested {total} points in {:.1} s",
        t.elapsed().as_secs_f64()
    );

    // no flush: drop and reopen replays whatever the wal still holds
    drop(e);
    let t = Instant::now();
    let e = Engine::open(cfg()).unwrap();
    println!("reopen without flush: {:.2} s", t.elapsed().as_secs_f64());

    let t = Instant::now();
    let agg = e
        .aggregate_select(&Selector::new("load"), i64::MIN, i64::MAX)
        .unwrap()
        .unwrap();
    println!(
        "full aggregate: {} points in {:.0} ms",
        agg.count,
        t.elapsed().as_secs_f64() * 1e3
    );
    assert_eq!(agg.count as i64, total, "points went missing");

    // this one decodes every chunk of a series instead of reading stats
    let k = SeriesKey::new("load").label("id", "0000");
    let t = Instant::now();
    let got = e.query_range(&k, i64::MIN, i64::MAX).unwrap();
    println!(
        "decode one series: {} points in {:.0} ms",
        got.len(),
        t.elapsed().as_secs_f64() * 1e3
    );
    assert_eq!(got.len() as i64, pts);

    e.flush().unwrap();
    e.compact().unwrap();
    drop(e);
    println!(
        "on disk: {:.1} MB ({:.2} bytes/point)",
        dir_size(&dir) as f64 / 1e6,
        dir_size(&dir) as f64 / total as f64
    );

    let t = Instant::now();
    let e = Engine::open(cfg()).unwrap();
    println!(
        "reopen after flush+compact: {:.2} s",
        t.elapsed().as_secs_f64()
    );
    let agg = e
        .aggregate_select(&Selector::new("load"), i64::MIN, i64::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(agg.count as i64, total, "points went missing after compact");
    drop(e);
    let _ = std::fs::remove_dir_all(&dir);
}
