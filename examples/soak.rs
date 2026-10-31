// cargo run --release --example soak [seconds] [series]
// a collector's life squeezed into minutes: every "tick" writes one point per series, 10 s of
// data time per tick, so a few minutes of wall time is days of data. A reader queries all the
// while. Every 20 s the engine is dropped without a flush and reopened; the point count must
// match what was written. Retention is off so the count is exact.
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use columnar_tsdb::query::{self, Ctx};
use columnar_tsdb::{Config, Engine, Point, Selector, SeriesKey};

const TICK_MS: i64 = 10_000;

fn cfg(dir: &std::path::Path) -> Config {
    let mut c = Config::new(dir);
    c.bucket_width = 24 * 3600 * 1000;
    c
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let secs: u64 = a.get(1).and_then(|s| s.parse().ok()).unwrap_or(180);
    let n_series: usize = a.get(2).and_then(|s| s.parse().ok()).unwrap_or(200);
    let dir = std::env::temp_dir().join("ctsdb-soak");
    let _ = std::fs::remove_dir_all(&dir);

    let keys: Vec<SeriesKey> = (0..n_series)
        .map(|i| {
            SeriesKey::new("sys")
                .label("host", format!("h{:03}", i / 10))
                .label("m", ["cpu", "mem", "disk", "net", "load"][i % 5])
                .label("i", i.to_string())
        })
        .collect();

    let mut engine = Engine::open(cfg(&dir)).unwrap();
    let written = Arc::new(AtomicU64::new(0));
    let tick = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let queries = Arc::new(AtomicU64::new(0));
    let qerrors = Arc::new(AtomicU64::new(0));

    let t0 = Instant::now();
    let mut reopen_at = Duration::from_secs(20);
    let mut reopens = 0;
    let mut worst_open = 0.0f64;

    while t0.elapsed() < Duration::from_secs(secs) {
        // writer + reader run until the next reopen
        stop.store(false, Ordering::SeqCst);
        let w = {
            let (e, keys, written, tick, stop) = (
                engine.clone(),
                keys.clone(),
                written.clone(),
                tick.clone(),
                stop.clone(),
            );
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    let t = tick.load(Ordering::Relaxed) as i64;
                    for (i, k) in keys.iter().enumerate() {
                        let v = ((t * 7 + i as i64 * 13) % 1000) as f64 * 0.1;
                        e.append_batch(k, &[Point::new(t * TICK_MS, v)]).unwrap();
                    }
                    written.fetch_add(keys.len() as u64, Ordering::Relaxed);
                    tick.fetch_add(1, Ordering::Relaxed);
                }
            })
        };
        let r = {
            let (e, tick, stop, queries, qerrors) = (
                engine.clone(),
                tick.clone(),
                stop.clone(),
                queries.clone(),
                qerrors.clone(),
            );
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    let now = tick.load(Ordering::Relaxed) as i64 * TICK_MS;
                    let ctx = Ctx {
                        start: now - 3600 * 1000,
                        end: now,
                        step: Some(60_000),
                        per_sec: 1000,
                    };
                    let q = r#"avg by (host) (rate(sys{m="cpu"}[5m]))"#;
                    match query::run(&e, q, &ctx) {
                        Ok(_) => queries.fetch_add(1, Ordering::Relaxed),
                        Err(_) => qerrors.fetch_add(1, Ordering::Relaxed),
                    };
                    std::thread::sleep(Duration::from_millis(200));
                }
            })
        };

        while t0.elapsed() < reopen_at.min(Duration::from_secs(secs)) {
            std::thread::sleep(Duration::from_millis(500));
        }
        stop.store(true, Ordering::SeqCst);
        w.join().unwrap();
        r.join().unwrap();

        let st = engine.stats();
        let days = tick.load(Ordering::Relaxed) as f64 * TICK_MS as f64 / 86_400_000.0;
        println!(
            "t={:>4.0}s  {:>5.1} days of data  {:>9} pts  wal {:>6.1} MB  chunks {:>5}  frozen {}  ooo {}  err {:?}",
            t0.elapsed().as_secs_f64(),
            days,
            written.load(Ordering::Relaxed),
            st.wal_bytes as f64 / 1e6,
            st.sealed_chunks,
            st.frozen_chunks,
            st.ooo_points,
            st.last_maintenance_error
        );

        // hard-ish stop: no flush, the wal has to carry whatever is not sealed yet
        drop(engine);
        let o = Instant::now();
        engine = Engine::open(cfg(&dir)).unwrap();
        let open_s = o.elapsed().as_secs_f64();
        worst_open = worst_open.max(open_s);
        reopens += 1;

        let agg = engine
            .aggregate_select(&Selector::new("sys"), i64::MIN, i64::MAX)
            .unwrap()
            .unwrap();
        assert_eq!(
            agg.count,
            written.load(Ordering::Relaxed),
            "point count changed across reopen #{reopens}"
        );
        println!("          reopen #{reopens}: {open_s:.2} s, count ok");
        reopen_at += Duration::from_secs(20);
    }

    println!(
        "done: {} points, {} queries ({} failed), {} reopens, slowest open {:.2} s",
        written.load(Ordering::Relaxed),
        queries.load(Ordering::Relaxed),
        qerrors.load(Ordering::Relaxed),
        reopens,
        worst_open
    );
    assert_eq!(qerrors.load(Ordering::Relaxed), 0, "queries failed");
    drop(engine);
    let _ = std::fs::remove_dir_all(&dir);
}
