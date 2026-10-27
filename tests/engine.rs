use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use columnar_tsdb::{Config, Engine, Error, Point, SeriesKey};

fn tmp(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("ctsdb-it-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    d
}

fn cfg(dir: &PathBuf) -> Config {
    let mut c = Config::new(dir);
    c.bucket_width = 1_000;
    c.max_points_per_chunk = 50;
    c.maintenance_interval = Duration::from_millis(20);
    c
}

fn key() -> SeriesKey {
    SeriesKey::new("cpu").label("host", "a")
}

fn fill(e: &Engine, k: &SeriesKey, n: i64) {
    for i in 0..n {
        e.append(k, i * 10, (i % 17) as f64 + 0.5).unwrap();
    }
}

#[test]
fn roundtrip_across_memory_and_disk() {
    let dir = tmp("rt");
    let e = Engine::open(cfg(&dir)).unwrap();
    let k = key();
    fill(&e, &k, 1_000);

    let all = e.query_range(&k, i64::MIN, i64::MAX).unwrap();
    assert_eq!(all.len(), 1_000);
    assert!(all.windows(2).all(|w| w[0].ts < w[1].ts));

    let slice = e.query_range(&k, 2_000, 2_500).unwrap();
    assert_eq!(slice.len(), 50);
    assert_eq!(slice[0].ts, 2_000);

    e.flush().unwrap();
    assert_eq!(e.query_range(&k, i64::MIN, i64::MAX).unwrap(), all);
    assert_eq!(e.stats().frozen_chunks, 0);
    drop(e);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn aggregates_match_brute_force() {
    let dir = tmp("agg");
    let e = Engine::open(cfg(&dir)).unwrap();
    let k = key();
    fill(&e, &k, 2_000);
    e.flush().unwrap();
    fill_more(&e, &k);

    let pts = e.query_range(&k, i64::MIN, i64::MAX).unwrap();
    for (s, t) in [
        (i64::MIN, i64::MAX),
        (0, 1),
        (1_234, 15_678),
        (19_990, 25_000),
        (500, 500),
    ] {
        let want: Vec<f64> = pts
            .iter()
            .filter(|p| p.ts >= s && p.ts < t)
            .map(|p| p.value)
            .collect();
        let got = e.aggregate(&k, s, t).unwrap();
        if want.is_empty() {
            assert!(got.is_none());
            continue;
        }
        let g = got.unwrap();
        assert_eq!(g.count as usize, want.len());
        assert_eq!(g.min, want.iter().cloned().fold(f64::INFINITY, f64::min));
        assert_eq!(
            g.max,
            want.iter().cloned().fold(f64::NEG_INFINITY, f64::max)
        );
        assert!((g.sum - want.iter().sum::<f64>()).abs() < 1e-6);
    }
    drop(e);
    std::fs::remove_dir_all(dir).unwrap();
}

fn fill_more(e: &Engine, k: &SeriesKey) {
    for i in 2_000..2_300 {
        e.append(k, i * 10, i as f64 * 0.1).unwrap();
    }
}

#[test]
fn rejects_bad_input() {
    let dir = tmp("bad");
    let e = Engine::open(cfg(&dir)).unwrap();
    let k = key();
    e.append(&k, 100, 1.0).unwrap();
    assert!(matches!(
        e.append(&k, 100, 2.0),
        Err(Error::OutOfOrder { .. })
    ));
    assert!(matches!(
        e.append(&k, 50, 2.0),
        Err(Error::OutOfOrder { .. })
    ));
    assert!(matches!(
        e.append(&k, 200, f64::NAN),
        Err(Error::BadValue(_))
    ));

    // bad batch must not leave a prefix behind
    let batch = [Point::new(300, 1.0), Point::new(250, 1.0)];
    assert!(e.append_batch(&k, &batch).is_err());
    assert_eq!(e.query_range(&k, 0, 1_000).unwrap().len(), 1);
    drop(e);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn compaction_merges_fragments() {
    let dir = tmp("compact");
    let mut c = cfg(&dir);
    c.bucket_width = 1_000_000; // one bucket, many 50-point chunks
    c.maintenance_interval = Duration::from_secs(3600);
    let e = Engine::open(c).unwrap();
    let k = key();
    fill(&e, &k, 1_000);
    e.flush().unwrap();
    let before = e.query_range(&k, i64::MIN, i64::MAX).unwrap();
    assert_eq!(e.stats().sealed_chunks, 20);

    assert_eq!(e.compact().unwrap(), 1);
    assert_eq!(e.stats().sealed_chunks, 1);
    assert_eq!(e.query_range(&k, i64::MIN, i64::MAX).unwrap(), before);
    assert_eq!(e.compact().unwrap(), 0);

    let files = std::fs::read_dir(dir.join("chunks")).unwrap().count();
    assert_eq!(files, 3);
    drop(e);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn reopen_after_clean_close() {
    let dir = tmp("reopen");
    let k = key();
    {
        let e = Engine::open(cfg(&dir)).unwrap();
        fill(&e, &k, 500);
    }
    let e = Engine::open(cfg(&dir)).unwrap();
    assert_eq!(e.query_range(&k, i64::MIN, i64::MAX).unwrap().len(), 500);
    assert_eq!(e.stats().wal_bytes, 0);
    // ordering state survives too
    assert!(e.append(&k, 4_990, 1.0).is_err());
    e.append(&k, 5_000, 1.0).unwrap();
    drop(e);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn crash_recovery_replays_wal() {
    let dir = tmp("crash");
    let k = key();
    let mut c = cfg(&dir);
    c.flush_on_close = false; // drop behaves like a crash: no final checkpoint
    c.maintenance_interval = Duration::from_secs(3600);
    {
        let e = Engine::open(c.clone()).unwrap();
        fill(&e, &k, 777);
    }
    let e = Engine::open(c).unwrap();
    let pts = e.query_range(&k, i64::MIN, i64::MAX).unwrap();
    assert_eq!(pts.len(), 777);
    assert_eq!(pts[776].ts, 7_760);
    drop(e);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn second_open_is_refused() {
    let dir = tmp("lock");
    let e = Engine::open(cfg(&dir)).unwrap();
    assert!(matches!(Engine::open(cfg(&dir)), Err(Error::Locked)));
    drop(e);
    Engine::open(cfg(&dir)).unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn retention_drops_old_chunks() {
    let dir = tmp("ret");
    let mut c = cfg(&dir);
    c.retention = Some(3_000);
    let e = Engine::open(c).unwrap();
    let k = key();
    fill(&e, &k, 2_000); // ts up to 19_990
    e.flush().unwrap();

    // expiry runs on the worker; poll instead of guessing a sleep
    let mut pts = Vec::new();
    for _ in 0..200 {
        pts = e.query_range(&k, i64::MIN, i64::MAX).unwrap();
        if pts[0].ts >= 19_990 - 3_000 - 1_000 {
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }
    assert!(
        pts[0].ts >= 19_990 - 3_000 - 1_000,
        "first ts {}",
        pts[0].ts
    );
    assert_eq!(pts.last().unwrap().ts, 19_990);
    drop(e);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn concurrent_writers_and_readers() {
    let dir = tmp("conc");
    let mut c = cfg(&dir);
    c.max_points_per_chunk = 200;
    let e = Engine::open(c).unwrap();
    let done = Arc::new(AtomicBool::new(false));
    const N: i64 = 20_000;

    let writers: Vec<_> = (0..4)
        .map(|w| {
            let e = e.clone();
            thread::spawn(move || {
                let k = SeriesKey::new("m").label("w", w.to_string());
                for i in 0..N {
                    e.append(&k, i, i as f64).unwrap();
                }
            })
        })
        .collect();

    // readers: every snapshot of a series must be a gap-free prefix 0..n, even while
    // chunks are being frozen, sealed and compacted underneath
    let readers: Vec<_> = (0..3)
        .map(|r| {
            let (e, done) = (e.clone(), done.clone());
            thread::spawn(move || {
                let k = SeriesKey::new("m").label("w", r.to_string());
                let mut checks = 0;
                while !done.load(Ordering::Relaxed) {
                    let pts = e.query_range(&k, i64::MIN, i64::MAX).unwrap();
                    for (i, p) in pts.iter().enumerate() {
                        assert_eq!(p.ts, i as i64, "gap or duplicate at {i}");
                    }
                    if let Some(a) = e.aggregate(&k, 0, i64::MAX).unwrap() {
                        assert!(a.count <= N as u64);
                    }
                    checks += 1;
                }
                checks
            })
        })
        .collect();

    for w in writers {
        w.join().unwrap();
    }
    done.store(true, Ordering::Relaxed);
    for r in readers {
        assert!(r.join().unwrap() > 0);
    }

    for w in 0..4 {
        let k = SeriesKey::new("m").label("w", w.to_string());
        let a = e.aggregate(&k, 0, N).unwrap().unwrap();
        assert_eq!(a.count, N as u64);
        assert_eq!(a.sum, (N * (N - 1) / 2) as f64);
    }
    drop(e);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn block_cache_stays_under_budget() {
    let dir = tmp("cache");
    let mut c = cfg(&dir);
    c.block_cache_bytes = 2_000;
    let e = Engine::open(c).unwrap();
    let k = key();
    fill(&e, &k, 3_000);
    e.flush().unwrap();

    let first = e.query_range(&k, i64::MIN, i64::MAX).unwrap();
    assert!(e.stats().cache_bytes <= 2_000);
    assert_eq!(e.query_range(&k, i64::MIN, i64::MAX).unwrap(), first);
    drop(e);
    std::fs::remove_dir_all(dir).unwrap();
}
