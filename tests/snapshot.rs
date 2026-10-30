use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use columnar_tsdb::snapshot::restore;
use columnar_tsdb::{Config, Engine, Error, Point, SeriesKey};

fn tmp(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("ctsdb-sn-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    d
}

fn cfg(dir: &PathBuf) -> Config {
    let mut c = Config::new(dir);
    c.bucket_width = 1_000;
    c.max_points_per_chunk = 50;
    c.maintenance_interval = Duration::from_millis(10);
    c
}

fn key(h: &str) -> SeriesKey {
    SeriesKey::new("cpu").label("host", h)
}

#[test]
fn snapshot_matches_source_and_is_independent() {
    let (src, snap) = (tmp("a-src"), tmp("a-snap"));
    let e = Engine::open(cfg(&src)).unwrap();
    for h in ["a", "b", "c"] {
        for i in 0..700 {
            e.append(&key(h), i * 10, (i % 13) as f64).unwrap();
        }
    }
    // a late overwrite that is still sitting in the wal / ooo buffer
    e.append(&key("a"), 50, 99.0).unwrap();

    let info = e.snapshot(&snap).unwrap();
    assert!(info.chunks > 0);
    assert!(info.linked_bytes + info.copied_bytes > 0);

    // the source keeps working and diverges
    e.append(&key("a"), 10_000, 1.0).unwrap();

    let s = Engine::open(cfg(&snap)).unwrap();
    for h in ["a", "b", "c"] {
        let want: Vec<Point> = e.query_range(&key(h), i64::MIN, 10_000).unwrap();
        let got = s.query_range(&key(h), i64::MIN, i64::MAX).unwrap();
        assert_eq!(got, want, "host {h}");
    }
    assert_eq!(s.query_range(&key("a"), 50, 51).unwrap()[0].value, 99.0);
    drop(s);
    drop(e);
    let _ = std::fs::remove_dir_all(src);
    let _ = std::fs::remove_dir_all(snap);
}

#[test]
fn snapshot_during_writes_is_a_gap_free_prefix() {
    let (src, snap) = (tmp("b-src"), tmp("b-snap"));
    // 50-point chunks would make the sealer slower than the writer and it never lets go of the
    // maintenance lock, so use sizes closer to a real setup here
    let mut c = cfg(&src);
    c.bucket_width = 100_000;
    c.max_points_per_chunk = 2_000;
    let e = Engine::open(c.clone()).unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let written = Arc::new(AtomicI64::new(0));

    let w = {
        let (e, stop, written) = (e.clone(), stop.clone(), written.clone());
        thread::spawn(move || {
            let k = key("w");
            let mut i = 0i64;
            while !stop.load(Ordering::Relaxed) {
                e.append(&k, i, i as f64).unwrap();
                i += 1;
                written.store(i, Ordering::Release);
                if i % 100 == 0 {
                    thread::sleep(Duration::from_millis(1));
                }
            }
        })
    };

    while written.load(Ordering::Acquire) < 2_000 {
        thread::sleep(Duration::from_millis(2));
    }
    let floor = written.load(Ordering::Acquire);
    e.snapshot(&snap).unwrap();
    stop.store(true, Ordering::Relaxed);
    w.join().unwrap();

    c.dir = snap.clone();
    let s = Engine::open(c).unwrap();
    let pts = s.query_range(&key("w"), i64::MIN, i64::MAX).unwrap();
    assert!(pts.len() as i64 >= floor, "{} < {floor}", pts.len());
    for (i, p) in pts.iter().enumerate() {
        assert_eq!((p.ts, p.value), (i as i64, i as f64), "gap at {i}");
    }
    drop(s);
    drop(e);
    let _ = std::fs::remove_dir_all(src);
    let _ = std::fs::remove_dir_all(snap);
}

#[test]
fn bad_targets_and_restore() {
    let (src, snap, back) = (tmp("c-src"), tmp("c-snap"), tmp("c-back"));
    let e = Engine::open(cfg(&src)).unwrap();
    for i in 0..300 {
        e.append(&key("x"), i, i as f64).unwrap();
    }

    // the live dir itself is not an empty target
    assert!(matches!(e.snapshot(&src), Err(Error::BadConfig(_))));

    e.snapshot(&snap).unwrap();
    assert!(matches!(e.snapshot(&snap), Err(Error::BadConfig(_))));
    drop(e);

    let n = restore(&snap, &back).unwrap();
    assert!(n > 0);
    let r = Engine::open(cfg(&back)).unwrap();
    assert_eq!(r.query_range(&key("x"), 0, 300).unwrap().len(), 300);
    drop(r);
    for d in [src, snap, back] {
        let _ = std::fs::remove_dir_all(d);
    }
}
