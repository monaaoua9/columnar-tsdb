use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use columnar_tsdb::{Config, Engine, Error, Point, Selector, SeriesKey};

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
        e.append(&k, 200, f64::NAN),
        Err(Error::BadValue(_))
    ));
    assert!(matches!(
        e.append(&k, 200, f64::INFINITY),
        Err(Error::BadValue(_))
    ));

    // bad batch must not leave a prefix behind, late points included
    let batch = [
        Point::new(300, 1.0),
        Point::new(50, 1.0),
        Point::new(60, f64::NAN),
    ];
    assert!(e.append_batch(&k, &batch).is_err());
    assert_eq!(e.query_range(&k, 0, 1_000).unwrap().len(), 1);
    assert_eq!(e.stats().ooo_points, 0);
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
    // "newest ts" survives the restart: this is a late overwrite, not a new point
    e.append(&k, 4_990, 1.0).unwrap();
    let pts = e.query_range(&k, i64::MIN, i64::MAX).unwrap();
    assert_eq!((pts.len(), pts[499]), (500, Point::new(4_990, 1.0)));
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

    // a late point that would be expired immediately is refused instead of silently kept
    assert!(matches!(
        e.append(&k, 0, 1.0),
        Err(Error::TooOld { got: 0, .. })
    ));
    e.append(&k, 19_000, 7.0).unwrap();
    drop(e);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn late_points_are_visible_immediately_and_survive_flush() {
    let dir = tmp("late");
    let e = Engine::open(cfg(&dir)).unwrap();
    let k = key();
    fill(&e, &k, 100); // 0, 10, ..., 990

    // between existing points, before the first one, and a duplicate that must win
    e.append(&k, 5, 55.0).unwrap();
    e.append(&k, -40, 44.0).unwrap();
    e.append(&k, 500, -1.0).unwrap();
    let check = |e: &Engine| {
        let pts = e.query_range(&k, i64::MIN, i64::MAX).unwrap();
        assert_eq!(pts.len(), 102);
        assert!(pts.windows(2).all(|w| w[0].ts < w[1].ts));
        assert_eq!(pts[0], Point::new(-40, 44.0));
        assert!(pts.contains(&Point::new(5, 55.0)));
        assert!(pts.contains(&Point::new(500, -1.0)));
        let a = e.aggregate(&k, i64::MIN, i64::MAX).unwrap().unwrap();
        assert_eq!(a.count, 102);
        assert_eq!(a.min, -1.0);
        assert_eq!(a.max, 55.0);
    };
    check(&e);
    assert_eq!(e.stats().ooo_points, 3);

    e.flush().unwrap();
    check(&e);
    assert_eq!(e.stats().ooo_points, 0);
    assert_eq!(e.stats().wal_bytes, 0);

    // overwrite a value that now lives in a sealed chunk
    e.append(&k, 500, 123.0).unwrap();
    let a = e.aggregate(&k, 400, 600).unwrap().unwrap();
    assert_eq!((a.count, a.max), (20, 123.0));
    e.flush().unwrap();
    assert_eq!(e.aggregate(&k, 400, 600).unwrap().unwrap().max, 123.0);
    drop(e);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn late_points_survive_crash_via_wal() {
    let dir = tmp("late-crash");
    let k = key();
    let mut c = cfg(&dir);
    c.flush_on_close = false;
    c.maintenance_interval = Duration::from_secs(3600);
    {
        let e = Engine::open(c.clone()).unwrap();
        fill(&e, &k, 300);
        e.append(&k, 3, 33.0).unwrap();
        e.append(&k, 3, 34.0).unwrap(); // second late write to the same ts
        e.append(&k, 1_005, -5.0).unwrap();
    }
    let e = Engine::open(c).unwrap();
    let pts = e.query_range(&k, i64::MIN, i64::MAX).unwrap();
    assert_eq!(pts.len(), 301 + 1);
    assert!(pts.contains(&Point::new(3, 34.0)));
    assert!(!pts.contains(&Point::new(3, 33.0)));
    assert!(pts.contains(&Point::new(1_005, -5.0)));
    drop(e);
    std::fs::remove_dir_all(dir).unwrap();
}

// xorshift, same as the unit tests use
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

// Throws random in-order and late writes at the engine, with flushes, compactions and
// crash-style reopens in between, and checks every answer against a BTreeMap.
#[test]
fn random_operations_match_a_model() {
    use std::collections::BTreeMap;

    let dir = tmp("model");
    let k = key();
    let mut c = cfg(&dir);
    c.bucket_width = 500;
    c.max_points_per_chunk = 20;
    c.maintenance_interval = Duration::from_millis(5);
    c.flush_on_close = false;

    let seed = std::env::var("TSDB_SEED").ok().and_then(|s| s.parse().ok());
    let mut rng = Rng(seed.unwrap_or(0x5EED));
    let mut model: BTreeMap<i64, f64> = BTreeMap::new();
    let mut e = Engine::open(c.clone()).unwrap();
    let mut head = 0i64;

    for step in 0..400 {
        match rng.below(10) {
            0..=4 => {
                // a batch: mostly moving forward, some late, some duplicates
                let mut batch = Vec::new();
                for _ in 0..1 + rng.below(12) {
                    let ts = if rng.below(3) == 0 {
                        rng.below((head + 1) as u64) as i64
                    } else {
                        head += 1 + rng.below(30) as i64;
                        head
                    };
                    let v = rng.below(10_000) as f64 / 100.0;
                    batch.push(Point::new(ts, v));
                }
                e.append_batch(&k, &batch).unwrap();
                for p in &batch {
                    model.insert(p.ts, p.value);
                }
            }
            5 => e.flush().unwrap(),
            6 => {
                e.compact().unwrap();
            }
            7 if step % 7 == 0 => {
                // crash: no checkpoint, everything not sealed comes back from the wal
                drop(e);
                e = Engine::open(c.clone()).unwrap();
            }
            _ => thread::sleep(Duration::from_millis(3)),
        }

        // random window plus the full range
        let a = rng.below((head + 2) as u64) as i64;
        let b = a + rng.below(800) as i64;
        for (lo, hi) in [(a, b), (i64::MIN, i64::MAX)] {
            let want: Vec<Point> = model
                .range(lo..hi)
                .map(|(&t, &v)| Point::new(t, v))
                .collect();
            let got = e.query_range(&k, lo, hi).unwrap();
            assert_eq!(got, want, "step {step} range {lo}..{hi}");

            match e.aggregate(&k, lo, hi).unwrap() {
                None => assert!(want.is_empty(), "step {step}"),
                Some(g) => {
                    assert_eq!(g.count as usize, want.len(), "step {step}");
                    let sum: f64 = want.iter().map(|p| p.value).sum();
                    assert!(
                        (g.sum - sum).abs() < 1e-6 * sum.abs().max(1.0),
                        "step {step}"
                    );
                    let min = want.iter().map(|p| p.value).fold(f64::INFINITY, f64::min);
                    let max = want
                        .iter()
                        .map(|p| p.value)
                        .fold(f64::NEG_INFINITY, f64::max);
                    assert_eq!((g.min, g.max), (min, max), "step {step}");
                }
            }
        }
    }

    // and once more after a clean checkpoint + reopen
    e.flush().unwrap();
    assert_eq!(e.stats().ooo_points, 0);
    drop(e);
    let e = Engine::open(c).unwrap();
    let want: Vec<Point> = model.iter().map(|(&t, &v)| Point::new(t, v)).collect();
    assert_eq!(e.query_range(&k, i64::MIN, i64::MAX).unwrap(), want);
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

#[test]
fn label_selectors_and_cross_series_queries() {
    let dir = tmp("labels");
    let c = cfg(&dir);
    let e = Engine::open(c.clone()).unwrap();

    let mut keys = Vec::new();
    for h in 0..4 {
        let dc = if h % 2 == 0 { "fra" } else { "ams" };
        let k = SeriesKey::new("cpu")
            .label("host", format!("h{h}"))
            .label("dc", dc);
        for i in 0..300i64 {
            e.append(&k, i * 10, (h * 100) as f64 + (i % 9) as f64)
                .unwrap();
        }
        keys.push(k);
    }
    let other = SeriesKey::new("mem").label("host", "h0");
    e.append(&other, 5, 999.0).unwrap();
    // late writes + a sealed/unsealed mix
    e.flush().unwrap();
    e.append(&keys[0], 15, 1_000.0).unwrap();
    e.append(&keys[1], 2_995, 2_000.0).unwrap();

    let sels = [
        Selector::new("cpu"),
        Selector::new("cpu").eq("dc", "fra"),
        Selector::new("cpu").eq("dc", "ams").ne("host", "h1"),
        Selector::new("cpu").ne("dc", "fra"),
        Selector::new("mem"),
        Selector::new("nothing"),
    ];
    for sel in &sels {
        let matched = e.select(sel);
        let want: Vec<_> = keys
            .iter()
            .chain([&other])
            .filter(|k| sel.matches(k))
            .collect();
        assert_eq!(matched.len(), want.len(), "{sel:?}");
        assert!(want.iter().all(|k| matched.contains(k)));

        for (lo, hi) in [(i64::MIN, i64::MAX), (100, 1_234)] {
            let series = e.query_select(sel, lo, hi).unwrap();
            let all: Vec<f64> = series
                .iter()
                .flat_map(|(_, p)| p.iter().map(|p| p.value))
                .collect();
            match e.aggregate_select(sel, lo, hi).unwrap() {
                None => assert!(all.is_empty()),
                Some(a) => {
                    assert_eq!(a.count as usize, all.len());
                    assert!((a.sum - all.iter().sum::<f64>()).abs() < 1e-6);
                    assert_eq!(a.max, all.iter().cloned().fold(f64::MIN, f64::max));
                    assert_eq!(a.min, all.iter().cloned().fold(f64::MAX, f64::min));
                }
            }
        }
    }

    let by_dc = e
        .aggregate_by(&Selector::new("cpu"), "dc", i64::MIN, i64::MAX)
        .unwrap();
    assert_eq!(by_dc.keys().collect::<Vec<_>>(), ["ams", "fra"]);
    let total = e
        .aggregate_select(&Selector::new("cpu"), i64::MIN, i64::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(by_dc.values().map(|a| a.count).sum::<u64>(), total.count);
    let missing = e
        .aggregate_by(&Selector::new("cpu"), "no_such_label", i64::MIN, i64::MAX)
        .unwrap();
    assert_eq!(missing.len(), 1);
    assert_eq!(missing[""].count, total.count);

    // the index is rebuilt from the catalog on open
    drop(e);
    let e = Engine::open(c).unwrap();
    assert_eq!(e.select(&Selector::new("cpu").eq("dc", "fra")).len(), 2);
    assert_eq!(e.select(&Selector::new("mem")).len(), 1);
    drop(e);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn windowed_aggregates_match_brute_force() {
    let dir = tmp("windows");
    let e = Engine::open(cfg(&dir)).unwrap();
    let a = SeriesKey::new("w").label("n", "a");
    let b = SeriesKey::new("w").label("n", "b");
    fill(&e, &a, 700);
    for i in 0..400i64 {
        e.append(&b, 1_000 + i * 15, i as f64).unwrap();
    }
    e.flush().unwrap();
    e.append(&a, 123, 500.0).unwrap(); // late, lands inside a sealed chunk's span
    e.append(&a, 7_005, 9.0).unwrap();

    let pts = e.query_range(&a, i64::MIN, i64::MAX).unwrap();
    for (start, end, step) in [(0, 7_100, 250), (-300, 8_000, 1_000), (333, 5_000, 77)] {
        let got = e.aggregate_windows(&a, start, end, step).unwrap();
        let mut want = Vec::new();
        let mut w = start;
        while w < end {
            let hi = (w + step).min(end);
            let vals: Vec<f64> = pts
                .iter()
                .filter(|p| p.ts >= w && p.ts < hi)
                .map(|p| p.value)
                .collect();
            if !vals.is_empty() {
                want.push((w, vals.len() as u64, vals.iter().sum::<f64>()));
            }
            w = hi;
        }
        assert_eq!(got.len(), want.len(), "{start}..{end} step {step}");
        for ((gw, ga), (ww, wc, ws)) in got.iter().zip(&want) {
            assert_eq!((*gw, ga.count), (*ww, *wc));
            assert!((ga.sum - ws).abs() < 1e-6);
        }
    }

    // huge range must not mean a huge loop, and windows are merged across series
    let all = e
        .aggregate_windows_select(&Selector::new("w"), i64::MIN, i64::MAX, 100_000)
        .unwrap();
    assert_eq!(all.len(), 1);
    assert_eq!(all[0].1.count, 702 + 400);
    assert!(matches!(
        e.aggregate_windows(&a, 0, 10, 0),
        Err(Error::BadConfig(_))
    ));
    assert!(e.aggregate_windows(&a, 50, 50, 10).unwrap().is_empty());
    drop(e);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn glob_matchers_through_the_engine() {
    let dir = tmp("glob");
    let e = Engine::open(cfg(&dir)).unwrap();
    for host in ["web-1", "web-2", "db-1", "cache"] {
        e.append(&SeriesKey::new("up").label("host", host), 1, 1.0)
            .unwrap();
    }
    let n = |sel: Selector| e.select(&sel).len();
    assert_eq!(n(Selector::new("up").like("host", "web-*")), 2);
    assert_eq!(n(Selector::new("up").like("host", "*-1")), 2);
    assert_eq!(
        n(Selector::new("up")
            .like("host", "web-?")
            .ne("host", "web-2")),
        1
    );
    assert_eq!(n(Selector::new("up").like("host", "*")), 4);
    assert_eq!(n(Selector::new("up").like("region", "*")), 0);
    let a = e
        .aggregate_select(&Selector::new("up").like("host", "w*"), 0, 10)
        .unwrap()
        .unwrap();
    assert_eq!(a.count, 2);
    drop(e);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn crash_recovery_across_wal_shards() {
    let dir = tmp("shards");
    let mut c = cfg(&dir);
    c.flush_on_close = false;
    c.wal_shards = 4;
    c.maintenance_interval = Duration::from_secs(3600);
    let keys: Vec<_> = (0..13)
        .map(|i| SeriesKey::new("s").label("i", i.to_string()))
        .collect();
    {
        let e = Engine::open(c.clone()).unwrap();
        for (n, k) in keys.iter().enumerate() {
            fill(&e, k, 120 + n as i64);
            e.append(k, 7, -1.0).unwrap(); // late, replayed from the same shard
        }
    }
    // data really is spread over several wal files
    let used = (0..4)
        .filter(|i| {
            std::fs::metadata(dir.join(format!("wal-{i}.log")))
                .unwrap()
                .len()
                > 0
        })
        .count();
    assert!(used >= 2, "only {used} wal files in use");

    // a different shard count with data still in the logs is refused, not silently misrouted
    let mut other = c.clone();
    other.wal_shards = 2;
    assert!(matches!(Engine::open(other), Err(Error::BadConfig(_))));

    let e = Engine::open(c).unwrap();
    for (n, k) in keys.iter().enumerate() {
        let pts = e.query_range(k, i64::MIN, i64::MAX).unwrap();
        assert_eq!(pts.len(), 121 + n, "series {n}");
        assert!(pts.contains(&Point::new(7, -1.0)));
    }
    e.flush().unwrap();
    assert_eq!(e.stats().wal_bytes, 0);
    drop(e);

    // after a clean flush the shard count may change
    let mut other = cfg(&dir);
    other.wal_shards = 2;
    let e = Engine::open(other).unwrap();
    assert_eq!(
        e.query_range(&keys[5], i64::MIN, i64::MAX).unwrap().len(),
        126
    );
    drop(e);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn metrics_follow_activity() {
    let dir = tmp("metrics");
    let e = Engine::open(cfg(&dir)).unwrap();
    let k = key();
    fill(&e, &k, 400);
    e.append(&k, 5, 1.0).unwrap(); // late
    e.flush().unwrap();
    e.query_range(&k, 0, 100).unwrap();
    e.aggregate(&k, 0, 100).unwrap();

    let m = e.metrics();
    assert_eq!(m.points_appended, 401);
    assert_eq!(m.late_points, 1);
    assert_eq!(m.append_calls, 401);
    assert!(m.chunks_sealed >= 8, "{m:?}");
    assert_eq!(m.queries, 2);
    assert!(m.ooo_merges >= 1);

    let text = e.render_metrics();
    assert!(text.contains("tsdb_points_appended_total 401"));
    assert!(text.contains("tsdb_series 1"));
    drop(e);
    let _ = std::fs::remove_dir_all(dir);
}
