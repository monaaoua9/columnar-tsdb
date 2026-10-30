use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use columnar_tsdb::{Config, Engine, Point, Selector, SeriesKey};

fn tmp(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("ctsdb-del-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    d
}

fn cfg(dir: &Path) -> Config {
    let mut c = Config::new(dir);
    c.bucket_width = 1_000;
    c.max_points_per_chunk = 50;
    c.maintenance_interval = Duration::from_millis(20);
    c
}

fn key() -> SeriesKey {
    SeriesKey::new("cpu").label("host", "a")
}

fn all(e: &Engine, k: &SeriesKey) -> Vec<(i64, f64)> {
    e.query_range(k, i64::MIN, i64::MAX)
        .unwrap()
        .iter()
        .map(|p| (p.ts, p.value))
        .collect()
}

fn fill(e: &Engine, k: &SeriesKey, n: i64) -> BTreeMap<i64, f64> {
    let mut m = BTreeMap::new();
    for i in 0..n {
        let v = (i % 17) as f64 + 0.5;
        e.append(k, i * 10, v).unwrap();
        m.insert(i * 10, v);
    }
    m
}

fn assert_matches(e: &Engine, k: &SeriesKey, model: &BTreeMap<i64, f64>, what: &str) {
    let got = all(e, k);
    let want: Vec<(i64, f64)> = model.iter().map(|(&t, &v)| (t, v)).collect();
    assert_eq!(got.len(), want.len(), "{what}: length");
    assert_eq!(got, want, "{what}");

    // the stats-only path must agree with the points, or chunk stats were not rebuilt
    let agg = e.aggregate(k, i64::MIN, i64::MAX).unwrap();
    match agg {
        None => assert!(model.is_empty(), "{what}: aggregate empty but model is not"),
        Some(a) => {
            assert_eq!(a.count as usize, model.len(), "{what}: agg count");
            let sum: f64 = model.values().sum();
            assert!(
                (a.sum - sum).abs() < 1e-6,
                "{what}: agg sum {} vs {sum}",
                a.sum
            );
            let min = model.values().cloned().fold(f64::INFINITY, f64::min);
            let max = model.values().cloned().fold(f64::NEG_INFINITY, f64::max);
            assert_eq!((a.min, a.max), (min, max), "{what}: agg min/max");
        }
    }
}

fn model_delete(m: &mut BTreeMap<i64, f64>, s: i64, e: i64) -> u64 {
    if s >= e {
        return 0;
    }
    let doomed: Vec<i64> = m.range(s..e).map(|(&t, _)| t).collect();
    for t in &doomed {
        m.remove(t);
    }
    doomed.len() as u64
}

#[test]
fn deletes_across_memory_and_disk() {
    let dir = tmp("basic");
    let e = Engine::open(cfg(&dir)).unwrap();
    let k = key();
    let mut m = fill(&e, &k, 1_000); // ts 0..9990, mostly sealed, tail still in memory

    // spans several sealed chunks, cuts the middle out of the edge ones
    let n = e.delete_range(&k, 1_234, 6_789).unwrap();
    assert_eq!(n, model_delete(&mut m, 1_234, 6_789));
    assert_eq!(n, 555);
    assert_matches(&e, &k, &m, "after first delete");

    // the newest points (active / frozen chunks)
    let n = e.delete_range(&k, 9_500, i64::MAX).unwrap();
    assert_eq!(n, model_delete(&mut m, 9_500, i64::MAX));
    assert_matches(&e, &k, &m, "after tail delete");

    // empty and inverted ranges, unknown series
    assert_eq!(e.delete_range(&k, 5, 5).unwrap(), 0);
    assert_eq!(e.delete_range(&k, 100, 50).unwrap(), 0);
    assert_eq!(e.delete_range(&SeriesKey::new("nope"), 0, 100).unwrap(), 0);
    assert_eq!(e.delete_range(&k, 2_000, 3_000).unwrap(), 0); // already gone
    assert_matches(&e, &k, &m, "after no-ops");

    // still the same after sealing everything, compacting and a clean reopen
    e.flush().unwrap();
    e.compact().unwrap();
    assert_matches(&e, &k, &m, "after flush + compact");
    drop(e);
    let e = Engine::open(cfg(&dir)).unwrap();
    assert_matches(&e, &k, &m, "after reopen");
    drop(e);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn delete_everything_leaves_an_empty_series() {
    let dir = tmp("all");
    let e = Engine::open(cfg(&dir)).unwrap();
    let k = key();
    fill(&e, &k, 400);
    e.append(&k, 5, 99.0).unwrap(); // late point in the buffer

    assert_eq!(e.delete_range(&k, i64::MIN, i64::MAX).unwrap(), 401);
    assert!(all(&e, &k).is_empty());
    assert_eq!(e.aggregate(&k, i64::MIN, i64::MAX).unwrap(), None);
    assert_eq!(e.bounds(&k), None);
    assert_eq!(e.list_series().len(), 1);
    e.flush().unwrap();
    assert_eq!(e.stats().sealed_chunks, 0);

    // the series is reusable, including at timestamps older than its old newest point
    e.append(&k, 20, 1.0).unwrap();
    e.append(&k, 3_000, 2.0).unwrap();
    assert_eq!(all(&e, &k), vec![(20, 1.0), (3_000, 2.0)]);
    drop(e);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn writes_after_a_delete_survive_it() {
    let dir = tmp("after");
    let e = Engine::open(cfg(&dir)).unwrap();
    let k = key();
    let mut m = fill(&e, &k, 300);
    e.flush().unwrap();

    e.delete_range(&k, 500, 1_500).unwrap();
    model_delete(&mut m, 500, 1_500);

    // same range again: some points are "late" (below the series' newest ts), they must stay
    for t in [510, 700, 1_499] {
        e.append(&k, t, -1.0).unwrap();
        m.insert(t, -1.0);
    }
    assert_matches(&e, &k, &m, "before restart");
    e.flush().unwrap();
    assert_matches(&e, &k, &m, "after flush");
    drop(e);
    let e = Engine::open(cfg(&dir)).unwrap();
    assert_matches(&e, &k, &m, "after reopen");
    drop(e);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn delete_survives_a_crash_even_if_old_chunk_files_come_back() {
    let dir = tmp("crash");
    let mut c = cfg(&dir);
    c.flush_on_close = false; // drop acts like a crash: no checkpoint
    c.maintenance_interval = Duration::from_secs(600);
    let k = key();

    let e = Engine::open(c.clone()).unwrap();
    let mut m = fill(&e, &k, 600);
    quiet(&e); // everything sealed and merged, wal empty

    // keep a copy of the chunk dir as it was before the delete
    let before = tmp("crash-before");
    copy_dir(&dir.join("chunks"), &before);

    e.delete_range(&k, 1_000, 4_000).unwrap();
    model_delete(&mut m, 1_000, 4_000);
    assert_matches(&e, &k, &m, "live");
    drop(e);

    // worst case: the replaced chunk files were still on disk when the process died
    let chunks = dir.join("chunks");
    for f in std::fs::read_dir(&before).unwrap() {
        let f = f.unwrap().path();
        let _ = std::fs::copy(&f, chunks.join(f.file_name().unwrap()));
    }
    let e = Engine::open(c.clone()).unwrap();
    assert_matches(&e, &k, &m, "recovered with resurrected chunk files");
    drop(e);

    // and once more, replaying the same wal again must not change anything
    let e = Engine::open(c).unwrap();
    assert_matches(&e, &k, &m, "recovered twice");
    drop(e);
    let _ = std::fs::remove_dir_all(dir);
    let _ = std::fs::remove_dir_all(before);
}

// no background pass may be rewriting chunk files while a test copies the directory
fn quiet(e: &Engine) {
    e.flush().unwrap();
    e.compact().unwrap();
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for f in std::fs::read_dir(from).unwrap() {
        let f = f.unwrap().path();
        std::fs::copy(&f, to.join(f.file_name().unwrap())).unwrap();
    }
}

#[test]
fn wal_is_not_truncated_while_replaced_files_exist() {
    let dir = tmp("pin");
    let mut c = cfg(&dir);
    c.flush_on_close = false;
    c.maintenance_interval = Duration::from_secs(600);
    let k = key();
    let e = Engine::open(c.clone()).unwrap();
    let mut m = fill(&e, &k, 400);
    quiet(&e);

    // a long-running reader pins the old chunks: it holds Arcs through an in-flight query,
    // which we emulate by deleting while the old files can't be removed (copy them back)
    let before = tmp("pin-before");
    copy_dir(&dir.join("chunks"), &before);
    e.delete_range(&k, 0, 2_000).unwrap();
    model_delete(&mut m, 0, 2_000);
    let chunks = dir.join("chunks");
    for f in std::fs::read_dir(&before).unwrap() {
        let f = f.unwrap().path();
        let _ = std::fs::copy(&f, chunks.join(f.file_name().unwrap()));
    }
    // checkpoint now: the old meta files exist again, so the delete record must stay in the wal
    e.flush().unwrap();
    assert!(
        e.stats().wal_bytes > 0,
        "wal was truncated under a live delete"
    );
    drop(e);

    let e = Engine::open(c).unwrap();
    assert_matches(&e, &k, &m, "after reopen");
    drop(e);
    let _ = std::fs::remove_dir_all(dir);
    let _ = std::fs::remove_dir_all(before);
}

#[test]
fn delete_select_and_metrics() {
    let dir = tmp("select");
    let e = Engine::open(cfg(&dir)).unwrap();
    for h in ["a", "b", "c"] {
        fill(&e, &SeriesKey::new("cpu").label("host", h), 100);
    }
    fill(&e, &SeriesKey::new("mem").label("host", "a"), 100);

    let sel = Selector::new("cpu").ne("host", "c");
    assert_eq!(e.delete_select(&sel, 0, 500).unwrap(), 2 * 50);

    let left = |name: &str, h: &str| all(&e, &SeriesKey::new(name).label("host", h)).len();
    assert_eq!(
        (left("cpu", "a"), left("cpu", "b"), left("cpu", "c")),
        (50, 50, 100)
    );
    assert_eq!(left("mem", "a"), 100);

    let m = e.metrics();
    assert_eq!((m.deletes, m.points_deleted), (2, 100));
    assert!(e.render_metrics().contains("tsdb_points_deleted_total 100"));
    drop(e);
    let _ = std::fs::remove_dir_all(dir);
}

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

// in-order, late and duplicate writes mixed with deletes, flushes, compactions and
// crash-style reopens; every step is checked against a BTreeMap
#[test]
fn randomized_against_a_model() {
    for seed in 1..=3u64 {
        let dir = tmp(&format!("rand{seed}"));
        let mut c = cfg(&dir);
        c.flush_on_close = false;
        c.max_ooo_points = 30;
        let k = key();
        let mut e = Engine::open(c.clone()).unwrap();
        let mut m: BTreeMap<i64, f64> = BTreeMap::new();
        let mut r = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
        let mut next_ts = 0i64;

        for step in 0..250 {
            match r.below(100) {
                0..=54 => {
                    let n = 1 + r.below(20);
                    let mut pts = Vec::new();
                    for _ in 0..n {
                        let t = if r.below(4) == 0 && next_ts > 0 {
                            r.below(next_ts as u64) as i64 // late or duplicate
                        } else {
                            next_ts += 1 + r.below(15) as i64;
                            next_ts
                        };
                        let v = r.below(1000) as f64 / 8.0;
                        pts.push(Point::new(t, v));
                    }
                    e.append_batch(&k, &pts).unwrap();
                    for p in pts {
                        m.insert(p.ts, p.value);
                    }
                }
                55..=74 => {
                    let a = r.below(next_ts as u64 + 50) as i64 - 20;
                    let b = a + r.below(400) as i64 - 20;
                    let got = e.delete_range(&k, a, b).unwrap();
                    let want = model_delete(&mut m, a, b);
                    // late points pending over a sealed point are counted twice, never fewer
                    assert!(
                        got >= want,
                        "seed {seed} step {step}: removed {got} < {want}"
                    );
                }
                75..=84 => e.flush().unwrap(),
                85..=89 => {
                    e.compact().unwrap();
                }
                _ => {
                    drop(e); // crash: wal replay has to redo whatever was not sealed
                    e = Engine::open(c.clone()).unwrap();
                }
            }
            assert_matches(&e, &k, &m, &format!("seed {seed} step {step}"));
        }
        drop(e);
        let e = Engine::open(c).unwrap();
        assert_matches(&e, &k, &m, &format!("seed {seed} final"));
        drop(e);
        let _ = std::fs::remove_dir_all(dir);
    }
}

// the bug the randomized test found: a delete range can reach past the newest ts, so points
// written after the delete are in-order, land inside the range, and may be sealed before a
// crash. Replaying the delete record must not eat them.
#[test]
fn points_written_after_a_delete_survive_replay_even_when_sealed() {
    let dir = tmp("regress");
    let mut c = cfg(&dir);
    c.flush_on_close = false;
    let k = key();
    let e = Engine::open(c.clone()).unwrap();
    let mut m = fill(&e, &k, 300); // last ts 2990
    e.flush().unwrap();

    e.delete_range(&k, 2_500, 4_000).unwrap();
    model_delete(&mut m, 2_500, 4_000);

    // in-order (above 2990) but inside the deleted range, plus a late one inside it
    for t in [3_100, 3_200, 3_300, 2_600] {
        e.append(&k, t, -7.0).unwrap();
        m.insert(t, -7.0);
    }
    // a later bucket freezes the chunk holding them, so the worker seals it
    for t in [5_000, 5_010] {
        e.append(&k, t, 1.0).unwrap();
        m.insert(t, 1.0);
    }
    for _ in 0..200 {
        if e.stats().frozen_chunks == 0 && e.stats().ooo_points == 0 {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        e.stats().wal_bytes > 0,
        "test needs the wal to still hold the delete"
    );
    assert_matches(&e, &k, &m, "live");
    drop(e); // crash

    for round in 0..3 {
        let e = Engine::open(c.clone()).unwrap();
        assert_matches(&e, &k, &m, &format!("replay round {round}"));
        // new writes between replays must not change the verdict of the next one
        let t = 6_000 + round * 10;
        e.append(&k, t, 2.0).unwrap();
        m.insert(t, 2.0);
        drop(e);
    }
    let _ = std::fs::remove_dir_all(dir);
}
