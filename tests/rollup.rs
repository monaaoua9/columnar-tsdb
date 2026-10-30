use std::path::PathBuf;
use std::time::Duration;

use columnar_tsdb::query::{self, Ctx, Value};
use columnar_tsdb::rollup::{rollup_key, Rollup, RollupAgg};
use columnar_tsdb::{Config, Engine, Error, Selector, SeriesKey};

fn open(tag: &str, retention: Option<i64>) -> (Engine, PathBuf) {
    let dir = std::env::temp_dir().join(format!("ctsdb-ru-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let mut c = Config::new(&dir);
    c.bucket_width = 600_000;
    c.maintenance_interval = Duration::from_millis(20);
    c.retention = retention;
    (Engine::open(c).unwrap(), dir)
}

fn key(host: &str) -> SeriesKey {
    SeriesKey::new("cpu").label("host", host)
}

// one point every 10s for `mins` minutes; value = seconds since start
fn fill(e: &Engine, host: &str, from_min: i64, mins: i64) {
    for i in from_min * 6..(from_min + mins) * 6 {
        e.append(&key(host), i * 10_000, (i * 10) as f64).unwrap();
    }
}

fn vals(e: &Engine, k: &SeriesKey) -> Vec<(i64, f64)> {
    e.query_range(k, i64::MIN, i64::MAX)
        .unwrap()
        .iter()
        .map(|p| (p.ts, p.value))
        .collect()
}

#[test]
fn rolls_complete_windows_only() {
    let (e, dir) = open("basic", None);
    fill(&e, "a", 0, 10); // 0..600s
    let r = Rollup::new(Selector::new("cpu"), 60_000);

    // upto in the middle of minute 5 -> minutes 0..=4 are complete
    let n = r.run(&e, 5 * 60_000 + 30_000).unwrap();
    assert_eq!(n, 5 * 5);

    let avg = vals(&e, &rollup_key(&key("a"), 60_000, RollupAgg::Avg));
    assert_eq!(avg.len(), 5);
    // minute 0 holds seconds 0,10,..,50 -> avg 25
    assert_eq!(avg[0], (0, 25.0));
    assert_eq!(avg[1], (60_000, 85.0));
    let cnt = vals(&e, &rollup_key(&key("a"), 60_000, RollupAgg::Count));
    assert!(cnt.iter().all(|c| c.1 == 6.0));
    let max = vals(&e, &rollup_key(&key("a"), 60_000, RollupAgg::Max));
    assert_eq!(max[0].1, 50.0);

    drop(e);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn incremental_and_idempotent() {
    let (e, dir) = open("incr", None);
    fill(&e, "a", 0, 5);
    let r = Rollup::new(Selector::new("cpu"), 60_000).with_aggs(&[RollupAgg::Sum]);
    r.run(&e, 5 * 60_000).unwrap();
    let k = rollup_key(&key("a"), 60_000, RollupAgg::Sum);
    let before = vals(&e, &k);
    assert_eq!(before.len(), 5);

    // same input again: only the newest window is recomputed, output unchanged
    assert_eq!(r.run(&e, 5 * 60_000).unwrap(), 1);
    assert_eq!(vals(&e, &k), before);

    // more data arrives; only the new windows (plus the recomputed edge) are written
    fill(&e, "a", 5, 3);
    assert_eq!(r.run(&e, 8 * 60_000).unwrap(), 4);
    let after = vals(&e, &k);
    assert_eq!(after.len(), 8);
    assert_eq!(&after[..5], &before[..]);

    // nothing complete beyond the last window -> nothing to do
    assert_eq!(r.run(&e, 8 * 60_000 - 1).unwrap(), 0);
    drop(e);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn late_point_in_newest_window_is_picked_up() {
    let (e, dir) = open("late", None);
    fill(&e, "a", 0, 3);
    let r = Rollup::new(Selector::new("cpu"), 60_000).with_aggs(&[RollupAgg::Max]);
    r.run(&e, 3 * 60_000).unwrap();
    let k = rollup_key(&key("a"), 60_000, RollupAgg::Max);
    assert_eq!(vals(&e, &k)[2], (120_000, 170.0));

    e.append(&key("a"), 125_000, 9999.0).unwrap(); // late, inside the last rolled window
    r.run(&e, 3 * 60_000).unwrap();
    assert_eq!(vals(&e, &k)[2], (120_000, 9999.0));
    drop(e);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn many_series_and_selectors() {
    let (e, dir) = open("multi", None);
    for h in ["a", "b", "c"] {
        fill(&e, h, 0, 4);
    }
    e.append(&SeriesKey::new("mem").label("host", "a"), 0, 1.0)
        .unwrap();

    let r = Rollup::new(Selector::new("cpu").ne("host", "c"), 120_000).with_aggs(&[RollupAgg::Avg]);
    assert_eq!(r.run(&e, 4 * 60_000).unwrap(), 2 * 2);
    assert!(e
        .bounds(&rollup_key(&key("c"), 120_000, RollupAgg::Avg))
        .is_none());
    assert!(e.select(&Selector::new("mem:2m:avg")).is_empty());

    // and the rolled series is queryable like any other
    let v = query::run(&e, "max(cpu:2m:avg)", &Ctx::instant(0, 240_000)).unwrap();
    let Value::Vector(rows) = v else { panic!() };
    assert_eq!(rows.len(), 1);
    drop(e);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn rollups_outlive_raw_data() {
    // keep 5 minutes of raw data, but the rollups were written before it expired
    let (e, dir) = open("ret", Some(300_000));
    fill(&e, "a", 0, 4);
    let r = Rollup::new(Selector::new("cpu"), 60_000).with_aggs(&[RollupAgg::Count]);
    r.run(&e, 4 * 60_000).unwrap();
    fill(&e, "a", 4, 40); // pushes the old buckets out of the retention window
    e.flush().unwrap();
    std::thread::sleep(Duration::from_millis(100));

    let raw_oldest = e.bounds(&key("a")).unwrap().0;
    assert!(
        raw_oldest > 0,
        "raw data should have been expired, oldest = {raw_oldest}"
    );
    let rolled = vals(&e, &rollup_key(&key("a"), 60_000, RollupAgg::Count));
    assert_eq!(rolled.len(), 4);
    drop(e);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn bad_params() {
    let (e, dir) = open("bad", None);
    let sel = Selector::new("cpu");
    assert!(matches!(
        Rollup::new(sel.clone(), 0).run(&e, 10),
        Err(Error::BadConfig(_))
    ));
    assert!(matches!(
        Rollup::new(sel.clone(), -5).run(&e, 10),
        Err(Error::BadConfig(_))
    ));
    assert!(matches!(
        Rollup::new(sel, 10).with_aggs(&[]).run(&e, 10),
        Err(Error::BadConfig(_))
    ));
    drop(e);
    let _ = std::fs::remove_dir_all(dir);
}
