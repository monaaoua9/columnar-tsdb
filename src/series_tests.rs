use std::sync::atomic::AtomicU64;

use crate::chunk::ActiveChunk;
use crate::config::Config;
use crate::model::Point;
use crate::series::State;

fn cfg() -> Config {
    let mut c = Config::new("unused");
    c.bucket_width = 100;
    c.max_points_per_chunk = 10;
    c
}

fn filled() -> State {
    let c = cfg();
    let ids = AtomicU64::new(1);
    let mut st = State::default();
    let pts: Vec<Point> = (0..35).map(|i| Point::new(i * 10, i as f64)).collect();
    let late = vec![false; pts.len()];
    st.apply(&pts, &late, &c, &ids);
    // plus two late points sitting in the buffer
    st.apply(
        &[Point::new(55, 1.0), Point::new(205, 2.0)],
        &[true, true],
        &c,
        &ids,
    );
    st
}

fn live_ts(st: &State) -> Vec<i64> {
    let snap = st.snapshot(i64::MIN, i64::MAX, 100, true);
    let mut v: Vec<i64> = snap.mem.iter().chain(&snap.ooo).map(|p| p.ts).collect();
    v.sort_unstable();
    v
}

#[test]
fn cut_memory_removes_exactly_the_range() {
    let mut st = filled();
    assert!(!st.frozen.is_empty() && st.active.is_some());
    let before = live_ts(&st);
    assert_eq!(before.len(), 37);

    // [50, 210) is half-open: 50..=200 in steps of 10 = 16 points, plus late 55 and 205
    let gone = st.cut_memory(50, 210);
    assert_eq!(gone, 18);
    let want: Vec<i64> = before
        .into_iter()
        .filter(|&t| !(50..210).contains(&t))
        .collect();
    assert_eq!(live_ts(&st), want);
    assert!(st.frozen.iter().all(|f| f.len() > 0));
    // chunk stats must describe what is left, not what was cut
    for f in &st.frozen {
        let s = f.stats();
        let (ts, _) = f.columns();
        assert_eq!((s.min_ts, s.max_ts), (ts[0], ts[ts.len() - 1]));
    }
}

#[test]
fn cut_everything_and_nothing() {
    let mut st = filled();
    assert_eq!(st.cut_memory(1000, 2000), 0);
    assert_eq!(live_ts(&st).len(), 37);
    assert_eq!(st.cut_memory(5, 5), 0);
    assert_eq!(st.cut_memory(i64::MIN, i64::MAX), 37);
    assert!(st.is_drained());
    // last_ts is deliberately kept: later writes at old timestamps are still "late"
    assert_eq!(st.last_ts, Some(340));
    assert_eq!(st.bounds(), None);
}

#[test]
fn without_range_rebuilds_the_aggregate() {
    let mut a = ActiveChunk::new(3, 0);
    for i in 0..10 {
        a.push(Point::new(i, i as f64));
    }
    let c = a.without_range(2, 8);
    assert_eq!(c.len(), 4);
    assert_eq!((c.id, c.bucket), (3, 0));
    let s = c.stats();
    assert_eq!(
        (s.agg.count, s.agg.sum, s.agg.min, s.agg.max),
        (4, 0.0 + 1.0 + 8.0 + 9.0, 0.0, 9.0)
    );
    assert_eq!(a.without_range(0, 10).len(), 0);
}
