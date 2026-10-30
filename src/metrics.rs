use std::fmt::Write;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::engine::EngineStats;

// all relaxed: these are counters for humans and dashboards, nothing synchronises on them
#[derive(Default)]
pub(crate) struct Counters {
    pub append_calls: AtomicU64,
    pub points_appended: AtomicU64,
    pub late_points: AtomicU64,
    pub chunks_sealed: AtomicU64,
    pub ooo_merges: AtomicU64,
    pub compactions: AtomicU64,
    pub chunks_expired: AtomicU64,
    pub maintenance_runs: AtomicU64,
    pub maintenance_errors: AtomicU64,
    pub queries: AtomicU64,
    pub snapshots: AtomicU64,
}

pub(crate) fn inc(c: &AtomicU64, n: u64) {
    c.fetch_add(n, Ordering::Relaxed);
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Metrics {
    pub append_calls: u64,
    pub points_appended: u64,
    pub late_points: u64,
    pub chunks_sealed: u64,
    pub ooo_merges: u64,
    pub compactions: u64,
    pub chunks_expired: u64,
    pub maintenance_runs: u64,
    pub maintenance_errors: u64,
    pub queries: u64,
    pub snapshots: u64,
}

impl Counters {
    pub fn read(&self) -> Metrics {
        let g = |a: &AtomicU64| a.load(Ordering::Relaxed);
        Metrics {
            append_calls: g(&self.append_calls),
            points_appended: g(&self.points_appended),
            late_points: g(&self.late_points),
            chunks_sealed: g(&self.chunks_sealed),
            ooo_merges: g(&self.ooo_merges),
            compactions: g(&self.compactions),
            chunks_expired: g(&self.chunks_expired),
            maintenance_runs: g(&self.maintenance_runs),
            maintenance_errors: g(&self.maintenance_errors),
            queries: g(&self.queries),
            snapshots: g(&self.snapshots),
        }
    }
}

impl Metrics {
    // prometheus text exposition: counters from here, gauges from the stats snapshot
    pub fn render(&self, st: &EngineStats) -> String {
        let mut out = String::new();
        let mut put = |name: &str, kind: &str, help: &str, v: u64| {
            let _ = writeln!(out, "# HELP tsdb_{name} {help}");
            let _ = writeln!(out, "# TYPE tsdb_{name} {kind}");
            let _ = writeln!(out, "tsdb_{name} {v}");
        };
        put("append_calls_total", "counter", "append / append_batch calls", self.append_calls);
        put("points_appended_total", "counter", "points accepted", self.points_appended);
        put("late_points_total", "counter", "points that arrived out of order", self.late_points);
        put("chunks_sealed_total", "counter", "frozen chunks written to disk", self.chunks_sealed);
        put("ooo_merges_total", "counter", "buckets rewritten to fold in late points", self.ooo_merges);
        put("compactions_total", "counter", "buckets merged into one chunk", self.compactions);
        put("chunks_expired_total", "counter", "chunks dropped by retention", self.chunks_expired);
        put("maintenance_runs_total", "counter", "background passes", self.maintenance_runs);
        put("maintenance_errors_total", "counter", "background passes that failed", self.maintenance_errors);
        put("queries_total", "counter", "range / aggregate queries", self.queries);
        put("snapshots_total", "counter", "snapshots taken", self.snapshots);
        put("series", "gauge", "series in the catalog", st.series as u64);
        put("sealed_chunks", "gauge", "chunks on disk", st.sealed_chunks as u64);
        put("sealed_points", "gauge", "points in sealed chunks", st.sealed_points);
        put("compressed_bytes", "gauge", "bytes of sealed ts+value columns", st.compressed_bytes);
        put("frozen_chunks", "gauge", "chunks waiting for the sealer", st.frozen_chunks as u64);
        put("ooo_points", "gauge", "late points buffered", st.ooo_points as u64);
        put("wal_bytes", "gauge", "bytes in the write-ahead log", st.wal_bytes);
        put("cache_bytes", "gauge", "decoded block cache size", st.cache_bytes as u64);
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_read_back() {
        let c = Counters::default();
        inc(&c.points_appended, 5);
        inc(&c.points_appended, 2);
        inc(&c.compactions, 1);
        let m = c.read();
        assert_eq!((m.points_appended, m.compactions, m.queries), (7, 1, 0));
    }

    #[test]
    fn render_is_well_formed() {
        let m = Metrics { points_appended: 12, ..Default::default() };
        let st = EngineStats { series: 3, wal_bytes: 99, ..Default::default() };
        let text = m.render(&st);
        assert!(text.contains("tsdb_points_appended_total 12\n"));
        assert!(text.contains("tsdb_series 3\n"));
        assert!(text.contains("tsdb_wal_bytes 99\n"));
        // every sample line is preceded by its HELP and TYPE
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len() % 3, 0);
        for c in lines.chunks(3) {
            let name = c[2].split(' ').next().unwrap();
            assert_eq!(c[0], format!("# HELP {name} {}", c[0].splitn(4, ' ').nth(3).unwrap()));
            assert!(c[1].starts_with(&format!("# TYPE {name} ")));
        }
    }
}
