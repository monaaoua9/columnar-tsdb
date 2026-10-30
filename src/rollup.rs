//! Rollups: coarse aggregates written back as ordinary series, so old raw data can expire
//! (`Config::retention`) while cheap long-range history stays queryable.
//!
//! `cpu{host="a"}` rolled up at 5m becomes `cpu:5m:avg{host="a"}`, `cpu:5m:max{..}`, ... with
//! one point per window at the window start. Windows are aligned to multiples of `step`, and
//! only windows that are completely in the past (`end <= upto`) are written.
//!
//! The newest rolled window is recomputed on every run, which picks up late points and makes
//! re-running harmless.

use crate::engine::Engine;
use crate::error::{Error, Result};
use crate::model::{Aggregate, Point, Selector, SeriesKey};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RollupAgg {
    Avg,
    Min,
    Max,
    Sum,
    Count,
}

impl RollupAgg {
    pub const ALL: [RollupAgg; 5] = [Self::Avg, Self::Min, Self::Max, Self::Sum, Self::Count];

    pub fn name(&self) -> &'static str {
        match self {
            Self::Avg => "avg",
            Self::Min => "min",
            Self::Max => "max",
            Self::Sum => "sum",
            Self::Count => "count",
        }
    }

    fn pick(&self, a: &Aggregate) -> f64 {
        match self {
            Self::Avg => a.sum / a.count as f64,
            Self::Min => a.min,
            Self::Max => a.max,
            Self::Sum => a.sum,
            Self::Count => a.count as f64,
        }
    }
}

// 300000 -> "5m"; odd sizes fall back to plain ms
pub fn step_label(step: i64) -> String {
    for (unit, ms) in [
        ("w", 604_800_000),
        ("d", 86_400_000),
        ("h", 3_600_000),
        ("m", 60_000),
        ("s", 1000),
    ] {
        if step % ms == 0 {
            return format!("{}{unit}", step / ms);
        }
    }
    format!("{step}ms")
}

pub fn rollup_key(src: &SeriesKey, step: i64, agg: RollupAgg) -> SeriesKey {
    let mut k = SeriesKey::new(format!(
        "{}:{}:{}",
        src.name(),
        step_label(step),
        agg.name()
    ));
    for (a, b) in src.labels() {
        k = k.label(a, b);
    }
    k
}

#[derive(Clone, Debug)]
pub struct Rollup {
    pub source: Selector,
    pub step: i64,
    pub aggs: Vec<RollupAgg>,
}

impl Rollup {
    pub fn new(source: Selector, step: i64) -> Rollup {
        Rollup {
            source,
            step,
            aggs: RollupAgg::ALL.to_vec(),
        }
    }

    pub fn with_aggs(mut self, aggs: &[RollupAgg]) -> Rollup {
        self.aggs = aggs.to_vec();
        self
    }

    // Rolls every complete window before `upto` for each matching series. Returns how many
    // points were written (windows x aggs, summed over series).
    pub fn run(&self, engine: &Engine, upto: i64) -> Result<usize> {
        if self.step <= 0 {
            return Err(Error::BadConfig("rollup step must be positive"));
        }
        if self.aggs.is_empty() {
            return Err(Error::BadConfig("rollup needs at least one aggregate"));
        }
        let end = upto.div_euclid(self.step) * self.step;
        let mut written = 0;

        for key in engine.select(&self.source) {
            let Some((first, _)) = engine.bounds(&key) else {
                continue;
            };
            // resume at the newest window already rolled (any agg will do, they move together)
            let done = engine
                .bounds(&rollup_key(&key, self.step, self.aggs[0]))
                .map(|b| b.1);
            let start = match done {
                Some(last) => last.max(first.div_euclid(self.step) * self.step),
                None => first.div_euclid(self.step) * self.step,
            };
            if start >= end {
                continue;
            }

            let wins = engine.aggregate_windows(&key, start, end, self.step)?;
            for agg in &self.aggs {
                let pts: Vec<Point> = wins
                    .iter()
                    .map(|(w, a)| Point::new(*w, agg.pick(a)))
                    .collect();
                if pts.is_empty() {
                    continue;
                }
                engine.append_batch(&rollup_key(&key, self.step, *agg), &pts)?;
                written += pts.len();
            }
        }
        Ok(written)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn labels() {
        assert_eq!(step_label(300_000), "5m");
        assert_eq!(step_label(3_600_000), "1h");
        assert_eq!(step_label(90_000), "90s");
        assert_eq!(step_label(86_400_000 * 7), "1w");
        assert_eq!(step_label(1500), "1500ms");
        assert_eq!(step_label(1), "1ms");
    }

    #[test]
    fn key_keeps_labels() {
        let src = SeriesKey::new("cpu").label("host", "a").label("dc", "eu");
        let k = rollup_key(&src, 60_000, RollupAgg::Max);
        assert_eq!(k.name(), "cpu:1m:max");
        assert_eq!(k.labels(), src.labels());
        assert_ne!(k.id(), rollup_key(&src, 60_000, RollupAgg::Min).id());
    }
}
