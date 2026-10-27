use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use crate::chunk::{ActiveChunk, SealedChunk};
use crate::config::Config;
use crate::error::{Error, Result};
use crate::model::{Aggregate, Point, SeriesId, SeriesKey};

pub fn bucket_of(ts: i64, width: i64) -> i64 {
    ts.div_euclid(width).saturating_mul(width)
}

pub struct Series {
    pub id: SeriesId,
    pub key: SeriesKey,
    pub state: RwLock<State>,
}

impl Series {
    pub fn new(key: SeriesKey) -> Self {
        Series {
            id: key.id(),
            key,
            state: RwLock::new(State::default()),
        }
    }
}

// Lock order (whole engine): maintenance -> catalog -> series state -> wal.
// Nothing in here blocks on IO while holding the state lock except the wal
// append in Engine::append_batch, which is the durability point.
#[derive(Default)]
pub struct State {
    pub active: Option<ActiveChunk>,
    // immutable raw chunks waiting for the background sealer, oldest first
    pub frozen: Vec<Arc<ActiveChunk>>,
    // bucket start -> chunks sorted by min_ts. usually 1 per bucket after compaction
    pub sealed: BTreeMap<i64, Vec<Arc<SealedChunk>>>,
    pub last_ts: Option<i64>,
}

pub struct Snapshot {
    pub sealed: Vec<Arc<SealedChunk>>,
    pub mem: Vec<Point>,
    // chunks fully inside the range, answered from stats alone (aggregate path only)
    pub covered: Aggregate,
}

impl State {
    pub fn validate(&self, pts: &[Point]) -> Result<()> {
        let mut prev = self.last_ts;
        for p in pts {
            if !p.value.is_finite() {
                return Err(Error::BadValue(p.value));
            }
            if let Some(last) = prev {
                if p.ts <= last {
                    return Err(Error::OutOfOrder { last, got: p.ts });
                }
            }
            prev = Some(p.ts);
        }
        Ok(())
    }

    // true if a chunk got frozen, i.e. the sealer has work
    pub fn apply(&mut self, pts: &[Point], cfg: &Config, ids: &AtomicU64) -> bool {
        let mut froze = false;
        for &p in pts {
            let b = bucket_of(p.ts, cfg.bucket_width);
            let roll = match &self.active {
                Some(a) => a.bucket != b || a.len() >= cfg.max_points_per_chunk,
                None => false,
            };
            if roll {
                self.freeze_active();
                froze = true;
            }
            let a = self
                .active
                .get_or_insert_with(|| ActiveChunk::new(ids.fetch_add(1, Ordering::Relaxed), b));
            a.push(p);
            self.last_ts = Some(p.ts);
        }
        froze
    }

    pub fn freeze_active(&mut self) {
        if let Some(a) = self.active.take() {
            self.frozen.push(Arc::new(a));
        }
    }

    pub fn is_drained(&self) -> bool {
        self.active.is_none() && self.frozen.is_empty()
    }

    pub fn insert_sealed(&mut self, c: Arc<SealedChunk>) {
        let v = self.sealed.entry(c.bucket).or_default();
        let at = v.partition_point(|x| x.stats.min_ts < c.stats.min_ts);
        v.insert(at, c);
        // keep last_ts honest after recovery
        let max = v.iter().map(|x| x.stats.max_ts).max();
        if let Some(m) = max {
            self.last_ts = Some(self.last_ts.map_or(m, |l| l.max(m)));
        }
    }

    pub fn snapshot(&self, start: i64, end: i64, width: i64, need_points: bool) -> Snapshot {
        let mut snap = Snapshot {
            sealed: Vec::new(),
            mem: Vec::new(),
            covered: Aggregate::EMPTY,
        };
        if start >= end {
            return snap;
        }

        for (_, chunks) in self.sealed.range(bucket_of(start, width)..end) {
            for c in chunks.iter().filter(|c| c.stats.overlaps(start, end)) {
                if !need_points && c.stats.covered_by(start, end) {
                    snap.covered.merge(&c.stats.agg);
                } else {
                    snap.sealed.push(c.clone());
                }
            }
        }

        let mem_chunks = self.frozen.iter().map(|f| &**f).chain(self.active.iter());
        for c in mem_chunks {
            if c.len() == 0 || !c.stats().overlaps(start, end) {
                continue;
            }
            if !need_points && c.stats().covered_by(start, end) {
                snap.covered.merge(&c.stats().agg);
            } else {
                c.collect_range(start, end, &mut snap.mem);
            }
        }
        snap
    }
}
