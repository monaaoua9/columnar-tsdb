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

// both inputs ascending by ts; on equal ts the `over` value wins
pub fn merge_override(base: Vec<Point>, over: &[Point]) -> Vec<Point> {
    if over.is_empty() {
        return base;
    }
    let mut out = Vec::with_capacity(base.len() + over.len());
    let (mut i, mut j) = (0, 0);
    while i < base.len() && j < over.len() {
        if base[i].ts < over[j].ts {
            out.push(base[i]);
            i += 1;
        } else if base[i].ts > over[j].ts {
            out.push(over[j]);
            j += 1;
        } else {
            out.push(over[j]);
            i += 1;
            j += 1;
        }
    }
    out.extend_from_slice(&base[i..]);
    out.extend_from_slice(&over[j..]);
    out
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
    // highest in-order timestamp seen; anything <= this is a late write
    pub last_ts: Option<i64>,
    // late writes not yet merged into sealed chunks. a ts here beats the same ts anywhere else
    pub ooo: BTreeMap<i64, f64>,
}

pub struct Snapshot {
    pub sealed: Vec<Arc<SealedChunk>>,
    pub mem: Vec<Point>,
    pub ooo: Vec<Point>,
    // chunks fully inside the range, answered from stats alone (aggregate path only)
    pub covered: Aggregate,
}

impl State {
    // Validates the whole batch and says which points are late (true). Nothing is applied,
    // so a bad batch leaves no prefix behind.
    pub fn plan(&self, pts: &[Point], retention: Option<i64>) -> Result<Vec<bool>> {
        let mut last = self.last_ts;
        let mut late = Vec::with_capacity(pts.len());
        for p in pts {
            if !p.value.is_finite() {
                return Err(Error::BadValue(p.value));
            }
            let is_late = last.is_some_and(|l| p.ts <= l);
            if is_late {
                if let (Some(l), Some(r)) = (last, retention) {
                    let cutoff = l.saturating_sub(r);
                    if p.ts < cutoff {
                        return Err(Error::TooOld { cutoff, got: p.ts });
                    }
                }
            } else {
                last = Some(p.ts);
            }
            late.push(is_late);
        }
        Ok(late)
    }

    // true if the maintenance thread should look at this series soon
    pub fn apply(&mut self, pts: &[Point], late: &[bool], cfg: &Config, ids: &AtomicU64) -> bool {
        let mut froze = false;
        for (&p, &is_late) in pts.iter().zip(late) {
            if is_late {
                self.ooo.insert(p.ts, p.value);
                continue;
            }
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
        froze || self.ooo.len() > cfg.max_ooo_points
    }

    // smallest ts still held in raw memory chunks. late points below it can be merged into
    // sealed chunks without overlapping anything
    pub fn mem_min(&self) -> Option<i64> {
        self.frozen
            .first()
            .map(|f| f.stats().min_ts)
            .or_else(|| self.active.as_ref().map(|a| a.stats().min_ts))
    }

    pub fn freeze_active(&mut self) {
        if let Some(a) = self.active.take() {
            self.frozen.push(Arc::new(a));
        }
    }

    pub fn is_drained(&self) -> bool {
        self.active.is_none() && self.frozen.is_empty() && self.ooo.is_empty()
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
            ooo: Vec::new(),
            covered: Aggregate::EMPTY,
        };
        if start >= end {
            return snap;
        }
        snap.ooo = self
            .ooo
            .range(start..end)
            .map(|(&t, &v)| Point::new(t, v))
            .collect();
        // stored stats can't be trusted for a chunk with a late point inside its span: the
        // point may add to it or overwrite one of its values
        let dirty = |lo: i64, hi: i64| self.ooo.range(lo..=hi).next().is_some();

        for (_, chunks) in self.sealed.range(bucket_of(start, width)..end) {
            for c in chunks.iter().filter(|c| c.stats.overlaps(start, end)) {
                if !need_points
                    && c.stats.covered_by(start, end)
                    && !dirty(c.stats.min_ts, c.stats.max_ts)
                {
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
            let st = c.stats();
            if !need_points && st.covered_by(start, end) && !dirty(st.min_ts, st.max_ts) {
                snap.covered.merge(&st.agg);
            } else {
                c.collect_range(start, end, &mut snap.mem);
            }
        }
        snap
    }
}
