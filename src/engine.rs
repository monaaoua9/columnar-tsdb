use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::catalog;
use crate::chunk::{self, BlockCache, SealedChunk};
use crate::config::{Config, SyncMode};
use crate::error::{Error, Result};
use crate::index::LabelIndex;
use crate::metrics::{inc, Counters, Metrics};
use crate::model::{Aggregate, Point, Selector, SeriesId, SeriesKey};
use crate::series::{bucket_of, merge_override, Series};
use crate::snapshot::{self, SnapshotInfo};
use crate::wal::Wal;

#[derive(Clone, Debug, Default)]
pub struct EngineStats {
    pub series: usize,
    pub sealed_chunks: usize,
    pub sealed_points: u64,
    pub compressed_bytes: u64,
    pub frozen_chunks: usize,
    pub ooo_points: usize,
    pub wal_bytes: u64,
    pub cache_bytes: usize,
    pub last_maintenance_error: Option<String>,
}

struct Signal {
    shutdown: bool,
    kick: bool,
}

struct Inner {
    cfg: Config,
    chunk_dir: PathBuf,
    catalog: RwLock<HashMap<SeriesId, Arc<Series>>>,
    // written only while the catalog write lock is held, so an id found here is always in the
    // catalog too
    index: RwLock<LabelIndex>,
    cat_file: Mutex<File>,
    wal: Vec<Mutex<Wal>>,
    next_id: AtomicU64,
    cache: Arc<BlockCache>,
    // serialises sealing / compaction / checkpoint. only the worker and flush() take it
    maintenance: Mutex<()>,
    signal: Mutex<Signal>,
    wake: Condvar,
    last_err: Mutex<Option<String>>,
    m: Counters,
    _dir_lock: File,
}

struct Guard {
    inner: Arc<Inner>,
    worker: Mutex<Option<JoinHandle<()>>>,
}

#[derive(Clone)]
pub struct Engine {
    inner: Arc<Inner>,
    _guard: Arc<Guard>,
}

impl Engine {
    pub fn open(cfg: Config) -> Result<Engine> {
        let inner = Inner::open(cfg)?;
        let worker_inner = inner.clone();
        let handle = thread::Builder::new()
            .name("tsdb-maintenance".into())
            .spawn(move || worker_loop(worker_inner))?;
        let guard = Arc::new(Guard {
            inner: inner.clone(),
            worker: Mutex::new(Some(handle)),
        });
        Ok(Engine {
            inner,
            _guard: guard,
        })
    }

    pub fn append(&self, key: &SeriesKey, ts: i64, value: f64) -> Result<()> {
        self.append_batch(key, &[Point::new(ts, value)])
    }

    // All-or-nothing: the batch is validated before anything is logged.
    // Any order is accepted. A point with ts <= the series' newest ts is a late write: it is
    // buffered, wins over an existing point with the same ts, and is merged into the sealed
    // chunk of its bucket by the maintenance thread.
    pub fn append_batch(&self, key: &SeriesKey, pts: &[Point]) -> Result<()> {
        if pts.is_empty() {
            return Ok(());
        }
        let s = self.inner.series_for(key)?;
        self.inner.throttle(&s)?;
        let mut st = s.state.write().unwrap();
        let late = st.plan(pts, self.inner.cfg.retention)?;
        // wal under the series lock so log order == apply order for this series
        self.inner
            .wal_for(s.id)
            .lock()
            .unwrap()
            .append(s.id, pts, &late)?;
        let busy = st.apply(pts, &late, &self.inner.cfg, &self.inner.next_id);
        drop(st);
        let m = &self.inner.m;
        inc(&m.append_calls, 1);
        inc(&m.points_appended, pts.len() as u64);
        inc(&m.late_points, late.iter().filter(|&&l| l).count() as u64);
        if busy {
            self.inner.kick();
        }
        Ok(())
    }

    // points with start <= ts < end, ascending
    pub fn query_range(&self, key: &SeriesKey, start: i64, end: i64) -> Result<Vec<Point>> {
        inc(&self.inner.m.queries, 1);
        match self.inner.find(key) {
            Some(s) => self.inner.points(&s, start, end),
            None => Ok(Vec::new()),
        }
    }

    // chunks fully inside [start, end) are answered from their stored stats
    pub fn aggregate(&self, key: &SeriesKey, start: i64, end: i64) -> Result<Option<Aggregate>> {
        inc(&self.inner.m.queries, 1);
        match self.inner.find(key) {
            Some(s) => Ok(self.inner.agg(&s, start, end)?.filter(|a| a.count > 0)),
            None => Ok(None),
        }
    }

    // keys of all series the selector matches, ordered by series id
    pub fn select(&self, sel: &Selector) -> Vec<SeriesKey> {
        self.inner
            .resolve(sel)
            .into_iter()
            .map(|s| s.key.clone())
            .collect()
    }

    // every series in the catalog, ordered by id
    pub fn list_series(&self) -> Vec<SeriesKey> {
        self.inner.series_list().iter().map(|s| s.key.clone()).collect()
    }

    // distinct metric names, sorted
    pub fn metric_names(&self) -> Vec<String> {
        let mut v: Vec<String> = self.list_series().iter().map(|k| k.name().to_string()).collect();
        v.sort();
        v.dedup();
        v
    }

    // raw points of every matching series; series with nothing in range are left out
    pub fn query_select(
        &self,
        sel: &Selector,
        start: i64,
        end: i64,
    ) -> Result<Vec<(SeriesKey, Vec<Point>)>> {
        inc(&self.inner.m.queries, 1);
        let mut out = Vec::new();
        for s in self.inner.resolve(sel) {
            let pts = self.inner.points(&s, start, end)?;
            if !pts.is_empty() {
                out.push((s.key.clone(), pts));
            }
        }
        Ok(out)
    }

    // one aggregate over all matching series (each still answered from chunk stats)
    pub fn aggregate_select(
        &self,
        sel: &Selector,
        start: i64,
        end: i64,
    ) -> Result<Option<Aggregate>> {
        let mut total = Aggregate::EMPTY;
        for s in self.inner.resolve(sel) {
            if let Some(a) = self.inner.agg(&s, start, end)? {
                total.merge(&a);
            }
        }
        Ok((total.count > 0).then_some(total))
    }

    // like GROUP BY label: one aggregate per value of `label`. series without the label are
    // grouped under the empty string
    pub fn aggregate_by(
        &self,
        sel: &Selector,
        label: &str,
        start: i64,
        end: i64,
    ) -> Result<BTreeMap<String, Aggregate>> {
        let mut groups: BTreeMap<String, Aggregate> = BTreeMap::new();
        for s in self.inner.resolve(sel) {
            let Some(a) = self.inner.agg(&s, start, end)? else {
                continue;
            };
            let v = s.key.labels().get(label).cloned().unwrap_or_default();
            groups.entry(v).or_insert(Aggregate::EMPTY).merge(&a);
        }
        Ok(groups)
    }

    // Downsampling: one aggregate per `step`-wide window over [start, end), windows aligned to
    // `start`, empty windows left out. A window that covers whole chunks is answered from their
    // stats, so a coarse step over a long range stays cheap.
    pub fn aggregate_windows(
        &self,
        key: &SeriesKey,
        start: i64,
        end: i64,
        step: i64,
    ) -> Result<Vec<(i64, Aggregate)>> {
        if step <= 0 {
            return Err(Error::BadConfig("step must be positive"));
        }
        match self.inner.find(key) {
            Some(s) => self.inner.windows(&s, start, end, step),
            None => Ok(Vec::new()),
        }
    }

    // same, merged across every series the selector matches
    pub fn aggregate_windows_select(
        &self,
        sel: &Selector,
        start: i64,
        end: i64,
        step: i64,
    ) -> Result<Vec<(i64, Aggregate)>> {
        if step <= 0 {
            return Err(Error::BadConfig("step must be positive"));
        }
        let mut merged: BTreeMap<i64, Aggregate> = BTreeMap::new();
        for s in self.inner.resolve(sel) {
            for (w, a) in self.inner.windows(&s, start, end, step)? {
                merged.entry(w).or_insert(Aggregate::EMPTY).merge(&a);
            }
        }
        Ok(merged.into_iter().collect())
    }

    // seals everything in memory and, if nothing raced in, truncates the wal
    pub fn flush(&self) -> Result<()> {
        self.inner.flush()
    }

    // merges the sealed chunks of every bucket (open one included) into one chunk each;
    // returns how many buckets were merged. the background thread does the closed-bucket
    // subset of this on its own
    pub fn compact(&self) -> Result<usize> {
        let _m = self.inner.maintenance.lock().unwrap();
        let series = self.inner.series_list();
        let mut n = 0;
        for s in &series {
            self.inner.seal_pending(s)?;
            self.inner.merge_ooo(s)?;
            n += self.inner.compact_series(s, true)?;
        }
        Ok(n)
    }

    // Consistent copy of the database into an empty dir while reads and writes keep going.
    // Sealed chunks are hard-linked (they are immutable), wal and catalog are copied. Open the
    // result with Engine::open, or snapshot::restore it somewhere else.
    pub fn snapshot(&self, dest: impl AsRef<Path>) -> Result<SnapshotInfo> {
        self.inner.snapshot(dest.as_ref())
    }

    pub fn metrics(&self) -> Metrics {
        self.inner.m.read()
    }

    // prometheus text for metrics() + stats()
    pub fn render_metrics(&self) -> String {
        self.metrics().render(&self.stats())
    }

    pub fn stats(&self) -> EngineStats {
        let series = self.inner.series_list();
        let mut st = EngineStats {
            series: series.len(),
            ..Default::default()
        };
        for s in &series {
            let g = s.state.read().unwrap();
            st.frozen_chunks += g.frozen.len();
            st.ooo_points += g.ooo.len();
            for c in g.sealed.values().flatten() {
                st.sealed_chunks += 1;
                st.sealed_points += c.stats.agg.count;
                st.compressed_bytes += c.ts_bytes + c.val_bytes;
            }
        }
        st.wal_bytes = self.inner.wal_bytes();
        st.cache_bytes = self.inner.cache.bytes();
        st.last_maintenance_error = self.inner.last_err.lock().unwrap().clone();
        st
    }
}

impl Inner {
    fn open(cfg: Config) -> Result<Arc<Inner>> {
        cfg.validate()?;
        let chunk_dir = cfg.dir.join("chunks");
        fs::create_dir_all(&chunk_dir)?;

        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(cfg.dir.join("LOCK"))?;
        match lock.try_lock() {
            Ok(()) => {}
            Err(fs::TryLockError::WouldBlock) => return Err(Error::Locked),
            Err(fs::TryLockError::Error(e)) => return Err(e.into()),
        }

        let (cat_file, keys) = catalog::open(&cfg.dir.join("series.cat"))?;
        let mut map: HashMap<SeriesId, Arc<Series>> = HashMap::new();
        let mut index = LabelIndex::default();
        for k in keys {
            let s = Series::new(k);
            index.insert(&s.key, s.id);
            map.insert(s.id, Arc::new(s));
        }

        let mut max_id = 0;
        let cache = Arc::new(BlockCache::new(cfg.block_cache_bytes));
        for c in chunk::scan(&chunk_dir)? {
            max_id = max_id.max(c.id);
            if let Some(s) = map.get(&c.series) {
                s.state
                    .write()
                    .unwrap()
                    .insert_sealed(Arc::new(c.with_cache(cache.clone())));
            }
        }

        // a series lives in exactly one shard, so replaying shard by shard keeps its write order
        let mut wal = Vec::with_capacity(cfg.wal_shards);
        let mut recs = Vec::new();
        for i in 0..cfg.wal_shards {
            let (w, r) = Wal::open(&cfg.dir.join(format!("wal-{i}.log")), cfg.sync)?;
            wal.push(Mutex::new(w));
            recs.extend(r);
        }
        // files left by a run with more shards: refuse if they hold data, else tidy up
        for entry in fs::read_dir(&cfg.dir)? {
            let path = entry?.path();
            let idx = path
                .file_name()
                .and_then(|n| n.to_str())
                .and_then(|n| n.strip_prefix("wal-"))
                .and_then(|n| n.strip_suffix(".log"))
                .and_then(|n| n.parse::<usize>().ok());
            if idx.is_some_and(|i| i >= cfg.wal_shards) {
                if fs::metadata(&path)?.len() > 0 {
                    return Err(Error::BadConfig(
                        "wal files from a run with more wal_shards still hold data; reopen with the old value and flush",
                    ));
                }
                fs::remove_file(path)?;
            }
        }
        let inner = Arc::new(Inner {
            cfg,
            chunk_dir,
            catalog: RwLock::new(map),
            index: RwLock::new(index),
            cat_file: Mutex::new(cat_file),
            wal,
            next_id: AtomicU64::new(max_id + 1),
            cache,
            maintenance: Mutex::new(()),
            signal: Mutex::new(Signal {
                shutdown: false,
                kick: false,
            }),
            wake: Condvar::new(),
            last_err: Mutex::new(None),
            m: Counters::default(),
            _dir_lock: lock,
        });

        // In-order records: sealed chunks hold a prefix of them, so ts <= last_ts is durable
        // already. Late records can't be judged by ts, so they are always re-applied; that is
        // idempotent (same ts, same value) and the wal replays in write order, so the last
        // write per ts still wins.
        for r in recs {
            let Some(s) = inner.find_id(r.series) else {
                continue;
            };
            let mut st = s.state.write().unwrap();
            if !r.late && st.last_ts.is_some_and(|l| r.point.ts <= l) {
                continue;
            }
            st.apply(&[r.point], &[r.late], &inner.cfg, &inner.next_id);
        }
        Ok(inner)
    }

    fn wal_for(&self, id: SeriesId) -> &Mutex<Wal> {
        &self.wal[(id % self.wal.len() as u64) as usize]
    }

    fn wal_bytes(&self) -> u64 {
        self.wal.iter().map(|w| w.lock().unwrap().size()).sum()
    }

    fn resolve(&self, sel: &Selector) -> Vec<Arc<Series>> {
        let ids = self.index.read().unwrap().resolve(sel);
        let map = self.catalog.read().unwrap();
        ids.iter().filter_map(|id| map.get(id).cloned()).collect()
    }

    fn points(&self, s: &Series, start: i64, end: i64) -> Result<Vec<Point>> {
        let snap = s
            .state
            .read()
            .unwrap()
            .snapshot(start, end, self.cfg.bucket_width, true);

        // decoding happens with no lock held; chunks are immutable
        let mut out = Vec::new();
        for c in &snap.sealed {
            out.extend(c.read_range(start, end)?);
        }
        out.extend(snap.mem);
        Ok(merge_override(out, &snap.ooo))
    }

    fn agg(&self, s: &Series, start: i64, end: i64) -> Result<Option<Aggregate>> {
        let snap = s
            .state
            .read()
            .unwrap()
            .snapshot(start, end, self.cfg.bucket_width, false);

        // only edge chunks (or ones with late points in them) end up here
        let mut pts = Vec::new();
        for c in &snap.sealed {
            pts.extend(c.read_range(start, end)?);
        }
        pts.extend(snap.mem);

        let mut agg = snap.covered;
        for p in merge_override(pts, &snap.ooo) {
            agg.add(p.value);
        }
        Ok((agg.count > 0).then_some(agg))
    }

    fn windows(
        &self,
        s: &Series,
        start: i64,
        end: i64,
        step: i64,
    ) -> Result<Vec<(i64, Aggregate)>> {
        // don't walk empty windows before the first / after the last point of the series
        let Some((lo, hi)) = s.state.read().unwrap().bounds() else {
            return Ok(Vec::new());
        };
        let mut w = start;
        if lo > start {
            let skip = (lo as i128 - start as i128) / step as i128;
            w = (start as i128 + skip * step as i128) as i64;
        }
        let end = end.min(hi.saturating_add(1));

        let mut out = Vec::new();
        while w < end {
            let next = w.saturating_add(step).min(end);
            if let Some(a) = self.agg(s, w, next)? {
                out.push((w, a));
            }
            if next <= w {
                break;
            }
            w = next;
        }
        Ok(out)
    }

    fn find_id(&self, id: SeriesId) -> Option<Arc<Series>> {
        self.catalog.read().unwrap().get(&id).cloned()
    }

    fn find(&self, key: &SeriesKey) -> Option<Arc<Series>> {
        self.find_id(key.id())
    }

    fn series_for(&self, key: &SeriesKey) -> Result<Arc<Series>> {
        let id = key.id();
        let check = |s: Arc<Series>| {
            if s.key == *key {
                Ok(s)
            } else {
                Err(Error::corrupt("series id collision"))
            }
        };
        if let Some(s) = self.find_id(id) {
            return check(s);
        }

        let mut map = self.catalog.write().unwrap();
        if let Some(s) = map.get(&id) {
            return check(s.clone());
        }
        // catalog first, so the wal never mentions an id we can't resolve after a crash
        catalog::append(&mut self.cat_file.lock().unwrap(), key, self.cfg.sync)?;
        let s = Arc::new(Series::new(key.clone()));
        map.insert(id, s.clone());
        self.index.write().unwrap().insert(key, id);
        Ok(s)
    }

    fn series_list(&self) -> Vec<Arc<Series>> {
        let mut v: Vec<_> = self.catalog.read().unwrap().values().cloned().collect();
        v.sort_by_key(|s| s.id);
        v
    }

    fn kick(&self) {
        self.signal.lock().unwrap().kick = true;
        self.wake.notify_one();
    }

    // must run before taking the series write lock: the sealer needs that lock to make progress
    fn throttle(&self, s: &Series) -> Result<()> {
        while s.state.read().unwrap().frozen.len() > self.cfg.max_frozen_chunks {
            // slow disk = wait. broken sealer = waiting would hang forever, so fail instead
            if let Some(msg) = &*self.last_err.lock().unwrap() {
                return Err(io::Error::other(format!("sealer failing: {msg}")).into());
            }
            self.kick();
            thread::sleep(Duration::from_millis(1));
        }
        Ok(())
    }

    fn sync_files(&self) -> bool {
        self.cfg.sync == SyncMode::Always
    }

    // IO happens outside the state lock; the swap frozen -> sealed is one write-lock section
    // so readers see each point exactly once
    fn seal_pending(&self, s: &Series) -> Result<usize> {
        let todo = s.state.read().unwrap().frozen.clone();
        for f in &todo {
            let (ts, vals) = f.columns();
            let sealed = SealedChunk::write(
                &self.chunk_dir,
                s.id,
                f.id,
                f.bucket,
                ts,
                vals,
                self.sync_files(),
            )?;
            let mut st = s.state.write().unwrap();
            st.frozen.retain(|x| x.id != f.id);
            st.insert_sealed(Arc::new(sealed.with_cache(self.cache.clone())));
        }
        inc(&self.m.chunks_sealed, todo.len() as u64);
        Ok(todo.len())
    }

    // Folds buffered late points into the sealed chunks of their buckets (one rewritten chunk
    // per bucket). Points at or above the oldest raw in-memory chunk wait: merging them now
    // would make a sealed chunk overlap data that isn't sealed yet.
    fn merge_ooo(&self, s: &Series) -> Result<usize> {
        type Group = (Vec<Point>, Vec<Arc<SealedChunk>>);
        let mut groups: BTreeMap<i64, Group> = BTreeMap::new();
        {
            let st = s.state.read().unwrap();
            let take: Vec<(i64, f64)> = match st.mem_min() {
                Some(m) => st.ooo.range(..m).map(|(&t, &v)| (t, v)).collect(),
                None => st.ooo.iter().map(|(&t, &v)| (t, v)).collect(),
            };
            for (t, v) in take {
                let b = bucket_of(t, self.cfg.bucket_width);
                groups.entry(b).or_default().0.push(Point::new(t, v));
            }
            for (b, g) in groups.iter_mut() {
                g.1 = st.sealed.get(b).cloned().unwrap_or_default();
            }
        }

        for (bucket, (late, old)) in &groups {
            let mut base = Vec::new();
            for c in old {
                let (t, v) = c.read_all()?;
                base.extend(t.iter().zip(&v).map(|(&t, &v)| Point::new(t, v)));
            }
            let merged = merge_override(base, late);
            let ts: Vec<i64> = merged.iter().map(|p| p.ts).collect();
            let vals: Vec<f64> = merged.iter().map(|p| p.value).collect();

            let id = self.next_id.fetch_add(1, Ordering::Relaxed);
            let chunk = SealedChunk::write(
                &self.chunk_dir,
                s.id,
                id,
                *bucket,
                &ts,
                &vals,
                self.sync_files(),
            )?;

            let mut st = s.state.write().unwrap();
            if let Some(v) = st.sealed.get_mut(bucket) {
                v.retain(|c| !old.iter().any(|g| g.id == c.id));
            }
            st.insert_sealed(Arc::new(chunk.with_cache(self.cache.clone())));
            // a newer write to the same ts may have landed meanwhile; only drop what we flushed
            for p in late {
                if st
                    .ooo
                    .get(&p.ts)
                    .is_some_and(|v| v.to_bits() == p.value.to_bits())
                {
                    st.ooo.remove(&p.ts);
                }
            }
            drop(st);
            for g in old {
                g.mark_obsolete();
            }
        }
        inc(&self.m.ooo_merges, groups.len() as u64);
        Ok(groups.len())
    }

    // background passes only touch closed buckets (series already moved past them); merging
    // the hot bucket would rewrite it again on every freeze
    fn compact_series(&self, s: &Series, include_open: bool) -> Result<usize> {
        let groups: Vec<Vec<Arc<SealedChunk>>> = {
            let st = s.state.read().unwrap();
            let last = st.last_ts.unwrap_or(i64::MIN);
            st.sealed
                .iter()
                .filter(|(b, v)| {
                    v.len() > 1 && (include_open || b.saturating_add(self.cfg.bucket_width) <= last)
                })
                .map(|(_, v)| v.clone())
                .collect()
        };

        for group in &groups {
            let mut ts = Vec::new();
            let mut vals = Vec::new();
            for c in group {
                let (t, v) = c.read_all()?;
                ts.extend(t);
                vals.extend(v);
            }
            debug_assert!(ts.windows(2).all(|w| w[0] < w[1]));

            let id = self.next_id.fetch_add(1, Ordering::Relaxed);
            let bucket = group[0].bucket;
            let merged = SealedChunk::write(
                &self.chunk_dir,
                s.id,
                id,
                bucket,
                &ts,
                &vals,
                self.sync_files(),
            )?;

            let mut st = s.state.write().unwrap();
            if let Some(v) = st.sealed.get_mut(&bucket) {
                v.retain(|c| !group.iter().any(|g| g.id == c.id));
            }
            st.insert_sealed(Arc::new(merged.with_cache(self.cache.clone())));
            drop(st);
            for g in group {
                g.mark_obsolete();
            }
        }
        inc(&self.m.compactions, groups.len() as u64);
        Ok(groups.len())
    }

    fn expire(&self, s: &Series) {
        let Some(r) = self.cfg.retention else { return };
        let mut st = s.state.write().unwrap();
        let Some(last) = st.last_ts else { return };
        let cutoff = last.saturating_sub(r);
        for v in st.sealed.values_mut() {
            v.retain(|c| {
                let keep = c.stats.max_ts >= cutoff;
                if !keep {
                    c.mark_obsolete();
                    inc(&self.m.chunks_expired, 1);
                }
                keep
            });
        }
        st.sealed.retain(|_, v| !v.is_empty());
    }

    fn maintain(&self) -> Result<()> {
        let _m = self.maintenance.lock().unwrap();
        inc(&self.m.maintenance_runs, 1);
        let mut first_err = None;
        for s in &self.series_list() {
            let r = self
                .seal_pending(s)
                .and_then(|_| self.merge_ooo(s))
                .and_then(|_| self.compact_series(s, false));
            if let Err(e) = r {
                first_err.get_or_insert(e);
            }
            self.expire(s);
        }
        if self.wal_bytes() > self.cfg.wal_max_bytes {
            if let Err(e) = self.flush_locked() {
                first_err.get_or_insert(e);
            }
        }
        match first_err {
            Some(e) => {
                inc(&self.m.maintenance_errors, 1);
                Err(e)
            }
            None => Ok(()),
        }
    }

    fn flush(&self) -> Result<()> {
        let _m = self.maintenance.lock().unwrap();
        self.flush_locked()
    }

    fn snapshot(&self, dest: &Path) -> Result<SnapshotInfo> {
        snapshot::empty_target(dest)?;
        // No sealing / compaction / expiry / wal reset while we copy, so the sealed set is
        // fixed and the wal still holds everything that isn't sealed. No flush here: with
        // writers running it would chase them forever. Opening the copy replays the wal.
        let _m = self.maintenance.lock().unwrap();

        let out = dest.join("chunks");
        fs::create_dir_all(&out)?;
        let mut info = SnapshotInfo::default();
        for entry in fs::read_dir(&self.chunk_dir)? {
            let meta = entry?.path();
            if meta.extension().and_then(|e| e.to_str()) != Some("meta") {
                continue;
            }
            // meta last: it is the commit marker. a chunk that vanishes under us (old one
            // dropped by the last reader) is simply not part of the snapshot
            let mut done = Vec::new();
            let mut bytes = (0u64, 0u64);
            let mut gone = false;
            for ext in ["ts", "val", "meta"] {
                let src = meta.with_extension(ext);
                let dst = out.join(src.file_name().unwrap());
                match snapshot::link_or_copy(&src, &dst) {
                    Ok(linked) => {
                        let n = fs::metadata(&dst)?.len();
                        if linked { bytes.0 += n } else { bytes.1 += n }
                        done.push(dst);
                    }
                    Err(e) if e.kind() == io::ErrorKind::NotFound => {
                        gone = true;
                        break;
                    }
                    Err(e) => return Err(e.into()),
                }
            }
            if gone {
                for d in done {
                    let _ = fs::remove_file(d);
                }
                continue;
            }
            info.chunks += 1;
            info.linked_bytes += bytes.0;
            info.copied_bytes += bytes.1;
        }

        // wal before catalog: every series a copied record refers to is already in the
        // catalog by then (series_for appends it before the first record is logged)
        for (i, w) in self.wal.iter().enumerate() {
            let g = w.lock().unwrap();
            let name = format!("wal-{i}.log");
            info.copied_bytes += fs::copy(self.cfg.dir.join(&name), dest.join(&name))?;
            drop(g);
        }
        let g = self.cat_file.lock().unwrap();
        info.copied_bytes += fs::copy(self.cfg.dir.join("series.cat"), dest.join("series.cat"))?;
        drop(g);
        inc(&self.m.snapshots, 1);
        Ok(info)
    }

    // caller holds `maintenance`.
    // Holding the catalog read lock for the whole thing keeps new series (and so new wal
    // records for them) out; grabbing every series lock before touching the wal keeps
    // writers out. Writers only ever hold one series lock, and we take ours in id order,
    // so this can't deadlock.
    fn flush_locked(&self) -> Result<()> {
        let catalog = self.catalog.read().unwrap();
        let mut series: Vec<&Arc<Series>> = catalog.values().collect();
        series.sort_by_key(|s| s.id);

        for s in &series {
            s.state.write().unwrap().freeze_active();
        }
        for s in &series {
            self.seal_pending(s)?;
            self.merge_ooo(s)?;
        }

        let guards: Vec<_> = series.iter().map(|s| s.state.write().unwrap()).collect();
        if guards.iter().all(|g| g.is_drained()) {
            for w in &self.wal {
                w.lock().unwrap().reset()?;
            }
        }
        Ok(())
    }
}

fn worker_loop(inner: Arc<Inner>) {
    loop {
        {
            let mut sig = inner.signal.lock().unwrap();
            if !sig.kick && !sig.shutdown {
                sig = inner
                    .wake
                    .wait_timeout(sig, inner.cfg.maintenance_interval)
                    .unwrap()
                    .0;
            }
            if sig.shutdown {
                return;
            }
            sig.kick = false;
        }
        let res = inner.maintain();
        *inner.last_err.lock().unwrap() = res.err().map(|e| e.to_string());
    }
}

impl Drop for Guard {
    fn drop(&mut self) {
        self.inner.signal.lock().unwrap().shutdown = true;
        self.inner.wake.notify_all();
        if let Some(h) = self.worker.lock().unwrap().take() {
            let _ = h.join();
        }
        if self.inner.cfg.flush_on_close {
            if let Err(e) = self.inner.flush() {
                eprintln!("columnar-tsdb: flush on close failed: {e}");
            }
        }
    }
}
