use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::catalog;
use crate::chunk::{self, BlockCache, SealedChunk};
use crate::config::{Config, SyncMode};
use crate::error::{Error, Result};
use crate::model::{Aggregate, Point, SeriesId, SeriesKey};
use crate::series::{bucket_of, merge_override, Series};
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
    cat_file: Mutex<File>,
    wal: Mutex<Wal>,
    next_id: AtomicU64,
    cache: Arc<BlockCache>,
    // serialises sealing / compaction / checkpoint. only the worker and flush() take it
    maintenance: Mutex<()>,
    signal: Mutex<Signal>,
    wake: Condvar,
    last_err: Mutex<Option<String>>,
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
        self.inner.wal.lock().unwrap().append(s.id, pts, &late)?;
        let busy = st.apply(pts, &late, &self.inner.cfg, &self.inner.next_id);
        drop(st);
        if busy {
            self.inner.kick();
        }
        Ok(())
    }

    // points with start <= ts < end, ascending
    pub fn query_range(&self, key: &SeriesKey, start: i64, end: i64) -> Result<Vec<Point>> {
        let Some(s) = self.inner.find(key) else {
            return Ok(Vec::new());
        };
        let snap = s
            .state
            .read()
            .unwrap()
            .snapshot(start, end, self.inner.cfg.bucket_width, true);

        // decoding happens with no lock held; chunks are immutable
        let mut out = Vec::new();
        for c in &snap.sealed {
            out.extend(c.read_range(start, end)?);
        }
        out.extend(snap.mem);
        Ok(merge_override(out, &snap.ooo))
    }

    // chunks fully inside [start, end) are answered from their stored stats
    pub fn aggregate(&self, key: &SeriesKey, start: i64, end: i64) -> Result<Option<Aggregate>> {
        let Some(s) = self.inner.find(key) else {
            return Ok(None);
        };
        let snap = s
            .state
            .read()
            .unwrap()
            .snapshot(start, end, self.inner.cfg.bucket_width, false);

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
        st.wal_bytes = self.inner.wal.lock().unwrap().size();
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
        for k in keys {
            let s = Series::new(k);
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

        let (wal, recs) = Wal::open(&cfg.dir.join("wal.log"), cfg.sync)?;
        let inner = Arc::new(Inner {
            cfg,
            chunk_dir,
            catalog: RwLock::new(map),
            cat_file: Mutex::new(cat_file),
            wal: Mutex::new(wal),
            next_id: AtomicU64::new(max_id + 1),
            cache,
            maintenance: Mutex::new(()),
            signal: Mutex::new(Signal {
                shutdown: false,
                kick: false,
            }),
            wake: Condvar::new(),
            last_err: Mutex::new(None),
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
                }
                keep
            });
        }
        st.sealed.retain(|_, v| !v.is_empty());
    }

    fn maintain(&self) -> Result<()> {
        let _m = self.maintenance.lock().unwrap();
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
        if self.wal.lock().unwrap().size() > self.cfg.wal_max_bytes {
            if let Err(e) = self.flush_locked() {
                first_err.get_or_insert(e);
            }
        }
        match first_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }

    fn flush(&self) -> Result<()> {
        let _m = self.maintenance.lock().unwrap();
        self.flush_locked()
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
            self.wal.lock().unwrap().reset()?;
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
