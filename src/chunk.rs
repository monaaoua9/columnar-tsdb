//! Chunk lifecycle: ActiveChunk (mutable, raw) -> frozen (immutable, raw, same type
//! behind an Arc) -> SealedChunk (immutable, compressed, on disk).
//!
//! A sealed chunk is three files, one per column group:
//!
//! ```text
//! {series:016x}-{id:010}.ts    delta-of-delta timestamps
//! {series:016x}-{id:010}.val   gorilla values
//! {series:016x}-{id:010}.meta  stats + ids, written last = commit marker
//! ```
//!
//! Each file is `"TSC1" | kind:u8 | len:u64 | payload | crc32(payload)`.

use std::collections::{HashMap, VecDeque};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use crate::compress::{gorilla, timestamp};
use crate::crc::crc32;
use crate::error::{Error, Result};
use crate::model::{Aggregate, Point, SeriesId};

const MAGIC: &[u8; 4] = b"TSC1";
const KIND_TS: u8 = 1;
const KIND_VAL: u8 = 2;
const KIND_META: u8 = 3;
const META_LEN: usize = 11 * 8;

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ChunkStats {
    pub min_ts: i64,
    pub max_ts: i64,
    pub agg: Aggregate,
}

impl ChunkStats {
    pub fn covered_by(&self, start: i64, end: i64) -> bool {
        start <= self.min_ts && self.max_ts < end
    }

    pub fn overlaps(&self, start: i64, end: i64) -> bool {
        self.min_ts < end && self.max_ts >= start
    }
}

pub struct ActiveChunk {
    pub id: u64,
    pub bucket: i64,
    ts: Vec<i64>,
    vals: Vec<f64>,
    agg: Aggregate,
}

impl ActiveChunk {
    pub fn new(id: u64, bucket: i64) -> Self {
        ActiveChunk {
            id,
            bucket,
            ts: Vec::new(),
            vals: Vec::new(),
            agg: Aggregate::EMPTY,
        }
    }

    pub fn push(&mut self, p: Point) {
        self.ts.push(p.ts);
        self.vals.push(p.value);
        self.agg.add(p.value);
    }

    pub fn len(&self) -> usize {
        self.ts.len()
    }

    pub fn stats(&self) -> ChunkStats {
        ChunkStats {
            min_ts: self.ts[0],
            max_ts: self.ts[self.ts.len() - 1],
            agg: self.agg,
        }
    }

    pub fn columns(&self) -> (&[i64], &[f64]) {
        (&self.ts, &self.vals)
    }

    pub fn collect_range(&self, start: i64, end: i64, out: &mut Vec<Point>) {
        let lo = self.ts.partition_point(|&t| t < start);
        let hi = self.ts.partition_point(|&t| t < end);
        for i in lo..hi {
            out.push(Point::new(self.ts[i], self.vals[i]));
        }
    }
}

#[derive(Clone, Copy)]
enum Col {
    Ts,
    Val,
}

type BlockKey = (SeriesId, u64, u8);

// Shared by every chunk of an engine. Capped by bytes; evicts oldest-inserted first (FIFO,
// not LRU: hits don't reorder). Good enough, readers mostly sweep time ranges.
pub struct BlockCache {
    cap: usize,
    inner: Mutex<CacheInner>,
}

#[derive(Default)]
struct CacheInner {
    map: HashMap<BlockKey, Arc<[u8]>>,
    order: VecDeque<BlockKey>,
    bytes: usize,
}

impl BlockCache {
    pub fn new(cap: usize) -> Self {
        BlockCache {
            cap,
            inner: Mutex::new(CacheInner::default()),
        }
    }

    fn get(&self, k: &BlockKey) -> Option<Arc<[u8]>> {
        self.inner.lock().unwrap().map.get(k).cloned()
    }

    fn put(&self, k: BlockKey, v: Arc<[u8]>) {
        if v.len() > self.cap {
            return;
        }
        let mut c = self.inner.lock().unwrap();
        if c.map.contains_key(&k) {
            return;
        }
        c.bytes += v.len();
        c.map.insert(k, v);
        c.order.push_back(k);
        while c.bytes > self.cap {
            let Some(old) = c.order.pop_front() else {
                break;
            };
            if let Some(b) = c.map.remove(&old) {
                c.bytes -= b.len();
            }
        }
    }

    pub fn bytes(&self) -> usize {
        self.inner.lock().unwrap().bytes
    }
}

pub struct SealedChunk {
    pub id: u64,
    pub series: SeriesId,
    pub bucket: i64,
    pub stats: ChunkStats,
    pub ts_bytes: u64,
    pub val_bytes: u64,
    dir: PathBuf,
    cache: Arc<BlockCache>,
    obsolete: AtomicBool,
}

fn chunk_path(dir: &Path, series: SeriesId, id: u64, ext: &str) -> PathBuf {
    dir.join(format!("{series:016x}-{id:010}.{ext}"))
}

fn frame(kind: u8, payload: &[u8]) -> Vec<u8> {
    let mut b = Vec::with_capacity(payload.len() + 17);
    b.extend_from_slice(MAGIC);
    b.push(kind);
    b.extend_from_slice(&(payload.len() as u64).to_le_bytes());
    b.extend_from_slice(payload);
    b.extend_from_slice(&crc32(payload).to_le_bytes());
    b
}

fn unframe(mut buf: Vec<u8>, kind: u8) -> Result<Vec<u8>> {
    if buf.len() < 17 || &buf[..4] != MAGIC || buf[4] != kind {
        return Err(Error::corrupt("bad chunk file header"));
    }
    let len = u64::from_le_bytes(buf[5..13].try_into().unwrap()) as usize;
    if buf.len() != 13 + len + 4 {
        return Err(Error::corrupt("chunk file length mismatch"));
    }
    let stored = u32::from_le_bytes(buf[13 + len..].try_into().unwrap());
    if crc32(&buf[13..13 + len]) != stored {
        return Err(Error::corrupt("chunk file checksum mismatch"));
    }
    buf.truncate(13 + len);
    buf.drain(..13);
    Ok(buf)
}

// tmp + rename so a reader/recovery never sees half a file
fn write_atomic(path: &Path, bytes: &[u8], sync: bool) -> Result<()> {
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(".tmp");
    let tmp = PathBuf::from(tmp);
    let mut f = fs::File::create(&tmp)?;
    f.write_all(bytes)?;
    if sync {
        f.sync_all()?;
    }
    drop(f);
    fs::rename(&tmp, path)?;
    Ok(())
}

fn put(b: &mut Vec<u8>, v: u64) {
    b.extend_from_slice(&v.to_le_bytes());
}

fn get(b: &[u8], i: usize) -> u64 {
    u64::from_le_bytes(b[i * 8..i * 8 + 8].try_into().unwrap())
}

impl SealedChunk {
    pub fn write(
        dir: &Path,
        series: SeriesId,
        id: u64,
        bucket: i64,
        ts: &[i64],
        vals: &[f64],
        sync: bool,
    ) -> Result<SealedChunk> {
        debug_assert!(!ts.is_empty() && ts.len() == vals.len());
        let mut agg = Aggregate::EMPTY;
        for &v in vals {
            agg.add(v);
        }
        let stats = ChunkStats {
            min_ts: ts[0],
            max_ts: ts[ts.len() - 1],
            agg,
        };

        let ts_blk = timestamp::encode(ts);
        let val_blk = gorilla::encode(vals);
        write_atomic(
            &chunk_path(dir, series, id, "ts"),
            &frame(KIND_TS, &ts_blk),
            sync,
        )?;
        write_atomic(
            &chunk_path(dir, series, id, "val"),
            &frame(KIND_VAL, &val_blk),
            sync,
        )?;

        let mut meta = Vec::with_capacity(META_LEN);
        put(&mut meta, series);
        put(&mut meta, id);
        put(&mut meta, bucket as u64);
        put(&mut meta, stats.min_ts as u64);
        put(&mut meta, stats.max_ts as u64);
        put(&mut meta, agg.count);
        put(&mut meta, agg.sum.to_bits());
        put(&mut meta, agg.min.to_bits());
        put(&mut meta, agg.max.to_bits());
        put(&mut meta, ts_blk.len() as u64);
        put(&mut meta, val_blk.len() as u64);
        write_atomic(
            &chunk_path(dir, series, id, "meta"),
            &frame(KIND_META, &meta),
            sync,
        )?;

        Ok(SealedChunk {
            id,
            series,
            bucket,
            stats,
            ts_bytes: ts_blk.len() as u64,
            val_bytes: val_blk.len() as u64,
            dir: dir.to_path_buf(),
            cache: Arc::new(BlockCache::new(0)),
            obsolete: AtomicBool::new(false),
        })
    }

    fn open(meta_path: &Path) -> Result<SealedChunk> {
        let meta = unframe(fs::read(meta_path)?, KIND_META)?;
        if meta.len() != META_LEN {
            return Err(Error::corrupt("bad meta size"));
        }
        let agg = Aggregate {
            count: get(&meta, 5),
            sum: f64::from_bits(get(&meta, 6)),
            min: f64::from_bits(get(&meta, 7)),
            max: f64::from_bits(get(&meta, 8)),
        };
        Ok(SealedChunk {
            series: get(&meta, 0),
            id: get(&meta, 1),
            bucket: get(&meta, 2) as i64,
            stats: ChunkStats {
                min_ts: get(&meta, 3) as i64,
                max_ts: get(&meta, 4) as i64,
                agg,
            },
            ts_bytes: get(&meta, 9),
            val_bytes: get(&meta, 10),
            dir: meta_path.parent().unwrap().to_path_buf(),
            cache: Arc::new(BlockCache::new(0)),
            obsolete: AtomicBool::new(false),
        })
    }

    pub fn with_cache(mut self, cache: Arc<BlockCache>) -> Self {
        self.cache = cache;
        self
    }

    // lazily pulled from disk; a covered aggregate never gets here
    fn load(&self, which: Col) -> Result<Arc<[u8]>> {
        let (ext, kind) = match which {
            Col::Ts => ("ts", KIND_TS),
            Col::Val => ("val", KIND_VAL),
        };
        let key = (self.series, self.id, kind);
        if let Some(b) = self.cache.get(&key) {
            return Ok(b);
        }
        let raw = fs::read(chunk_path(&self.dir, self.series, self.id, ext))?;
        let bytes: Arc<[u8]> = unframe(raw, kind)?.into();
        self.cache.put(key, bytes.clone());
        Ok(bytes)
    }

    pub fn read_range(&self, start: i64, end: i64) -> Result<Vec<Point>> {
        let ts = timestamp::decode(&self.load(Col::Ts)?, usize::MAX)?;
        if ts.len() as u64 != self.stats.agg.count {
            return Err(Error::corrupt("timestamp count disagrees with meta"));
        }
        let lo = ts.partition_point(|&t| t < start);
        let hi = ts.partition_point(|&t| t < end);
        if lo >= hi {
            return Ok(Vec::new());
        }
        let vals = gorilla::decode(&self.load(Col::Val)?, hi)?;
        if vals.len() != hi {
            return Err(Error::corrupt("value column shorter than timestamps"));
        }
        Ok((lo..hi).map(|i| Point::new(ts[i], vals[i])).collect())
    }

    pub fn read_all(&self) -> Result<(Vec<i64>, Vec<f64>)> {
        let ts = timestamp::decode(&self.load(Col::Ts)?, usize::MAX)?;
        let vals = gorilla::decode(&self.load(Col::Val)?, usize::MAX)?;
        if ts.len() != vals.len() || ts.len() as u64 != self.stats.agg.count {
            return Err(Error::corrupt("column length mismatch"));
        }
        Ok((ts, vals))
    }

    // files go away when the last Arc (maybe held by a running query) is dropped
    pub fn mark_obsolete(&self) {
        self.obsolete.store(true, Ordering::Release);
    }
}

impl Drop for SealedChunk {
    fn drop(&mut self) {
        if !self.obsolete.load(Ordering::Acquire) {
            return;
        }
        // meta first: a half-deleted chunk must not look committed
        for ext in ["meta", "ts", "val"] {
            let _ = fs::remove_file(chunk_path(&self.dir, self.series, self.id, ext));
        }
    }
}

// Loads every committed chunk under dir. Orphans (tmp files, data without meta)
// are deleted. A chunk whose [min_ts, max_ts] sits inside another chunk of the same
// series is a compaction leftover (crash between "new chunk written" and "old
// chunks deleted") and gets dropped too.
pub fn scan(dir: &Path) -> Result<Vec<SealedChunk>> {
    let mut chunks = Vec::new();
    let mut data_files = Vec::new();
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        match path.extension().and_then(|e| e.to_str()) {
            Some("meta") => chunks.push(SealedChunk::open(&path)?),
            Some("ts") | Some("val") | Some("tmp") => data_files.push(path),
            _ => {}
        }
    }

    for p in data_files {
        let committed =
            p.extension().is_some_and(|e| e != "tmp") && p.with_extension("meta").exists();
        if !committed {
            let _ = fs::remove_file(p);
        }
    }

    chunks.sort_by(|a, b| {
        (
            a.series,
            a.stats.min_ts,
            std::cmp::Reverse(a.stats.max_ts),
            std::cmp::Reverse(a.id),
        )
            .cmp(&(
                b.series,
                b.stats.min_ts,
                std::cmp::Reverse(b.stats.max_ts),
                std::cmp::Reverse(b.id),
            ))
    });

    let mut kept: Vec<SealedChunk> = Vec::with_capacity(chunks.len());
    for c in chunks {
        let dup = kept
            .last()
            .is_some_and(|k| k.series == c.series && c.stats.max_ts <= k.stats.max_ts);
        if dup {
            c.mark_obsolete();
        } else {
            kept.push(c);
        }
    }
    Ok(kept)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmpdir(tag: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("ctsdb-chunk-{tag}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&d);
        fs::create_dir_all(&d).unwrap();
        d
    }

    fn sample(n: usize, base: i64) -> (Vec<i64>, Vec<f64>) {
        (
            (0..n as i64).map(|i| base + i * 1000).collect(),
            (0..n).map(|i| i as f64 * 0.25).collect(),
        )
    }

    #[test]
    fn write_reopen_read() {
        let d = tmpdir("rw");
        let (ts, vals) = sample(500, 10_000);
        SealedChunk::write(&d, 7, 1, 0, &ts, &vals, false).unwrap();

        let found = scan(&d).unwrap();
        assert_eq!(found.len(), 1);
        let c = &found[0];
        assert_eq!(c.stats.agg.count, 500);
        let (t2, v2) = c.read_all().unwrap();
        assert_eq!((t2, v2), (ts.clone(), vals));

        let part = c.read_range(ts[10], ts[20]).unwrap();
        assert_eq!(part.len(), 10);
        assert_eq!(part[0].ts, ts[10]);
        fs::remove_dir_all(d).unwrap();
    }

    #[test]
    fn corruption_is_detected() {
        let d = tmpdir("crc");
        let (ts, vals) = sample(50, 0);
        SealedChunk::write(&d, 1, 1, 0, &ts, &vals, false).unwrap();
        let p = chunk_path(&d, 1, 1, "val");
        let mut raw = fs::read(&p).unwrap();
        raw[20] ^= 0x40;
        fs::write(&p, raw).unwrap();

        let c = scan(&d).unwrap().pop().unwrap();
        assert!(matches!(c.read_all(), Err(Error::Corrupt(_))));
        fs::remove_dir_all(d).unwrap();
    }

    #[test]
    fn compaction_leftovers_and_orphans_are_cleaned() {
        let d = tmpdir("dup");
        let (ts, vals) = sample(100, 0);
        SealedChunk::write(&d, 1, 1, 0, &ts[..50], &vals[..50], false).unwrap();
        SealedChunk::write(&d, 1, 2, 0, &ts[50..], &vals[50..], false).unwrap();
        SealedChunk::write(&d, 1, 3, 0, &ts, &vals, false).unwrap(); // merged, olds not deleted
        fs::write(d.join("0000000000000001-0000000009.ts"), b"orphan").unwrap();

        let found = scan(&d).unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].id, 3);
        drop(found);
        let mut names: Vec<_> = fs::read_dir(&d)
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        names.sort();
        assert_eq!(names.len(), 3); // .meta .ts .val of chunk 3
        fs::remove_dir_all(d).unwrap();
    }
}
