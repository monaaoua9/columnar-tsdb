use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::config::SyncMode;
use crate::crc::crc32;
use crate::error::Result;
use crate::model::{Point, SeriesId};

// series:u64 | ts:i64 | value bits:u64 | kind:u8 | crc32 of the first 25 bytes
// kind 0 = in-order point, 1 = late point.
// A delete is two records written in one go: kind 2 (ts = range start, value bytes = exclusive
// end as i64) then kind 3 (value bytes = the chunk id mark, see Engine::delete). Replay only
// yields it when both made it, so a torn write drops the whole delete.
const REC: usize = 29;
const KIND_DEL_RANGE: u8 = 2;
const KIND_DEL_MARK: u8 = 3;

pub struct WalRecord {
    pub series: SeriesId,
    pub point: Point,
    // written out of order (ts <= series last_ts at that moment)
    pub late: bool,
    // Some(end) marks a delete of [point.ts, end); point.value is meaningless then
    pub del_end: Option<i64>,
    // chunks with an id below this existed when the delete ran; only they can hold what it removes
    pub del_mark: u64,
}

pub struct Wal {
    file: File,
    size: u64,
    sync: SyncMode,
    buf: Vec<u8>,
}

impl Wal {
    // replays what's there; a torn or corrupt tail is cut off
    pub fn open(path: &Path, sync: SyncMode) -> Result<(Wal, Vec<WalRecord>)> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        let mut raw = Vec::new();
        file.read_to_end(&mut raw)?;

        let mut recs = Vec::with_capacity(raw.len() / REC);
        let mut valid = 0usize;
        let mut at = 0usize;
        // a kind-2 record waiting for its kind-3 partner
        let mut half: Option<WalRecord> = None;
        for r in raw.chunks(REC).filter(|r| r.len() == REC) {
            let stored = u32::from_le_bytes(r[25..29].try_into().unwrap());
            if crc32(&r[..25]) != stored {
                break;
            }
            let series = u64::from_le_bytes(r[0..8].try_into().unwrap());
            let ts = i64::from_le_bytes(r[8..16].try_into().unwrap());
            let raw_val = u64::from_le_bytes(r[16..24].try_into().unwrap());
            at += REC;
            match (r[24], half.take()) {
                (KIND_DEL_RANGE, None) => {
                    half = Some(WalRecord {
                        series,
                        point: Point::new(ts, 0.0),
                        late: false,
                        del_end: Some(raw_val as i64),
                        del_mark: 0,
                    });
                    continue;
                }
                (KIND_DEL_MARK, Some(mut rec)) if rec.series == series => {
                    rec.del_mark = raw_val;
                    recs.push(rec);
                }
                // a lone half, a mark without a range, or a point in the middle of a pair
                (KIND_DEL_RANGE | KIND_DEL_MARK, _) | (_, Some(_)) => break,
                (kind, None) => recs.push(WalRecord {
                    series,
                    point: Point::new(ts, f64::from_bits(raw_val)),
                    late: kind != 0,
                    del_end: None,
                    del_mark: 0,
                }),
            }
            valid = at;
        }

        if valid != raw.len() {
            file.set_len(valid as u64)?;
        }
        file.seek(SeekFrom::Start(valid as u64))?;
        Ok((
            Wal {
                file,
                size: valid as u64,
                sync,
                buf: Vec::new(),
            },
            recs,
        ))
    }

    pub fn append(&mut self, series: SeriesId, pts: &[Point], late: &[bool]) -> Result<()> {
        debug_assert_eq!(pts.len(), late.len());
        self.buf.clear();
        for (p, &l) in pts.iter().zip(late) {
            let at = self.buf.len();
            self.buf.extend_from_slice(&series.to_le_bytes());
            self.buf.extend_from_slice(&p.ts.to_le_bytes());
            self.buf.extend_from_slice(&p.value.to_bits().to_le_bytes());
            self.buf.push(l as u8);
            let crc = crc32(&self.buf[at..]);
            self.buf.extend_from_slice(&crc.to_le_bytes());
        }
        // one write per batch. if it fails halfway the tail is torn and replay drops it
        self.file.write_all(&self.buf)?;
        if self.sync == SyncMode::Always {
            self.file.sync_data()?;
        }
        self.size += self.buf.len() as u64;
        Ok(())
    }

    pub fn append_delete(
        &mut self,
        series: SeriesId,
        start: i64,
        end: i64,
        mark: u64,
    ) -> Result<()> {
        self.buf.clear();
        for (kind, ts, val) in [
            (KIND_DEL_RANGE, start, end as u64),
            (KIND_DEL_MARK, 0, mark),
        ] {
            let at = self.buf.len();
            self.buf.extend_from_slice(&series.to_le_bytes());
            self.buf.extend_from_slice(&ts.to_le_bytes());
            self.buf.extend_from_slice(&val.to_le_bytes());
            self.buf.push(kind);
            let crc = crc32(&self.buf[at..]);
            self.buf.extend_from_slice(&crc.to_le_bytes());
        }
        self.file.write_all(&self.buf)?;
        if self.sync == SyncMode::Always {
            self.file.sync_data()?;
        }
        self.size += self.buf.len() as u64;
        Ok(())
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    pub fn reset(&mut self) -> Result<()> {
        self.file.set_len(0)?;
        self.file.seek(SeekFrom::Start(0))?;
        if self.sync == SyncMode::Always {
            self.file.sync_all()?;
        }
        self.size = 0;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replay_and_torn_tail() {
        let p = std::env::temp_dir().join(format!("ctsdb-wal-{}.log", std::process::id()));
        let _ = std::fs::remove_file(&p);

        let (mut w, recs) = Wal::open(&p, SyncMode::Never).unwrap();
        assert!(recs.is_empty());
        w.append(
            9,
            &[Point::new(1, 1.5), Point::new(2, -2.5)],
            &[false, true],
        )
        .unwrap();
        w.append(10, &[Point::new(3, 0.0)], &[false]).unwrap();
        drop(w);

        // simulate a crash mid-write
        let mut f = OpenOptions::new().append(true).open(&p).unwrap();
        f.write_all(&[1, 2, 3, 4, 5]).unwrap();
        drop(f);

        let (mut w, recs) = Wal::open(&p, SyncMode::Never).unwrap();
        assert_eq!(recs.len(), 3);
        assert_eq!(recs[1].point, Point::new(2, -2.5));
        assert!(recs[1].late && !recs[0].late);
        assert_eq!(recs[2].series, 10);
        assert_eq!(w.size(), 3 * REC as u64);

        w.reset().unwrap();
        drop(w);
        assert!(Wal::open(&p, SyncMode::Never).unwrap().1.is_empty());
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn delete_records_round_trip_in_order() {
        let p = std::env::temp_dir().join(format!("ctsdb-wal-del-{}.log", std::process::id()));
        let _ = std::fs::remove_file(&p);
        let (mut w, _) = Wal::open(&p, SyncMode::Never).unwrap();
        w.append(7, &[Point::new(5, 1.0)], &[false]).unwrap();
        w.append_delete(7, i64::MIN, i64::MAX, 41).unwrap();
        w.append_delete(7, -40, 12, u64::MAX).unwrap();
        w.append(7, &[Point::new(2, 9.0)], &[true]).unwrap();
        drop(w);

        let (w, recs) = Wal::open(&p, SyncMode::Never).unwrap();
        assert_eq!(recs.len(), 4);
        assert_eq!(recs[0].del_end, None);
        assert_eq!(
            (recs[1].point.ts, recs[1].del_end, recs[1].del_mark),
            (i64::MIN, Some(i64::MAX), 41)
        );
        assert_eq!(
            (recs[2].point.ts, recs[2].del_end, recs[2].del_mark),
            (-40, Some(12), u64::MAX)
        );
        assert!(!recs[2].late && recs[2].series == 7);
        assert!(recs[3].late && recs[3].del_end.is_none());
        // 1 point + 2 deletes of 2 records each + 1 point
        assert_eq!(w.size(), 6 * REC as u64);
        drop(w);
        let _ = std::fs::remove_file(&p);
    }

    #[test]
    fn a_torn_delete_is_dropped_whole() {
        let p = std::env::temp_dir().join(format!("ctsdb-wal-tdel-{}.log", std::process::id()));
        let _ = std::fs::remove_file(&p);
        let (mut w, _) = Wal::open(&p, SyncMode::Never).unwrap();
        w.append(7, &[Point::new(5, 1.0)], &[false]).unwrap();
        w.append_delete(7, 0, 100, 9).unwrap();
        drop(w);
        let full = std::fs::read(&p).unwrap();
        assert_eq!(full.len(), 3 * REC);

        // cut inside the second record, then exactly between the two: either way the delete goes
        for cut in [3 * REC - 5, 2 * REC] {
            std::fs::write(&p, &full[..cut]).unwrap();
            let (w, recs) = Wal::open(&p, SyncMode::Never).unwrap();
            assert_eq!(recs.len(), 1, "cut at {cut}");
            assert_eq!(w.size(), REC as u64, "tail must be truncated, cut at {cut}");
            drop(w);
            assert_eq!(std::fs::metadata(&p).unwrap().len(), REC as u64);
        }

        // a flipped bit in either half also kills the whole delete (and everything after it)
        for flip in [REC + 9, 2 * REC + 17] {
            let mut raw = full.clone();
            raw[flip] ^= 1;
            std::fs::write(&p, &raw).unwrap();
            let (_, recs) = Wal::open(&p, SyncMode::Never).unwrap();
            assert_eq!(recs.len(), 1, "flip at {flip}");
        }
        let _ = std::fs::remove_file(&p);
    }
}
