use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::config::SyncMode;
use crate::crc::crc32;
use crate::error::Result;
use crate::model::{Point, SeriesId};

// series:u64 | ts:i64 | value bits:u64 | late:u8 | crc32 of the first 25 bytes
const REC: usize = 29;

pub struct WalRecord {
    pub series: SeriesId,
    pub point: Point,
    // written out of order (ts <= series last_ts at that moment)
    pub late: bool,
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
        for r in raw.chunks(REC).filter(|r| r.len() == REC) {
            let stored = u32::from_le_bytes(r[25..29].try_into().unwrap());
            if crc32(&r[..25]) != stored {
                break;
            }
            recs.push(WalRecord {
                series: u64::from_le_bytes(r[0..8].try_into().unwrap()),
                point: Point::new(
                    i64::from_le_bytes(r[8..16].try_into().unwrap()),
                    f64::from_bits(u64::from_le_bytes(r[16..24].try_into().unwrap())),
                ),
                late: r[24] != 0,
            });
            valid += REC;
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
}
