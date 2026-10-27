use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::config::SyncMode;
use crate::crc::crc32;
use crate::error::{Error, Result};
use crate::model::SeriesKey;

// record: len:u32 | payload | crc32(payload)
// payload: name | n:u32 | (k, v)*   with every string as len:u32 + bytes

fn put_str(b: &mut Vec<u8>, s: &str) {
    b.extend_from_slice(&(s.len() as u32).to_le_bytes());
    b.extend_from_slice(s.as_bytes());
}

fn take<'a>(buf: &mut &'a [u8], n: usize) -> Result<&'a [u8]> {
    if buf.len() < n {
        return Err(Error::corrupt("catalog record truncated"));
    }
    let (head, tail) = buf.split_at(n);
    *buf = tail;
    Ok(head)
}

fn take_u32(buf: &mut &[u8]) -> Result<u32> {
    Ok(u32::from_le_bytes(take(buf, 4)?.try_into().unwrap()))
}

fn take_str(buf: &mut &[u8]) -> Result<String> {
    let n = take_u32(buf)? as usize;
    String::from_utf8(take(buf, n)?.to_vec())
        .map_err(|_| Error::corrupt("catalog string not utf-8"))
}

fn decode(mut p: &[u8]) -> Result<SeriesKey> {
    let mut key = SeriesKey::new(take_str(&mut p)?);
    for _ in 0..take_u32(&mut p)? {
        let k = take_str(&mut p)?;
        let v = take_str(&mut p)?;
        key = key.label(k, v);
    }
    Ok(key)
}

pub fn open(path: &Path) -> Result<(File, Vec<SeriesKey>)> {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)?;
    let mut raw = Vec::new();
    file.read_to_end(&mut raw)?;

    let mut keys = Vec::new();
    let mut pos = 0usize;
    while pos + 4 <= raw.len() {
        let len = u32::from_le_bytes(raw[pos..pos + 4].try_into().unwrap()) as usize;
        let end = pos + 4 + len + 4;
        if end > raw.len() {
            break;
        }
        let payload = &raw[pos + 4..pos + 4 + len];
        let stored = u32::from_le_bytes(raw[end - 4..end].try_into().unwrap());
        if crc32(payload) != stored {
            break;
        }
        keys.push(decode(payload)?);
        pos = end;
    }

    if pos != raw.len() {
        file.set_len(pos as u64)?;
    }
    file.seek(SeekFrom::Start(pos as u64))?;
    Ok((file, keys))
}

pub fn append(file: &mut File, key: &SeriesKey, sync: SyncMode) -> Result<()> {
    let mut payload = Vec::new();
    put_str(&mut payload, key.name());
    payload.extend_from_slice(&(key.labels().len() as u32).to_le_bytes());
    for (k, v) in key.labels() {
        put_str(&mut payload, k);
        put_str(&mut payload, v);
    }

    let mut rec = Vec::with_capacity(payload.len() + 8);
    rec.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    rec.extend_from_slice(&payload);
    rec.extend_from_slice(&crc32(&payload).to_le_bytes());
    file.write_all(&rec)?;
    if sync == SyncMode::Always {
        file.sync_data()?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_with_torn_tail() {
        let p = std::env::temp_dir().join(format!("ctsdb-cat-{}.cat", std::process::id()));
        let _ = std::fs::remove_file(&p);

        let a = SeriesKey::new("cpu").label("host", "a1").label("dc", "fra");
        let b = SeriesKey::new("mem");
        let (mut f, keys) = open(&p).unwrap();
        assert!(keys.is_empty());
        append(&mut f, &a, SyncMode::Never).unwrap();
        append(&mut f, &b, SyncMode::Never).unwrap();
        f.write_all(&[9, 0, 0]).unwrap();
        drop(f);

        let (_, keys) = open(&p).unwrap();
        assert_eq!(keys, vec![a, b]);
        let _ = std::fs::remove_file(&p);
    }
}
