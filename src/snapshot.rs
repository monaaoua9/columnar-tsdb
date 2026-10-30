use std::fs;
use std::io;
use std::path::Path;

use crate::error::{Error, Result};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SnapshotInfo {
    pub chunks: usize,
    // chunk bytes shared with the live dir through hard links (no extra disk used)
    pub linked_bytes: u64,
    pub copied_bytes: u64,
}

// chunk files never change after the tmp+rename that created them, so a hard link is a free,
// consistent copy. falls back to a real copy when the fs can't link (other volume, fat, ...)
pub(crate) fn link_or_copy(src: &Path, dst: &Path) -> io::Result<bool> {
    match fs::hard_link(src, dst) {
        Ok(()) => Ok(true),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Err(e),
        Err(_) => fs::copy(src, dst).map(|_| false),
    }
}

pub(crate) fn empty_target(dest: &Path) -> Result<()> {
    if dest.exists() {
        if !dest.is_dir() || fs::read_dir(dest)?.next().is_some() {
            return Err(Error::BadConfig("snapshot target must be an empty directory"));
        }
    }
    fs::create_dir_all(dest)?;
    Ok(())
}

// Copies a snapshot (or any closed data dir) into a fresh directory. Real copies, so the
// result is independent of the source. Refuses to run on a dir that an engine has open.
pub fn restore(src: &Path, dst: &Path) -> Result<u64> {
    if src.join("LOCK").exists() {
        // a snapshot never carries LOCK; a live dir does, and copying that mid-write is not a backup
        return Err(Error::BadConfig("source looks like a live data dir, snapshot it first"));
    }
    empty_target(dst)?;
    let mut bytes = 0;
    let mut stack = vec![(src.to_path_buf(), dst.to_path_buf())];
    while let Some((from, to)) = stack.pop() {
        for entry in fs::read_dir(&from)? {
            let entry = entry?;
            let (p, q) = (entry.path(), to.join(entry.file_name()));
            if entry.file_type()?.is_dir() {
                fs::create_dir_all(&q)?;
                stack.push((p, q));
            } else {
                bytes += fs::copy(&p, &q)?;
            }
        }
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::Rng;

    fn tmp(tag: &str) -> std::path::PathBuf {
        let d = std::env::temp_dir().join(format!("ctsdb-snap-{tag}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&d);
        d
    }

    #[test]
    fn target_must_be_empty() {
        let d = tmp("empty");
        assert!(empty_target(&d).is_ok());
        assert!(empty_target(&d).is_ok()); // exists but empty
        fs::write(d.join("x"), b"1").unwrap();
        assert!(empty_target(&d).is_err());
        let f = d.join("x");
        assert!(empty_target(&f).is_err());
        fs::remove_dir_all(d).unwrap();
    }

    #[test]
    fn link_falls_back_and_reports() {
        let d = tmp("link");
        fs::create_dir_all(&d).unwrap();
        fs::write(d.join("a"), b"hello").unwrap();
        assert!(link_or_copy(&d.join("a"), &d.join("b")).unwrap());
        assert_eq!(fs::read(d.join("b")).unwrap(), b"hello");
        // target exists (and is a different file) -> link fails, copy overwrites
        fs::write(d.join("x"), b"old").unwrap();
        assert!(!link_or_copy(&d.join("a"), &d.join("x")).unwrap());
        assert_eq!(fs::read(d.join("x")).unwrap(), b"hello");
        let e = link_or_copy(&d.join("gone"), &d.join("c")).unwrap_err();
        assert_eq!(e.kind(), io::ErrorKind::NotFound);
        fs::remove_dir_all(d).unwrap();
    }

    #[test]
    fn restore_copies_tree_and_refuses_live_dirs() {
        let (s, t) = (tmp("rs-src"), tmp("rs-dst"));
        fs::create_dir_all(s.join("chunks")).unwrap();
        let mut r = Rng::new(5);
        let blob: Vec<u8> = (0..4096).map(|_| r.below(256) as u8).collect();
        fs::write(s.join("chunks/c1.ts"), &blob).unwrap();
        fs::write(s.join("series.cat"), b"cat").unwrap();

        let n = restore(&s, &t).unwrap();
        assert_eq!(n, 4096 + 3);
        assert_eq!(fs::read(t.join("chunks/c1.ts")).unwrap(), blob);

        fs::write(s.join("LOCK"), b"").unwrap();
        assert!(restore(&s, &tmp("rs-dst2")).is_err());
        fs::remove_dir_all(s).unwrap();
        fs::remove_dir_all(t).unwrap();
    }
}
