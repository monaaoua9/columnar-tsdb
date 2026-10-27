use std::path::PathBuf;
use std::time::Duration;

use crate::error::{Error, Result};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SyncMode {
    // write() to the os, no fsync. survives process crash, not power loss
    Never,
    // fsync wal on every append call and chunk files when sealed
    Always,
}

#[derive(Clone, Debug)]
pub struct Config {
    pub dir: PathBuf,
    // same unit as your timestamps (ms by default in the docs)
    pub bucket_width: i64,
    pub max_points_per_chunk: usize,
    // appends block while a series has more frozen chunks than this (fail if the sealer errors)
    pub max_frozen_chunks: usize,
    // decoded-from-disk column blocks kept in memory; 0 disables the cache
    pub block_cache_bytes: usize,
    pub sync: SyncMode,
    pub maintenance_interval: Duration,
    pub wal_max_bytes: u64,
    // drop sealed chunks older than (series last_ts - retention)
    pub retention: Option<i64>,
    pub flush_on_close: bool,
}

impl Config {
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Config {
            dir: dir.into(),
            bucket_width: 2 * 3600 * 1000,
            max_points_per_chunk: 16_384,
            max_frozen_chunks: 64,
            block_cache_bytes: 64 << 20,
            sync: SyncMode::Never,
            maintenance_interval: Duration::from_millis(500),
            wal_max_bytes: 64 << 20,
            retention: None,
            flush_on_close: true,
        }
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if self.bucket_width <= 0 {
            return Err(Error::BadConfig("bucket_width must be positive"));
        }
        if self.max_points_per_chunk == 0 {
            return Err(Error::BadConfig("max_points_per_chunk must be positive"));
        }
        if matches!(self.retention, Some(r) if r <= 0) {
            return Err(Error::BadConfig("retention must be positive"));
        }
        Ok(())
    }
}
