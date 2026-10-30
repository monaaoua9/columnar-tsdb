//! Embedded columnar time-series storage engine.
//!
//! Points flow: `append` -> WAL -> active chunk (raw, in memory) -> frozen when its
//! time bucket rolls or it fills up -> sealed by the background thread into
//! immutable, compressed, per-column files -> merged by compaction.

mod bitio;
mod catalog;
mod chunk;
mod crc;
mod index;
mod metrics;
mod series;
mod wal;

pub mod funcs;
mod json;
pub mod lineproto;
pub mod query;
pub mod rollup;
pub mod server;
pub mod sketch;
pub mod snapshot;

pub mod compress;
pub mod config;
pub mod engine;
pub mod error;
pub mod model;

#[cfg(test)]
mod testutil;

pub use config::{Config, SyncMode};
pub use engine::{Engine, EngineStats};
pub use error::{Error, Result};
pub use metrics::Metrics;
pub use model::{Aggregate, Matcher, Point, Selector, SeriesKey};
