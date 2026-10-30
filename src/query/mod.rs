//! A small PromQL-flavoured query language on top of the engine.
//!
//! ```text
//! cpu{dc="fra", host!="web-9", rack=~"r-*"}          last value per series
//! rate(reqs[1m])                                     per-second counter rate
//! avg_over_time(cpu[5m])                             also sum/min/max/count/first/last/stddev
//! quantile_over_time(0.99, latency[5m])              p50 p90 p95 p99 shortcuts exist too
//! sum(rate(reqs[1m])) by (host)                      across-series aggregation
//! topk(5, max_over_time(cpu[1h]))
//! rate(reqs[1m]) * 60 > 100                          scalar arithmetic and filters
//! ```
//!
//! Durations are in ms (`5m` = 300000), so timestamps are assumed to be ms as well.

mod eval;
mod parse;

pub use eval::{eval, Ctx, Row, Value};
pub use parse::{parse, parse_duration, AggOp, BinOp, Expr};

use crate::engine::Engine;
use crate::error::Result;

pub fn run(engine: &Engine, text: &str, ctx: &Ctx) -> Result<Value> {
    let e = parse(text)?;
    eval(engine, &e, ctx)
}
