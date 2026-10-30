//! A small PromQL-flavoured query language on top of the engine.
//!
//! ```text
//! cpu{dc="fra", host!="web-9", rack=~"r-(1|2)"}      last value per series (=~ regex, !~ not, =* glob)
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
use crate::error::{Error, Result};
use crate::model::Selector;

// `cpu{host="a", dc=~"fra|ams"}` on its own, no range / offset / function around it
pub fn parse_selector(text: &str) -> Result<Selector> {
    match parse(text)? {
        Expr::Select(sel, None) => Ok(sel),
        _ => Err(Error::Query(format!(
            "expected a plain selector like name{{label=\"x\"}}, got {text:?}"
        ))),
    }
}

pub fn run(engine: &Engine, text: &str, ctx: &Ctx) -> Result<Value> {
    let e = parse(text)?;
    eval(engine, &e, ctx)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_selectors_only() {
        let s = parse_selector(r#"cpu{host="a", dc=~"fra|ams"}"#).unwrap();
        assert_eq!(s, Selector::new("cpu").eq("host", "a").re("dc", "fra|ams"));
        assert_eq!(parse_selector("cpu").unwrap(), Selector::new("cpu"));
        for bad in [
            "cpu[5m]",
            "rate(cpu[1m])",
            "cpu offset 1m",
            "sum(cpu)",
            "1",
            "cpu +",
            "",
        ] {
            assert!(parse_selector(bad).is_err(), "{bad:?}");
        }
    }
}
