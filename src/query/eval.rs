use std::collections::BTreeMap;

use super::parse::{AggOp, BinOp, Expr};
use crate::engine::Engine;
use crate::error::{Error, Result};
use crate::funcs::{top_k, Func};
use crate::model::{Aggregate, Point, Selector};

// evaluation window. without `step` the whole [start, end) collapses to one point at `end`
#[derive(Clone, Copy, Debug)]
pub struct Ctx {
    pub start: i64,
    pub end: i64,
    pub step: Option<i64>,
    // ts units per second, for rate()
    pub per_sec: i64,
}

impl Ctx {
    pub fn instant(start: i64, end: i64) -> Ctx {
        Ctx { start, end, step: None, per_sec: 1000 }
    }

    pub fn stepped(start: i64, end: i64, step: i64) -> Ctx {
        Ctx { start, end, step: Some(step), per_sec: 1000 }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Row {
    pub labels: BTreeMap<String, String>,
    pub points: Vec<Point>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Scalar(f64),
    Vector(Vec<Row>),
}

const MAX_STEPS: i64 = 100_000;
const NAME: &str = "__name__";

fn qerr<T>(m: impl Into<String>) -> Result<T> {
    Err(Error::Query(m.into()))
}

pub fn eval(engine: &Engine, e: &Expr, ctx: &Ctx) -> Result<Value> {
    if ctx.end <= ctx.start {
        return qerr("end must be after start");
    }
    match ctx.step {
        Some(s) if s <= 0 => return qerr("step must be positive"),
        Some(s) if (ctx.end - ctx.start) / s > MAX_STEPS => return qerr("too many steps"),
        _ => {}
    }
    go(engine, e, ctx)
}

fn go(engine: &Engine, e: &Expr, ctx: &Ctx) -> Result<Value> {
    match e {
        Expr::Num(n) => Ok(Value::Scalar(*n)),
        Expr::Select(sel, range) => reduce(engine, sel, *range, Func::Last, ctx, true),
        Expr::Call(f, sel, range) => reduce(engine, sel, *range, *f, ctx, false),
        Expr::Agg(op, by, arg) => match go(engine, arg, ctx)? {
            Value::Scalar(_) => qerr("cannot aggregate a scalar"),
            Value::Vector(rows) => Ok(Value::Vector(aggregate(*op, by, rows))),
        },
        Expr::TopK { k, largest, arg } => match go(engine, arg, ctx)? {
            Value::Scalar(_) => qerr("topk needs a vector"),
            Value::Vector(rows) => Ok(Value::Vector(rank(rows, *k, *largest))),
        },
        Expr::Bin(op, l, r) => {
            let (l, r) = (go(engine, l, ctx)?, go(engine, r, ctx)?);
            binary(*op, l, r)
        }
    }
}

// (label ts, window lo, window hi) triples, hi exclusive
fn windows(ctx: &Ctx, range: Option<i64>) -> Vec<(i64, i64, i64)> {
    match (ctx.step, range) {
        (None, None) => vec![(ctx.end, ctx.start, ctx.end)],
        (None, Some(r)) => vec![(ctx.end, ctx.end.saturating_sub(r).max(ctx.start), ctx.end)],
        (Some(s), None) => (0..)
            .map(|i| ctx.start + i * s)
            .take_while(|&w| w < ctx.end)
            .map(|w| (w, w, (w + s).min(ctx.end)))
            .collect(),
        // sliding: at each step t look back `r` (t included)
        (Some(s), Some(r)) => (0..)
            .map(|i| ctx.start + i * s)
            .take_while(|&t| t < ctx.end)
            .map(|t| (t, t - r + 1, t + 1))
            .collect(),
    }
}

fn reduce(
    engine: &Engine,
    sel: &Selector,
    range: Option<i64>,
    f: Func,
    ctx: &Ctx,
    keep_name: bool,
) -> Result<Value> {
    let wins = windows(ctx, range);
    let lo = wins.iter().map(|w| w.1).min().unwrap_or(ctx.start);
    let hi = wins.iter().map(|w| w.2).max().unwrap_or(ctx.end);

    let mut rows = Vec::new();
    for (key, pts) in engine.query_select(sel, lo, hi)? {
        let mut out = Vec::new();
        for &(t, a, b) in &wins {
            let i = pts.partition_point(|p| p.ts < a);
            let j = pts.partition_point(|p| p.ts < b);
            if let Some(v) = f.apply(&pts[i..j], ctx.per_sec) {
                out.push(Point::new(t, v));
            }
        }
        if out.is_empty() {
            continue;
        }
        let mut labels = key.labels().clone();
        if keep_name {
            labels.insert(NAME.into(), key.name().to_string());
        }
        rows.push(Row { labels, points: out });
    }
    rows.sort_by(|a, b| a.labels.cmp(&b.labels));
    Ok(Value::Vector(rows))
}

fn aggregate(op: AggOp, by: &[String], rows: Vec<Row>) -> Vec<Row> {
    let mut groups: BTreeMap<BTreeMap<String, String>, BTreeMap<i64, Aggregate>> = BTreeMap::new();
    for r in rows {
        let key: BTreeMap<String, String> = by
            .iter()
            .filter_map(|l| r.labels.get(l).map(|v| (l.clone(), v.clone())))
            .collect();
        let g = groups.entry(key).or_default();
        for p in &r.points {
            g.entry(p.ts).or_insert(Aggregate::EMPTY).add(p.value);
        }
    }
    groups
        .into_iter()
        .map(|(labels, g)| Row {
            labels,
            points: g
                .into_iter()
                .map(|(ts, a)| {
                    let v = match op {
                        AggOp::Sum => a.sum,
                        AggOp::Avg => a.sum / a.count as f64,
                        AggOp::Min => a.min,
                        AggOp::Max => a.max,
                        AggOp::Count => a.count as f64,
                    };
                    Point::new(ts, v)
                })
                .collect(),
        })
        .collect()
}

fn rank(rows: Vec<Row>, k: usize, largest: bool) -> Vec<Row> {
    let scored: Vec<(Row, f64)> = rows
        .into_iter()
        .filter_map(|r| {
            let v = r.points.last()?.value;
            Some((r, v))
        })
        .collect();
    top_k(scored, k, largest).into_iter().map(|x| x.0).collect()
}

fn arith(op: BinOp, a: f64, b: f64) -> Option<f64> {
    let v = match op {
        BinOp::Add => a + b,
        BinOp::Sub => a - b,
        BinOp::Mul => a * b,
        BinOp::Div => a / b,
        // filters: keep the left value when the comparison holds
        BinOp::Gt => return (a > b).then_some(a),
        BinOp::Lt => return (a < b).then_some(a),
        BinOp::Ge => return (a >= b).then_some(a),
        BinOp::Le => return (a <= b).then_some(a),
        BinOp::Eq => return (a == b).then_some(a),
    };
    v.is_finite().then_some(v)
}

fn is_cmp(op: BinOp) -> bool {
    matches!(op, BinOp::Gt | BinOp::Lt | BinOp::Ge | BinOp::Le | BinOp::Eq)
}

fn binary(op: BinOp, l: Value, r: Value) -> Result<Value> {
    match (l, r) {
        (Value::Scalar(_), Value::Scalar(_)) if is_cmp(op) => {
            qerr("comparison between two scalars is not supported")
        }
        (Value::Scalar(a), Value::Scalar(b)) => match arith(op, a, b) {
            Some(v) => Ok(Value::Scalar(v)),
            None => qerr("scalar result is not finite"),
        },
        (Value::Vector(rows), Value::Scalar(b)) => Ok(Value::Vector(map_rows(rows, |v| arith(op, v, b)))),
        (Value::Scalar(a), Value::Vector(rows)) => {
            if is_cmp(op) {
                return qerr("scalar must be on the right of a comparison");
            }
            Ok(Value::Vector(map_rows(rows, |v| arith(op, a, v))))
        }
        (Value::Vector(a), Value::Vector(b)) => {
            let mut right: BTreeMap<BTreeMap<String, String>, BTreeMap<i64, f64>> = BTreeMap::new();
            for r in b {
                right.insert(
                    without_name(r.labels),
                    r.points.iter().map(|p| (p.ts, p.value)).collect(),
                );
            }
            let mut out = Vec::new();
            for row in a {
                let labels = without_name(row.labels);
                let Some(rp) = right.get(&labels) else { continue };
                let pts: Vec<Point> = row
                    .points
                    .iter()
                    .filter_map(|p| {
                        let v = arith(op, p.value, *rp.get(&p.ts)?)?;
                        Some(Point::new(p.ts, v))
                    })
                    .collect();
                if !pts.is_empty() {
                    out.push(Row { labels, points: pts });
                }
            }
            Ok(Value::Vector(out))
        }
    }
}

fn without_name(mut l: BTreeMap<String, String>) -> BTreeMap<String, String> {
    l.remove(NAME);
    l
}

fn map_rows(rows: Vec<Row>, f: impl Fn(f64) -> Option<f64>) -> Vec<Row> {
    rows.into_iter()
        .filter_map(|r| {
            let pts: Vec<Point> = r
                .points
                .iter()
                .filter_map(|p| f(p.value).map(|v| Point::new(p.ts, v)))
                .collect();
            (!pts.is_empty()).then(|| Row { labels: without_name(r.labels), points: pts })
        })
        .collect()
}
