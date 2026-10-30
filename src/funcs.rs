//! Reductions over raw points. The engine answers sum/min/max/count from chunk stats; these
//! need the points themselves, so they run on whatever `query_range` returned.

use crate::model::Point;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Func {
    Count,
    Sum,
    Min,
    Max,
    Avg,
    First,
    Last,
    Stddev,
    Stdvar,
    Quantile(f64),
    Delta,
    // counter-aware: a drop in value is treated as a reset to zero
    Increase,
    // increase divided by the time span, scaled to `per` ts units
    Rate,
}

impl Func {
    pub fn parse(name: &str, arg: Option<f64>) -> Option<Func> {
        Some(match name {
            "count" => Func::Count,
            "sum" => Func::Sum,
            "min" => Func::Min,
            "max" => Func::Max,
            "avg" => Func::Avg,
            "first" => Func::First,
            "last" => Func::Last,
            "stddev" => Func::Stddev,
            "stdvar" => Func::Stdvar,
            "delta" => Func::Delta,
            "increase" => Func::Increase,
            "rate" => Func::Rate,
            "quantile" => Func::Quantile(arg?),
            "p50" => Func::Quantile(0.5),
            "p90" => Func::Quantile(0.9),
            "p95" => Func::Quantile(0.95),
            "p99" => Func::Quantile(0.99),
            _ => return None,
        })
    }

    // None when the input is too short for the function (rate needs two points, etc)
    pub fn apply(&self, pts: &[Point], per: i64) -> Option<f64> {
        if pts.is_empty() {
            return None;
        }
        match *self {
            Func::Count => Some(pts.len() as f64),
            Func::Sum => Some(pts.iter().map(|p| p.value).sum()),
            Func::Min => pts.iter().map(|p| p.value).reduce(f64::min),
            Func::Max => pts.iter().map(|p| p.value).reduce(f64::max),
            Func::Avg => Some(pts.iter().map(|p| p.value).sum::<f64>() / pts.len() as f64),
            Func::First => pts.first().map(|p| p.value),
            Func::Last => pts.last().map(|p| p.value),
            Func::Stddev => variance(pts).map(f64::sqrt),
            Func::Stdvar => variance(pts),
            Func::Quantile(q) => quantile(pts, q),
            Func::Delta => delta(pts),
            Func::Increase => increase(pts),
            Func::Rate => rate(pts, per),
        }
    }
}

// population variance, welford so big offsets don't eat the precision
pub fn variance(pts: &[Point]) -> Option<f64> {
    if pts.is_empty() {
        return None;
    }
    let (mut mean, mut m2) = (0.0, 0.0);
    for (i, p) in pts.iter().enumerate() {
        let d = p.value - mean;
        mean += d / (i + 1) as f64;
        m2 += d * (p.value - mean);
    }
    Some(m2 / pts.len() as f64)
}

// exact quantile, linear interpolation between the two nearest ranks. q outside 0..=1 clamps
pub fn quantile(pts: &[Point], q: f64) -> Option<f64> {
    if pts.is_empty() || q.is_nan() {
        return None;
    }
    let mut v: Vec<f64> = pts.iter().map(|p| p.value).collect();
    v.sort_by(|a, b| a.total_cmp(b));
    let pos = q.clamp(0.0, 1.0) * (v.len() - 1) as f64;
    let (lo, hi) = (pos.floor() as usize, pos.ceil() as usize);
    Some(v[lo] + (v[hi] - v[lo]) * (pos - lo as f64))
}

pub fn delta(pts: &[Point]) -> Option<f64> {
    if pts.len() < 2 {
        return None;
    }
    Some(pts[pts.len() - 1].value - pts[0].value)
}

pub fn increase(pts: &[Point]) -> Option<f64> {
    if pts.len() < 2 {
        return None;
    }
    let mut total = 0.0;
    for w in pts.windows(2) {
        let d = w[1].value - w[0].value;
        // reset: the counter restarted from 0, so everything it reads now is new
        total += if d < 0.0 { w[1].value } else { d };
    }
    Some(total)
}

pub fn rate(pts: &[Point], per: i64) -> Option<f64> {
    let inc = increase(pts)?;
    let span = pts[pts.len() - 1].ts - pts[0].ts;
    if span <= 0 {
        return None;
    }
    Some(inc / span as f64 * per as f64)
}

// per-point slope (value units per `per` ts units); the first point has no predecessor
pub fn derivative(pts: &[Point], per: i64) -> Vec<Point> {
    pts.windows(2)
        .filter(|w| w[1].ts != w[0].ts)
        .map(|w| {
            let dv = w[1].value - w[0].value;
            let dt = (w[1].ts - w[0].ts) as f64;
            Point::new(w[1].ts, dv / dt * per as f64)
        })
        .collect()
}

// trailing average over the last `n` points (fewer at the start)
pub fn moving_avg(pts: &[Point], n: usize) -> Vec<Point> {
    let n = n.max(1);
    let mut out = Vec::with_capacity(pts.len());
    let mut sum = 0.0;
    for (i, p) in pts.iter().enumerate() {
        sum += p.value;
        if i >= n {
            sum -= pts[i - n].value;
        }
        out.push(Point::new(p.ts, sum / (i + 1).min(n) as f64));
    }
    out
}

// exponentially weighted; alpha in (0, 1], 1 means no smoothing
pub fn ewma(pts: &[Point], alpha: f64) -> Vec<Point> {
    let mut out = Vec::with_capacity(pts.len());
    let mut cur: Option<f64> = None;
    for p in pts {
        let v = match cur {
            Some(c) => alpha * p.value + (1.0 - alpha) * c,
            None => p.value,
        };
        cur = Some(v);
        out.push(Point::new(p.ts, v));
    }
    out
}

// rank series by a reduction and keep the best k. ties keep input order
pub fn top_k<K>(items: Vec<(K, f64)>, k: usize, largest: bool) -> Vec<(K, f64)> {
    let mut items = items;
    items.sort_by(|a, b| {
        let o = a.1.total_cmp(&b.1);
        if largest {
            o.reverse()
        } else {
            o
        }
    });
    items.truncate(k);
    items
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pts(v: &[f64]) -> Vec<Point> {
        v.iter()
            .enumerate()
            .map(|(i, &x)| Point::new(i as i64 * 1000, x))
            .collect()
    }

    #[test]
    fn quantile_interpolates() {
        let p = pts(&[4.0, 1.0, 3.0, 2.0]);
        assert_eq!(quantile(&p, 0.0), Some(1.0));
        assert_eq!(quantile(&p, 1.0), Some(4.0));
        assert_eq!(quantile(&p, 0.5), Some(2.5));
        assert_eq!(quantile(&p, 7.0), Some(4.0));
        assert_eq!(quantile(&[], 0.5), None);
    }

    #[test]
    fn variance_matches_naive() {
        let p = pts(&[2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0]);
        assert_eq!(variance(&p), Some(4.0));
        assert_eq!(Func::Stddev.apply(&p, 1), Some(2.0));
        // large offset would wreck a sum-of-squares formula
        let big = pts(&[1e9 + 1.0, 1e9 + 2.0, 1e9 + 3.0]);
        let v = variance(&big).unwrap();
        assert!((v - 2.0 / 3.0).abs() < 1e-6, "{v}");
    }

    #[test]
    fn increase_handles_counter_reset() {
        let p = pts(&[10.0, 15.0, 20.0, 3.0, 8.0]);
        // 5 + 5 + (reset: 3) + 5
        assert_eq!(increase(&p), Some(18.0));
        assert_eq!(delta(&p), Some(-2.0));
        assert_eq!(increase(&p[..1]), None);
    }

    #[test]
    fn rate_scales_to_requested_unit() {
        let p = pts(&[0.0, 10.0, 20.0]); // +10 per second (ts in ms)
        assert_eq!(rate(&p, 1000), Some(10.0));
        assert_eq!(rate(&p, 60_000), Some(600.0));
        assert_eq!(rate(&[Point::new(5, 1.0), Point::new(5, 2.0)], 1000), None);
    }

    #[test]
    fn derivative_skips_duplicate_ts() {
        let p = [Point::new(0, 0.0), Point::new(1000, 5.0), Point::new(1000, 9.0)];
        let d = derivative(&p, 1000);
        assert_eq!(d, vec![Point::new(1000, 5.0)]);
    }

    #[test]
    fn moving_average_window() {
        let p = pts(&[1.0, 2.0, 3.0, 4.0]);
        let m: Vec<f64> = moving_avg(&p, 2).iter().map(|p| p.value).collect();
        assert_eq!(m, vec![1.0, 1.5, 2.5, 3.5]);
        assert_eq!(moving_avg(&p, 0).len(), 4);
    }

    #[test]
    fn ewma_alpha_one_is_identity() {
        let p = pts(&[1.0, 5.0, 2.0]);
        assert_eq!(ewma(&p, 1.0), p);
        let e = ewma(&p, 0.5);
        assert_eq!(e[1].value, 3.0);
    }

    #[test]
    fn top_k_both_directions() {
        let v = vec![("a", 3.0), ("b", 9.0), ("c", 1.0), ("d", 9.0)];
        let hi = top_k(v.clone(), 2, true);
        assert_eq!(hi.iter().map(|x| x.0).collect::<Vec<_>>(), vec!["b", "d"]);
        let lo = top_k(v, 1, false);
        assert_eq!(lo[0].0, "c");
    }

    #[test]
    fn parse_names() {
        assert_eq!(Func::parse("p99", None), Some(Func::Quantile(0.99)));
        assert_eq!(Func::parse("quantile", None), None);
        assert_eq!(Func::parse("quantile", Some(0.3)), Some(Func::Quantile(0.3)));
        assert_eq!(Func::parse("nope", None), None);
    }
}
