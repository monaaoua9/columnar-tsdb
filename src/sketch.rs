//! DDSketch: quantiles with a guaranteed relative error, mergeable, tiny.
//! Values land in log-spaced buckets (ratio gamma = (1+a)/(1-a)); a quantile is read back as the
//! midpoint of its bucket, which is within `a` of the true value for anything the sketch saw.

use std::collections::BTreeMap;

use crate::error::{Error, Result};
use crate::model::Point;

#[derive(Clone, Debug, PartialEq)]
pub struct Sketch {
    alpha: f64,
    ln_gamma: f64,
    pos: BTreeMap<i32, u64>,
    neg: BTreeMap<i32, u64>,
    zeros: u64,
    count: u64,
    min: f64,
    max: f64,
}

// anything this close to 0 goes in the zero bucket, log() of it would blow up the index range
const TINY: f64 = 1e-300;

impl Sketch {
    pub fn new(alpha: f64) -> Sketch {
        assert!(alpha > 0.0 && alpha < 1.0, "alpha must be in (0, 1)");
        Sketch {
            alpha,
            ln_gamma: ((1.0 + alpha) / (1.0 - alpha)).ln(),
            pos: BTreeMap::new(),
            neg: BTreeMap::new(),
            zeros: 0,
            count: 0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
        }
    }

    pub fn from_points(alpha: f64, pts: &[Point]) -> Sketch {
        let mut s = Sketch::new(alpha);
        for p in pts {
            s.add(p.value);
        }
        s
    }

    pub fn count(&self) -> u64 {
        self.count
    }

    pub fn alpha(&self) -> f64 {
        self.alpha
    }

    pub fn buckets(&self) -> usize {
        self.pos.len() + self.neg.len() + (self.zeros > 0) as usize
    }

    fn index(&self, x: f64) -> i32 {
        (x.ln() / self.ln_gamma).ceil() as i32
    }

    // midpoint estimate for bucket i: 2*gamma^i / (gamma+1)
    fn value(&self, i: i32) -> f64 {
        let g = self.ln_gamma.exp();
        2.0 * (i as f64 * self.ln_gamma).exp() / (g + 1.0)
    }

    pub fn add(&mut self, v: f64) {
        if !v.is_finite() {
            return;
        }
        self.count += 1;
        self.min = self.min.min(v);
        self.max = self.max.max(v);
        if v.abs() < TINY {
            self.zeros += 1;
        } else if v > 0.0 {
            *self.pos.entry(self.index(v)).or_insert(0) += 1;
        } else {
            *self.neg.entry(self.index(-v)).or_insert(0) += 1;
        }
    }

    // both sketches must have been built with the same alpha, otherwise the buckets don't line up
    pub fn merge(&mut self, o: &Sketch) -> Result<()> {
        if self.alpha != o.alpha {
            return Err(Error::BadConfig("sketch alpha mismatch"));
        }
        for (&i, &c) in &o.pos {
            *self.pos.entry(i).or_insert(0) += c;
        }
        for (&i, &c) in &o.neg {
            *self.neg.entry(i).or_insert(0) += c;
        }
        self.zeros += o.zeros;
        self.count += o.count;
        self.min = self.min.min(o.min);
        self.max = self.max.max(o.max);
        Ok(())
    }

    pub fn quantile(&self, q: f64) -> Option<f64> {
        if self.count == 0 || q.is_nan() {
            return None;
        }
        let q = q.clamp(0.0, 1.0);
        if q == 0.0 {
            return Some(self.min);
        }
        if q == 1.0 {
            return Some(self.max);
        }
        // rank is 0-based, walk buckets from the most negative value up
        let rank = (q * (self.count - 1) as f64).floor() as u64;
        let mut seen = 0u64;
        for (&i, &c) in self.neg.iter().rev() {
            seen += c;
            if seen > rank {
                return Some(-self.value(i));
            }
        }
        seen += self.zeros;
        if seen > rank {
            return Some(0.0);
        }
        for (&i, &c) in &self.pos {
            seen += c;
            if seen > rank {
                return Some(self.value(i));
            }
        }
        Some(self.max)
    }

    // alpha, count, min, max, zeros, then (npos, [idx, cnt]..) and the same for neg. little endian
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut b = Vec::with_capacity(48 + 12 * self.buckets());
        b.extend(self.alpha.to_le_bytes());
        b.extend(self.count.to_le_bytes());
        b.extend(self.min.to_le_bytes());
        b.extend(self.max.to_le_bytes());
        b.extend(self.zeros.to_le_bytes());
        for m in [&self.pos, &self.neg] {
            b.extend((m.len() as u32).to_le_bytes());
            for (&i, &c) in m {
                b.extend(i.to_le_bytes());
                b.extend(c.to_le_bytes());
            }
        }
        b
    }

    pub fn from_bytes(b: &[u8]) -> Result<Sketch> {
        struct Rd<'a>(&'a [u8]);
        impl Rd<'_> {
            fn take<const N: usize>(&mut self) -> Result<[u8; N]> {
                if self.0.len() < N {
                    return Err(Error::corrupt("sketch truncated"));
                }
                let (h, t) = self.0.split_at(N);
                self.0 = t;
                Ok(h.try_into().unwrap())
            }
        }
        let mut r = Rd(b);
        let alpha = f64::from_le_bytes(r.take()?);
        if !(alpha > 0.0 && alpha < 1.0) {
            return Err(Error::corrupt("sketch alpha out of range"));
        }
        let mut s = Sketch::new(alpha);
        s.count = u64::from_le_bytes(r.take()?);
        s.min = f64::from_le_bytes(r.take()?);
        s.max = f64::from_le_bytes(r.take()?);
        s.zeros = u64::from_le_bytes(r.take()?);
        let mut total = s.zeros;
        for neg in [false, true] {
            let n = u32::from_le_bytes(r.take()?);
            for _ in 0..n {
                let i = i32::from_le_bytes(r.take()?);
                let c = u64::from_le_bytes(r.take()?);
                total = total.saturating_add(c);
                if neg { &mut s.neg } else { &mut s.pos }.insert(i, c);
            }
        }
        if !r.0.is_empty() || total != s.count {
            return Err(Error::corrupt("sketch counts don't add up"));
        }
        Ok(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // tiny lcg, no rand crate here
    fn rng(seed: &mut u64) -> f64 {
        *seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        (*seed >> 11) as f64 / (1u64 << 53) as f64
    }

    fn exact(sorted: &[f64], q: f64) -> f64 {
        sorted[(q * (sorted.len() - 1) as f64).floor() as usize]
    }

    #[test]
    fn relative_error_holds() {
        let mut seed = 7;
        let mut vals: Vec<f64> = (0..20_000).map(|_| (rng(&mut seed) * 12.0).exp()).collect();
        let mut sk = Sketch::new(0.01);
        for &v in &vals {
            sk.add(v);
        }
        vals.sort_by(|a, b| a.total_cmp(b));
        for q in [0.01, 0.25, 0.5, 0.9, 0.99, 0.999] {
            let (got, want) = (sk.quantile(q).unwrap(), exact(&vals, q));
            assert!((got - want).abs() / want <= 0.0101, "q={q} got={got} want={want}");
        }
        // 5 decades of range fit in a few hundred buckets
        assert!(sk.buckets() < 1500, "{}", sk.buckets());
    }

    #[test]
    fn negatives_zero_and_extremes() {
        let mut sk = Sketch::new(0.02);
        for v in [-100.0, -1.0, 0.0, 0.0, 1.0, 100.0] {
            sk.add(v);
        }
        assert_eq!(sk.quantile(0.0), Some(-100.0));
        assert_eq!(sk.quantile(1.0), Some(100.0));
        assert_eq!(sk.quantile(0.5), Some(0.0));
        let lo = sk.quantile(0.1).unwrap();
        assert!((lo + 100.0).abs() / 100.0 <= 0.021, "{lo}");
    }

    #[test]
    fn merge_equals_single_pass() {
        let mut seed = 3;
        let (mut a, mut b, mut all) = (Sketch::new(0.01), Sketch::new(0.01), Sketch::new(0.01));
        for i in 0..5000 {
            let v = rng(&mut seed) * 1000.0 - 300.0;
            all.add(v);
            if i % 2 == 0 { &mut a } else { &mut b }.add(v);
        }
        a.merge(&b).unwrap();
        assert_eq!(a, all);
        assert!(a.merge(&Sketch::new(0.05)).is_err());
    }

    #[test]
    fn bytes_round_trip_and_rejects_damage() {
        let mut sk = Sketch::new(0.01);
        for v in [-5.0, 0.0, 3.0, 3.1, 900.0] {
            sk.add(v);
        }
        let b = sk.to_bytes();
        assert_eq!(Sketch::from_bytes(&b).unwrap(), sk);
        assert!(Sketch::from_bytes(&b[..b.len() - 1]).is_err());
        let mut longer = b.clone();
        longer.push(0);
        assert!(Sketch::from_bytes(&longer).is_err());
        let mut bad = b;
        bad[8] ^= 1; // count no longer matches the buckets
        assert!(Sketch::from_bytes(&bad).is_err());
    }

    #[test]
    fn empty_and_nonfinite() {
        let mut sk = Sketch::new(0.01);
        assert_eq!(sk.quantile(0.5), None);
        sk.add(f64::NAN);
        sk.add(f64::INFINITY);
        assert_eq!(sk.count(), 0);
        sk.add(4.0);
        assert_eq!(sk.quantile(0.5).map(|v| (v - 4.0).abs() / 4.0 < 0.011), Some(true));
    }
}
