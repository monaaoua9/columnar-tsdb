// randomized property checks, fixed seeds so a failure reproduces. no proptest: no dependencies

use columnar_tsdb::compress::{gorilla, timestamp};
use columnar_tsdb::funcs;
use columnar_tsdb::lineproto::{format_line, parse_line};
use columnar_tsdb::query::parse;
use columnar_tsdb::sketch::Sketch;
use columnar_tsdb::{Point, SeriesKey};

struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Rng {
        Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
    fn unit(&mut self) -> f64 {
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }
}

// strictly increasing timestamps with a mix of regular and wild gaps
fn gen_ts(r: &mut Rng, n: usize) -> Vec<i64> {
    let mut t = r.next() as i64 >> 2;
    let mut out = Vec::with_capacity(n);
    let step = 1 + r.below(5000) as i64;
    for _ in 0..n {
        out.push(t);
        t = t.saturating_add(match r.below(10) {
            0 => 1 + r.below(1 << 40) as i64,
            1 => step + r.below(50) as i64,
            _ => step,
        });
    }
    out.dedup();
    out
}

fn gen_vals(r: &mut Rng, n: usize) -> Vec<f64> {
    let kind = r.below(5);
    (0..n)
        .map(|i| match kind {
            0 => 42.0,
            1 => (i as f64 * 0.1).sin() * 100.0,
            2 => f64::from_bits(r.next()),
            3 => (r.below(1000) as f64) / 100.0,
            _ => i as f64,
        })
        .filter(|v| v.is_finite())
        .collect()
}

#[test]
fn timestamp_codec_roundtrips() {
    let mut r = Rng::new(1);
    for round in 0..400 {
        let n = r.below(600) as usize;
        let ts = gen_ts(&mut r, n);
        let back = timestamp::decode(&timestamp::encode(&ts), usize::MAX).unwrap();
        assert_eq!(back, ts, "round {round}");
    }
}

#[test]
fn gorilla_codec_roundtrips_bit_exact() {
    let mut r = Rng::new(2);
    for round in 0..400 {
        let n = r.below(600) as usize;
        let v = gen_vals(&mut r, n);
        let back = gorilla::decode(&gorilla::encode(&v), usize::MAX).unwrap();
        assert_eq!(back.len(), v.len(), "round {round}");
        for (a, b) in back.iter().zip(&v) {
            assert_eq!(a.to_bits(), b.to_bits(), "round {round}");
        }
    }
}

#[test]
fn constant_series_compress_hard() {
    let ts: Vec<i64> = (0..10_000)
        .map(|i| 1_700_000_000_000 + i * 10_000)
        .collect();
    let v = vec![3.5; 10_000];
    // about one bit per point each, plus headers
    assert!(timestamp::encode(&ts).len() < 1500);
    assert!(gorilla::encode(&v).len() < 1500);
}

fn gen_text(r: &mut Rng, max: u64) -> String {
    const ALPHA: &[char] = &[
        'a', 'z', '0', '_', '-', ' ', ',', '=', '\\', '"', '{', '.', 'é', 'ع',
    ];
    (0..r.below(max))
        .map(|_| ALPHA[r.below(ALPHA.len() as u64) as usize])
        .collect()
}

#[test]
fn line_protocol_roundtrips_hostile_names() {
    let mut r = Rng::new(3);
    let mut checked = 0;
    for _ in 0..3000 {
        let name = gen_text(&mut r, 8);
        if name.is_empty() {
            continue;
        }
        let mut k = SeriesKey::new(name);
        for _ in 0..r.below(4) {
            let lk = gen_text(&mut r, 6);
            if lk.is_empty() {
                continue;
            }
            k = k.label(lk, gen_text(&mut r, 8));
        }
        let p = Point::new(r.next() as i64, (r.unit() - 0.5) * 1e6);
        let text = format_line(&k, &p);
        let back = parse_line(&text, 0).unwrap_or_else(|e| panic!("{text:?}: {e}"));
        assert_eq!(back.key, k, "{text:?}");
        assert_eq!((back.ts, back.value), (p.ts, p.value), "{text:?}");
        checked += 1;
    }
    assert!(checked > 2000);
}

const NOISE: &[char] = &[
    '(', ')', '{', '}', '[', ']', ',', '"', '=', '!', '~', '<', '>', '+', '-', '*', '/', ' ', '0',
    'a', '_', ':', 'é', '\\',
];

#[test]
fn query_parser_never_panics() {
    let seeds = [
        r#"sum(rate(reqs{dc="fra",host!="x"}[5m])) by (host)"#,
        "topk(3, avg_over_time(cpu[1h30m])) * 100 > 5",
        "quantile_over_time(0.99, lat[10s])",
        "-(a + b) / 2",
    ];
    let mut r = Rng::new(4);
    for _ in 0..30_000 {
        let mut b: Vec<char> = seeds[r.below(seeds.len() as u64) as usize]
            .chars()
            .collect();
        for _ in 0..1 + r.below(4) {
            if b.is_empty() {
                break;
            }
            let i = r.below(b.len() as u64) as usize;
            match r.below(3) {
                0 => {
                    b.remove(i);
                }
                1 => b.insert(i, NOISE[r.below(NOISE.len() as u64) as usize]),
                _ => b.truncate(i),
            }
        }
        let s: String = b.into_iter().collect();
        let _ = parse(&s); // Ok or Err, both fine; a panic fails the test
    }
}

#[test]
fn sketch_quantiles_are_monotone_and_bounded() {
    let mut r = Rng::new(5);
    for _ in 0..50 {
        let n = 1 + r.below(2000) as usize;
        let vals: Vec<f64> = (0..n)
            .map(|_| (r.unit() - 0.3) * 10f64.powi(r.below(8) as i32))
            .collect();
        let pts: Vec<Point> = vals
            .iter()
            .enumerate()
            .map(|(i, &v)| Point::new(i as i64, v))
            .collect();
        let sk = Sketch::from_points(0.02, &pts);
        let (lo, hi) = (
            vals.iter().cloned().fold(f64::MAX, f64::min),
            vals.iter().cloned().fold(f64::MIN, f64::max),
        );
        let mut prev = f64::NEG_INFINITY;
        for i in 0..=20 {
            let q = sk.quantile(i as f64 / 20.0).unwrap();
            assert!(q >= prev - 1e-9, "not monotone: {prev} then {q}");
            // bucket midpoints may overshoot the extremes by alpha, not more
            assert!(q >= lo - lo.abs() * 0.021 - 1e-9 && q <= hi + hi.abs() * 0.021 + 1e-9);
            prev = q;
        }
    }
}

#[test]
fn exact_quantile_matches_sorted_order() {
    let mut r = Rng::new(6);
    for _ in 0..200 {
        let n = 1 + r.below(300) as usize;
        let pts: Vec<Point> = (0..n)
            .map(|i| Point::new(i as i64, r.unit() * 100.0))
            .collect();
        let mut sorted: Vec<f64> = pts.iter().map(|p| p.value).collect();
        sorted.sort_by(|a, b| a.total_cmp(b));
        assert_eq!(funcs::quantile(&pts, 0.0), Some(sorted[0]));
        assert_eq!(funcs::quantile(&pts, 1.0), Some(sorted[n - 1]));
        let mut prev = f64::NEG_INFINITY;
        for i in 0..=10 {
            let q = funcs::quantile(&pts, i as f64 / 10.0).unwrap();
            assert!(q >= prev);
            prev = q;
        }
    }
}

#[test]
fn increase_is_never_negative_and_bounded_by_delta_sum() {
    let mut r = Rng::new(7);
    for _ in 0..300 {
        let n = 2 + r.below(50) as usize;
        let pts: Vec<Point> = (0..n)
            .map(|i| Point::new(i as i64 * 1000, r.below(1000) as f64))
            .collect();
        let inc = funcs::increase(&pts).unwrap();
        assert!(inc >= 0.0);
        // every step contributes at most max(value) so the total can't exceed (n-1) * max
        let max = pts.iter().map(|p| p.value).fold(0.0, f64::max);
        assert!(inc <= (n - 1) as f64 * max);
        // a monotone series has no resets: increase == delta
        let mono: Vec<Point> = pts
            .iter()
            .scan(0.0, |acc, p| {
                *acc += p.value;
                Some(Point::new(p.ts, *acc))
            })
            .collect();
        assert_eq!(funcs::increase(&mono), funcs::delta(&mono));
    }
}
