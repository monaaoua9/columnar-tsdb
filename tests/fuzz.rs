// mutation fuzzing without a fuzzer: take inputs that are valid, flip / drop / duplicate / insert
// bytes and see that the parsers answer Ok or Err. A panic or a hang fails the test.
// fixed seeds, kept short on purpose
use std::time::Instant;

use columnar_tsdb::lineproto::{parse_line, parse_lines};
use columnar_tsdb::query::{parse, parse_selector};
use columnar_tsdb::regex::Regex;

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
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

// bytes that mean something to at least one of the parsers
const SPICY: &[u8] = b"{}[]()\"'\\,= |*+?.^$-:/%@#\n\t0e9";

fn mutate(r: &mut Rng, seed: &str) -> String {
    let mut b = seed.as_bytes().to_vec();
    for _ in 0..1 + r.below(6) {
        if b.is_empty() {
            b.push(SPICY[r.below(SPICY.len())]);
            continue;
        }
        let i = r.below(b.len());
        match r.below(5) {
            0 => b[i] = SPICY[r.below(SPICY.len())],
            1 => b[i] = r.next() as u8,
            2 => {
                b.remove(i);
            }
            3 => b.insert(i, SPICY[r.below(SPICY.len())]),
            _ => {
                let j = r.below(b.len());
                let (lo, hi) = (i.min(j), i.max(j));
                let chunk = b[lo..hi].to_vec();
                b.splice(hi..hi, chunk);
            }
        }
    }
    // not always valid utf8 after the byte flips; the parsers take &str like the real callers
    String::from_utf8_lossy(&b).into_owned()
}

const QUERIES: &[&str] = &[
    r#"cpu{host="a",dc=~"fra|ams"}"#,
    r#"sum by (dc) (rate(cpu{host!="b"}[5m] offset 1h))"#,
    r#"topk(3, avg_over_time(cpu[1h])) > 10"#,
    r#"quantile_over_time(0.9, m{a=*"x?",b!~"y+"}[30s]) * 2 + 1"#,
    "count(a) / count(b)",
];

const LINES: &[&str] = &[
    "cpu,host=a,dc=fra value=1.5 1700000000000",
    "cpu value=-3e5",
    r#"we\ ird,k\,ey=v\=al value=0 12"#,
    "m,a=b value=NaN 5\nm,a=c value=7 6\n",
];

const PATTERNS: &[&str] = &[
    "fra|ams|iad",
    "h0[0-4].",
    "(ab)*c?d+",
    r"\d{3}-[a-z]*",
    "^x.*y$",
    "[^a-c]+(foo|bar)?",
];

#[test]
fn query_and_selector_parsers_survive_mutation() {
    let mut r = Rng::new(11);
    for _ in 0..4_000 {
        let seed = QUERIES[r.below(QUERIES.len())];
        let s = mutate(&mut r, seed);
        let _ = parse(&s);
        let _ = parse_selector(&s);
    }
}

#[test]
fn line_protocol_survives_mutation() {
    let mut r = Rng::new(12);
    for _ in 0..4_000 {
        let seed = LINES[r.below(LINES.len())];
        let s = mutate(&mut r, seed);
        let _ = parse_line(&s, 0);
        let _ = parse_lines(&s, 0);
    }
}

#[test]
fn regex_compiles_and_matches_without_panics() {
    let mut r = Rng::new(13);
    let hay = ["", "fra", "h03x", "abababcd", "123-abc", "xyzzy", "ünïcode"];
    for _ in 0..4_000 {
        let seed = PATTERNS[r.below(PATTERNS.len())];
        let p = mutate(&mut r, seed);
        if let Ok(re) = Regex::new(&p) {
            for h in hay {
                let _ = re.is_match(h);
            }
        }
    }
}

// the engine is an nfa simulation, so classic backtracking bombs must stay linear
#[test]
fn regex_has_no_exponential_blowup() {
    let long = "a".repeat(20_000);
    for p in ["(a*)*b", "(a|aa)+$", "(a+)+c", "(.*)*x", "((a?){30})a{30}"] {
        let re = Regex::new(p).unwrap();
        let t = Instant::now();
        let _ = re.is_match(&long);
        assert!(
            t.elapsed().as_secs() < 5,
            "{p} took {:?} on 20k chars",
            t.elapsed()
        );
    }
}

// a pattern that nests deeper than anyone writes by hand must be an error or work, not overflow
// the stack
#[test]
fn deep_nesting_does_not_overflow() {
    let n = 5_000;
    let pat = format!("{}a{}", "(".repeat(n), ")".repeat(n));
    let _ = Regex::new(&pat).map(|re| re.is_match("a"));
    let q = format!("{}a{}", "(".repeat(n), ")".repeat(n));
    let _ = parse(&q);
}
