use std::collections::BTreeMap;

pub type SeriesId = u64;

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Point {
    pub ts: i64,
    pub value: f64,
}

impl Point {
    pub fn new(ts: i64, value: f64) -> Self {
        Point { ts, value }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SeriesKey {
    name: String,
    labels: BTreeMap<String, String>,
}

impl SeriesKey {
    pub fn new(name: impl Into<String>) -> Self {
        SeriesKey {
            name: name.into(),
            labels: BTreeMap::new(),
        }
    }

    pub fn label(mut self, k: impl Into<String>, v: impl Into<String>) -> Self {
        self.labels.insert(k.into(), v.into());
        self
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn labels(&self) -> &BTreeMap<String, String> {
        &self.labels
    }

    // fnv-1a over the canonical form; must stay stable, ids are persisted in the catalog + wal
    pub fn id(&self) -> SeriesId {
        let mut h: u64 = 0xcbf2_9ce4_8422_2325;
        let mut feed = |bytes: &[u8]| {
            for &b in bytes {
                h ^= b as u64;
                h = h.wrapping_mul(0x0000_0100_0000_01b3);
            }
            h ^= 0xff; // separator so ("ab","c") != ("a","bc")
            h = h.wrapping_mul(0x0000_0100_0000_01b3);
        };
        feed(self.name.as_bytes());
        for (k, v) in &self.labels {
            feed(k.as_bytes());
            feed(v.as_bytes());
        }
        h
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Matcher {
    // label must be present with exactly this value
    Eq(String, String),
    // label absent, or present with a different value
    Ne(String, String),
    // label present and its value matches the glob (`*` any run, `?` one char)
    Like(String, String),
}

// iterative glob with single-star backtracking; no recursion, no allocation beyond the char vecs
pub fn glob_match(pat: &str, s: &str) -> bool {
    let p: Vec<char> = pat.chars().collect();
    let t: Vec<char> = s.chars().collect();
    let (mut i, mut j) = (0, 0);
    let mut star: Option<usize> = None;
    let mut mark = 0;
    while j < t.len() {
        if i < p.len() && (p[i] == '?' || p[i] == t[j]) {
            i += 1;
            j += 1;
        } else if i < p.len() && p[i] == '*' {
            star = Some(i);
            mark = j;
            i += 1;
        } else if let Some(si) = star {
            i = si + 1;
            mark += 1;
            j = mark;
        } else {
            return false;
        }
    }
    while i < p.len() && p[i] == '*' {
        i += 1;
    }
    i == p.len()
}

// metric name plus label conditions, all of which must hold
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Selector {
    pub name: String,
    pub matchers: Vec<Matcher>,
}

impl Selector {
    pub fn new(name: impl Into<String>) -> Self {
        Selector {
            name: name.into(),
            matchers: Vec::new(),
        }
    }

    pub fn eq(mut self, k: impl Into<String>, v: impl Into<String>) -> Self {
        self.matchers.push(Matcher::Eq(k.into(), v.into()));
        self
    }

    pub fn ne(mut self, k: impl Into<String>, v: impl Into<String>) -> Self {
        self.matchers.push(Matcher::Ne(k.into(), v.into()));
        self
    }

    pub fn like(mut self, k: impl Into<String>, pattern: impl Into<String>) -> Self {
        self.matchers.push(Matcher::Like(k.into(), pattern.into()));
        self
    }

    // slow reference check; the engine resolves selectors through the label index instead
    pub fn matches(&self, key: &SeriesKey) -> bool {
        key.name() == self.name
            && self.matchers.iter().all(|m| match m {
                Matcher::Eq(k, v) => key.labels().get(k) == Some(v),
                Matcher::Ne(k, v) => key.labels().get(k) != Some(v),
                Matcher::Like(k, pat) => key.labels().get(k).is_some_and(|v| glob_match(pat, v)),
            })
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Aggregate {
    pub count: u64,
    pub sum: f64,
    pub min: f64,
    pub max: f64,
}

impl Aggregate {
    pub const EMPTY: Aggregate = Aggregate {
        count: 0,
        sum: 0.0,
        min: f64::INFINITY,
        max: f64::NEG_INFINITY,
    };

    pub fn add(&mut self, v: f64) {
        self.count += 1;
        self.sum += v;
        if v < self.min {
            self.min = v;
        }
        if v > self.max {
            self.max = v;
        }
    }

    pub fn merge(&mut self, o: &Aggregate) {
        self.count += o.count;
        self.sum += o.sum;
        self.min = self.min.min(o.min);
        self.max = self.max.max(o.max);
    }

    pub fn avg(&self) -> Option<f64> {
        (self.count > 0).then(|| self.sum / self.count as f64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn id_is_stable_and_label_order_independent() {
        let a = SeriesKey::new("cpu").label("host", "a").label("dc", "eu");
        let b = SeriesKey::new("cpu").label("dc", "eu").label("host", "a");
        assert_eq!(a.id(), b.id());
        assert_ne!(a.id(), SeriesKey::new("cpu").label("host", "b").id());
        assert_ne!(
            SeriesKey::new("ab").label("c", "d").id(),
            SeriesKey::new("a").label("bc", "d").id()
        );
    }

    #[test]
    fn aggregate_merge() {
        let mut a = Aggregate::EMPTY;
        a.add(1.0);
        a.add(5.0);
        let mut b = Aggregate::EMPTY;
        b.add(-2.0);
        a.merge(&b);
        assert_eq!((a.count, a.sum, a.min, a.max), (3, 4.0, -2.0, 5.0));
        assert_eq!(Aggregate::EMPTY.avg(), None);
    }
}
