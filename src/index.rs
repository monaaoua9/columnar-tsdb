use std::collections::HashMap;

use crate::model::{Matcher, Selector, SeriesId, SeriesKey};

// Inverted index: metric name -> series ids, and (label, value) -> series ids. Every list
// stays sorted so selectors resolve with merge-style intersect / subtract.
#[derive(Default)]
pub struct LabelIndex {
    by_name: HashMap<String, Vec<SeriesId>>,
    postings: HashMap<(String, String), Vec<SeriesId>>,
}

fn add_sorted(v: &mut Vec<SeriesId>, id: SeriesId) {
    if let Err(at) = v.binary_search(&id) {
        v.insert(at, id);
    }
}

fn intersect(a: &[SeriesId], b: &[SeriesId]) -> Vec<SeriesId> {
    let mut out = Vec::with_capacity(a.len().min(b.len()));
    let (mut i, mut j) = (0, 0);
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
            std::cmp::Ordering::Equal => {
                out.push(a[i]);
                i += 1;
                j += 1;
            }
        }
    }
    out
}

fn subtract(a: &[SeriesId], b: &[SeriesId]) -> Vec<SeriesId> {
    a.iter()
        .copied()
        .filter(|id| b.binary_search(id).is_err())
        .collect()
}

impl LabelIndex {
    pub fn insert(&mut self, key: &SeriesKey, id: SeriesId) {
        add_sorted(self.by_name.entry(key.name().to_string()).or_default(), id);
        for (k, v) in key.labels() {
            add_sorted(self.postings.entry((k.clone(), v.clone())).or_default(), id);
        }
    }

    pub fn resolve(&self, sel: &Selector) -> Vec<SeriesId> {
        let Some(base) = self.by_name.get(&sel.name) else {
            return Vec::new();
        };
        let mut ids = base.clone();
        for m in &sel.matchers {
            match m {
                Matcher::Eq(k, v) => match self.postings.get(&(k.clone(), v.clone())) {
                    Some(p) => ids = intersect(&ids, p),
                    None => return Vec::new(),
                },
                Matcher::Ne(k, v) => {
                    if let Some(p) = self.postings.get(&(k.clone(), v.clone())) {
                        ids = subtract(&ids, p);
                    }
                }
            }
            if ids.is_empty() {
                break;
            }
        }
        ids
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_helpers() {
        assert_eq!(intersect(&[1, 3, 5, 7], &[3, 4, 5, 9]), vec![3, 5]);
        assert_eq!(subtract(&[1, 3, 5, 7], &[3, 7]), vec![1, 5]);
        assert!(intersect(&[], &[1]).is_empty());
    }

    #[test]
    fn resolve_matches_reference_implementation() {
        let keys: Vec<SeriesKey> = (0..40)
            .map(|i| {
                let mut k = SeriesKey::new(if i % 5 == 0 { "mem" } else { "cpu" })
                    .label("host", format!("h{}", i % 7))
                    .label("dc", if i % 2 == 0 { "fra" } else { "ams" });
                if i % 3 == 0 {
                    k = k.label("role", "db");
                }
                k
            })
            .collect();
        let mut idx = LabelIndex::default();
        for k in &keys {
            idx.insert(k, k.id());
        }

        let selectors = [
            Selector::new("cpu"),
            Selector::new("cpu").eq("dc", "fra"),
            Selector::new("cpu").eq("dc", "fra").ne("host", "h2"),
            Selector::new("cpu").ne("role", "db"),
            Selector::new("mem").eq("role", "db").eq("host", "h0"),
            Selector::new("cpu").eq("nope", "x"),
            Selector::new("missing"),
        ];
        for sel in &selectors {
            let mut want: Vec<_> = keys
                .iter()
                .filter(|k| sel.matches(k))
                .map(|k| k.id())
                .collect();
            want.sort();
            want.dedup();
            assert_eq!(idx.resolve(sel), want, "{sel:?}");
        }
    }
}
