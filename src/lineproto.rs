// influx-style line protocol, trimmed down: one f64 field per line.
//
//   cpu_usage,host=web-1,dc=fra value=41.5 1700000000000
//   cpu_usage,host=web-1 41.5
//
// the field part may be `value=41.5` (any field name is accepted and ignored, so two lines with
// different field names but the same metric, labels and timestamp are the same point) or a bare
// number. several fields on one line are an error.
// timestamp is optional; the caller supplies a default (usually "now").
// backslash escapes `\,` `\ ` `\=` in names / label values.

use std::collections::HashMap;

use crate::engine::Engine;
use crate::error::{Error, Result};
use crate::model::{Point, SeriesKey};

#[derive(Clone, Debug, PartialEq)]
pub struct Line {
    pub key: SeriesKey,
    pub ts: i64,
    pub value: f64,
}

fn bad(line: &str, why: &str) -> Error {
    Error::corrupt(format!("line protocol: {why}: {line:?}"))
}

// split on `sep` unless escaped; escapes stay in the pieces, unescape() them at the end
fn split_raw(s: &str, sep: char) -> Vec<&str> {
    let mut out = Vec::new();
    let mut start = 0;
    let mut esc = false;
    for (i, c) in s.char_indices() {
        if esc {
            esc = false;
        } else if c == '\\' {
            esc = true;
        } else if c == sep {
            out.push(&s[start..i]);
            start = i + c.len_utf8();
        }
    }
    out.push(&s[start..]);
    out
}

fn unescape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut it = s.chars();
    while let Some(c) = it.next() {
        if c == '\\' {
            out.push(it.next().unwrap_or('\\'));
        } else {
            out.push(c);
        }
    }
    out
}

// first unescaped space-separated pieces, at most `n`
fn split_fields(line: &str, n: usize) -> Vec<&str> {
    let b = line.as_bytes();
    let mut out = Vec::new();
    let (mut start, mut i) = (0, 0);
    while i < b.len() && out.len() + 1 < n {
        if b[i] == b'\\' {
            i += 2;
            continue;
        }
        if b[i] == b' ' {
            if i > start {
                out.push(&line[start..i]);
            }
            start = i + 1;
        }
        i += 1;
    }
    if start < line.len() {
        out.push(line[start..].trim());
    }
    out
}

pub fn parse_line(line: &str, default_ts: i64) -> Result<Line> {
    let parts = split_fields(line.trim(), 3);
    if parts.len() < 2 {
        return Err(bad(line, "need series and value"));
    }

    let head = split_raw(parts[0], ',');
    if head[0].is_empty() {
        return Err(bad(line, "empty metric name"));
    }
    let mut key = SeriesKey::new(unescape(head[0]));
    for kv in &head[1..] {
        let mut it = split_raw(kv, '=').into_iter();
        let (Some(k), Some(v), None) = (it.next(), it.next(), it.next()) else {
            return Err(bad(line, "label must be key=value"));
        };
        if k.is_empty() {
            return Err(bad(line, "empty label key"));
        }
        key = key.label(unescape(k), unescape(v));
    }

    // `usage=1,idle=2` is valid influx but we keep one field; taking the last one silently
    // would drop data, so refuse
    if parts[1].contains(',') {
        return Err(bad(
            line,
            "one field per line (put the field name in the metric name)",
        ));
    }
    let raw = parts[1].rsplit('=').next().unwrap_or("");
    let value: f64 = raw
        .parse()
        .map_err(|_| bad(line, "value is not a number"))?;
    if !value.is_finite() {
        return Err(Error::BadValue(value));
    }

    let ts = match parts.get(2) {
        Some(t) => t
            .parse()
            .map_err(|_| bad(line, "timestamp is not an integer"))?,
        None => default_ts,
    };
    Ok(Line { key, ts, value })
}

// blank lines and `#` comments are skipped. stops at the first bad line (nothing from
// the lines after it is returned), error message carries the 1-based line number
pub fn parse_lines(text: &str, default_ts: i64) -> Result<Vec<Line>> {
    let mut out = Vec::new();
    for (n, l) in text.lines().enumerate() {
        let t = l.trim();
        if t.is_empty() || t.starts_with('#') {
            continue;
        }
        match parse_line(t, default_ts) {
            Ok(x) => out.push(x),
            Err(Error::Corrupt(m)) => return Err(Error::corrupt(format!("line {}: {m}", n + 1))),
            Err(e) => return Err(e),
        }
    }
    Ok(out)
}

// groups by series so each series is one append_batch (one wal record). returns points written
pub fn ingest(engine: &Engine, text: &str, default_ts: i64) -> Result<usize> {
    let lines = parse_lines(text, default_ts)?;
    let n = lines.len();
    let mut by_series: HashMap<SeriesKey, Vec<Point>> = HashMap::new();
    for l in lines {
        by_series
            .entry(l.key)
            .or_default()
            .push(Point::new(l.ts, l.value));
    }
    for (k, pts) in &by_series {
        engine.append_batch(k, pts)?;
    }
    Ok(n)
}

// inverse of parse_line, used by the cli export
pub fn format_line(key: &SeriesKey, p: &Point) -> String {
    fn esc(s: &str) -> String {
        s.replace('\\', "\\\\")
            .replace(',', "\\,")
            .replace(' ', "\\ ")
            .replace('=', "\\=")
    }
    let mut s = esc(key.name());
    for (k, v) in key.labels() {
        s.push(',');
        s.push_str(&esc(k));
        s.push('=');
        s.push_str(&esc(v));
    }
    format!("{s} value={} {}", p.value, p.ts)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn full_line() {
        let l = parse_line("cpu,host=web-1,dc=fra value=41.5 1700000000000", 0).unwrap();
        assert_eq!(
            l.key,
            SeriesKey::new("cpu")
                .label("host", "web-1")
                .label("dc", "fra")
        );
        assert_eq!((l.ts, l.value), (1_700_000_000_000, 41.5));
    }

    #[test]
    fn bare_value_and_default_ts() {
        let l = parse_line("temp 21", 99).unwrap();
        assert_eq!(l.key, SeriesKey::new("temp"));
        assert_eq!((l.ts, l.value), (99, 21.0));
    }

    #[test]
    fn negative_and_exponent_values() {
        assert_eq!(parse_line("m v=-1.5e3 5", 0).unwrap().value, -1500.0);
        assert_eq!(parse_line("m -2 -7", 0).unwrap().ts, -7);
    }

    #[test]
    fn escapes_round_trip() {
        let k = SeriesKey::new("we ird,name").label("a=b", "x y,z");
        let p = Point::new(10, 2.5);
        let text = format_line(&k, &p);
        let back = parse_line(&text, 0).unwrap();
        assert_eq!(back.key, k);
        assert_eq!((back.ts, back.value), (10, 2.5));
    }

    #[test]
    fn rejects_garbage() {
        for s in [
            "",
            "justaname",
            ",a=b value=1",
            "m,novalue value=1",
            "m,=x value=1",
            "m,a=b=c value=1",
            "m value=abc",
            "m value=1 12.5",
            "m value=NaN",
            "m value=inf",
            "m usage=1,idle=2 5",
        ] {
            assert!(parse_line(s, 0).is_err(), "should fail: {s:?}");
        }
    }

    #[test]
    fn multiline_with_comments_reports_line_no() {
        let ok = parse_lines("# hi\n\nm a=1 1\nm a=2 2\n", 0).unwrap();
        assert_eq!(ok.len(), 2);
        let err = parse_lines("m a=1 1\n\nm a=oops 2\n", 0).unwrap_err();
        assert!(err.to_string().contains("line 3"), "{err}");
    }

    #[test]
    fn extra_spaces_between_fields() {
        let l = parse_line("m   v=3    42  ", 0).unwrap();
        assert_eq!((l.ts, l.value), (42, 3.0));
    }
}
