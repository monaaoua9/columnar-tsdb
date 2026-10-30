// just enough json to answer http queries; no parsing, nothing generic
use std::fmt::Write;

use crate::query::{Row, Value};

pub fn escape(s: &str, out: &mut String) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

// json has no nan / inf
pub fn num(v: f64, out: &mut String) {
    if v.is_finite() {
        let _ = write!(out, "{v}");
    } else {
        out.push_str("null");
    }
}

fn row(r: &Row, out: &mut String) {
    out.push_str("{\"labels\":{");
    for (i, (k, v)) in r.labels.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        escape(k, out);
        out.push(':');
        escape(v, out);
    }
    out.push_str("},\"points\":[");
    for (i, p) in r.points.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        let _ = write!(out, "[{},", p.ts);
        num(p.value, out);
        out.push(']');
    }
    out.push_str("]}");
}

pub fn value(v: &Value, out: &mut String) {
    match v {
        Value::Scalar(x) => {
            out.push_str("{\"type\":\"scalar\",\"value\":");
            num(*x, out);
            out.push('}');
        }
        Value::Vector(rows) => {
            out.push_str("{\"type\":\"vector\",\"result\":[");
            for (i, r) in rows.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                row(r, out);
            }
            out.push_str("]}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Point;
    use std::collections::BTreeMap;

    fn esc(s: &str) -> String {
        let mut o = String::new();
        escape(s, &mut o);
        o
    }

    #[test]
    fn escaping() {
        assert_eq!(esc("plain"), "\"plain\"");
        assert_eq!(esc("a\"b\\c\nd\te"), "\"a\\\"b\\\\c\\nd\\te\"");
        assert_eq!(esc("\u{1}"), "\"\\u0001\"");
        assert_eq!(esc("عربي"), "\"عربي\"");
    }

    #[test]
    fn numbers() {
        let mut o = String::new();
        num(1.5, &mut o);
        o.push(' ');
        num(f64::NAN, &mut o);
        o.push(' ');
        num(-0.0, &mut o);
        assert_eq!(o, "1.5 null -0");
    }

    #[test]
    fn value_shapes() {
        let mut o = String::new();
        value(&Value::Scalar(2.0), &mut o);
        assert_eq!(o, "{\"type\":\"scalar\",\"value\":2}");

        let mut labels = BTreeMap::new();
        labels.insert("host".to_string(), "a\"1".to_string());
        let r = Row { labels, points: vec![Point::new(10, 1.5), Point::new(20, f64::INFINITY)] };
        let mut o = String::new();
        value(&Value::Vector(vec![r]), &mut o);
        assert_eq!(
            o,
            "{\"type\":\"vector\",\"result\":[{\"labels\":{\"host\":\"a\\\"1\"},\"points\":[[10,1.5],[20,null]]}]}"
        );
        let mut o = String::new();
        value(&Value::Vector(vec![]), &mut o);
        assert_eq!(o, "{\"type\":\"vector\",\"result\":[]}");
    }
}
