use crate::error::{Error, Result};
use crate::funcs::Func;
use crate::model::{Matcher, Selector};

#[derive(Clone, Debug, PartialEq)]
enum Tok {
    Ident(String),
    Num(f64),
    Str(String),
    // duration in ts units (ms)
    Dur(i64),
    LParen,
    RParen,
    LBrace,
    RBrace,
    LBrack,
    RBrack,
    Comma,
    Eq,
    Ne,
    Glob,
    Plus,
    Minus,
    Star,
    Slash,
    Gt,
    Lt,
    Ge,
    Le,
    EqEq,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AggOp {
    Sum,
    Avg,
    Min,
    Max,
    Count,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BinOp {
    Add,
    Sub,
    Mul,
    Div,
    // the comparisons filter instead of producing booleans
    Gt,
    Lt,
    Ge,
    Le,
    Eq,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Expr {
    Num(f64),
    Select(Selector, Option<i64>),
    Call(Func, Selector, Option<i64>),
    Agg(AggOp, Vec<String>, Box<Expr>),
    TopK { k: usize, largest: bool, arg: Box<Expr> },
    Bin(BinOp, Box<Expr>, Box<Expr>),
}

fn err<T>(msg: impl Into<String>) -> Result<T> {
    Err(Error::Query(msg.into()))
}

pub fn parse_duration(s: &str) -> Result<i64> {
    let toks = lex(s)?;
    match toks.as_slice() {
        [(Tok::Dur(d), _)] => Ok(*d),
        [(Tok::Num(n), _)] if n.fract() == 0.0 => Ok(*n as i64),
        _ => err(format!("bad duration {s:?}")),
    }
}

fn unit_ms(u: &str) -> Option<i64> {
    Some(match u {
        "ms" => 1,
        "s" => 1000,
        "m" => 60_000,
        "h" => 3_600_000,
        "d" => 86_400_000,
        "w" => 604_800_000,
        _ => return None,
    })
}

fn lex(src: &str) -> Result<Vec<(Tok, usize)>> {
    let b: Vec<char> = src.chars().collect();
    let mut out = Vec::new();
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        let at = i;
        if c.is_whitespace() {
            i += 1;
            continue;
        }
        let two = |n: char| b.get(i + 1) == Some(&n);
        let tok = match c {
            '(' => Tok::LParen,
            ')' => Tok::RParen,
            '{' => Tok::LBrace,
            '}' => Tok::RBrace,
            '[' => Tok::LBrack,
            ']' => Tok::RBrack,
            ',' => Tok::Comma,
            '+' => Tok::Plus,
            '-' => Tok::Minus,
            '*' => Tok::Star,
            '/' => Tok::Slash,
            '=' if two('~') => {
                i += 1;
                Tok::Glob
            }
            '=' if two('=') => {
                i += 1;
                Tok::EqEq
            }
            '=' => Tok::Eq,
            '!' if two('=') => {
                i += 1;
                Tok::Ne
            }
            '>' if two('=') => {
                i += 1;
                Tok::Ge
            }
            '>' => Tok::Gt,
            '<' if two('=') => {
                i += 1;
                Tok::Le
            }
            '<' => Tok::Lt,
            '"' => {
                let mut s = String::new();
                i += 1;
                loop {
                    match b.get(i) {
                        None => return err(format!("unterminated string at {at}")),
                        Some('"') => break,
                        Some('\\') if i + 1 < b.len() => {
                            i += 1;
                            s.push(b[i]);
                        }
                        Some(&ch) => s.push(ch),
                    }
                    i += 1;
                }
                Tok::Str(s)
            }
            d if d.is_ascii_digit() || d == '.' => {
                let st = i;
                while i < b.len() && (b[i].is_ascii_digit() || b[i] == '.') {
                    i += 1;
                }
                // exponent only if a digit follows, so `5e` stays a unit error rather than a number
                if i < b.len() && (b[i] == 'e' || b[i] == 'E') {
                    let mut j = i + 1;
                    if j < b.len() && (b[j] == '+' || b[j] == '-') {
                        j += 1;
                    }
                    if j < b.len() && b[j].is_ascii_digit() {
                        while j < b.len() && b[j].is_ascii_digit() {
                            j += 1;
                        }
                        i = j;
                    }
                }
                let text: String = b[st..i].iter().collect();
                if i < b.len() && b[i].is_ascii_alphabetic() {
                    // duration like 90s or 1h30m
                    i = st;
                    let mut total = 0i64;
                    while i < b.len() && b[i].is_ascii_digit() {
                        let ds = i;
                        while i < b.len() && b[i].is_ascii_digit() {
                            i += 1;
                        }
                        let n: i64 = b[ds..i].iter().collect::<String>().parse().unwrap();
                        let us = i;
                        while i < b.len() && b[i].is_ascii_alphabetic() {
                            i += 1;
                        }
                        let u: String = b[us..i].iter().collect();
                        let Some(m) = unit_ms(&u) else {
                            return err(format!("unknown duration unit {u:?} at {us}"));
                        };
                        total = n
                            .checked_mul(m)
                            .and_then(|x| total.checked_add(x))
                            .ok_or_else(|| Error::Query("duration overflow".into()))?;
                    }
                    out.push((Tok::Dur(total), at));
                    continue;
                }
                match text.parse::<f64>() {
                    Ok(n) => {
                        out.push((Tok::Num(n), at));
                        continue;
                    }
                    Err(_) => return err(format!("bad number {text:?} at {at}")),
                }
            }
            a if a.is_alphabetic() || a == '_' || a == ':' => {
                let st = i;
                while i < b.len()
                    && (b[i].is_alphanumeric() || b[i] == '_' || b[i] == ':' || b[i] == '.')
                {
                    i += 1;
                }
                out.push((Tok::Ident(b[st..i].iter().collect()), at));
                continue;
            }
            other => return err(format!("unexpected {other:?} at {at}")),
        };
        i += 1;
        out.push((tok, at));
    }
    Ok(out)
}

struct Parser {
    toks: Vec<(Tok, usize)>,
    pos: usize,
    len: usize,
}

pub fn parse(src: &str) -> Result<Expr> {
    let toks = lex(src)?;
    let mut p = Parser {
        toks,
        pos: 0,
        len: src.chars().count(),
    };
    let e = p.expr()?;
    if p.pos < p.toks.len() {
        return err(format!("unexpected trailing input at {}", p.toks[p.pos].1));
    }
    Ok(e)
}

impl Parser {
    fn peek(&self) -> Option<&Tok> {
        self.toks.get(self.pos).map(|t| &t.0)
    }

    fn at(&self) -> usize {
        self.toks.get(self.pos).map_or(self.len, |t| t.1)
    }

    fn next(&mut self) -> Option<Tok> {
        let t = self.toks.get(self.pos).map(|t| t.0.clone());
        self.pos += 1;
        t
    }

    fn eat(&mut self, t: &Tok) -> bool {
        if self.peek() == Some(t) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    fn expect(&mut self, t: Tok) -> Result<()> {
        if self.eat(&t) {
            Ok(())
        } else {
            err(format!("expected {t:?} at {}", self.at()))
        }
    }

    fn ident(&mut self) -> Result<String> {
        let at = self.at();
        match self.next() {
            Some(Tok::Ident(s)) => Ok(s),
            _ => err(format!("expected a name at {at}")),
        }
    }

    fn number(&mut self) -> Result<f64> {
        let at = self.at();
        match self.next() {
            Some(Tok::Num(n)) => Ok(n),
            _ => err(format!("expected a number at {at}")),
        }
    }

    fn expr(&mut self) -> Result<Expr> {
        let l = self.additive()?;
        let op = match self.peek() {
            Some(Tok::Gt) => BinOp::Gt,
            Some(Tok::Lt) => BinOp::Lt,
            Some(Tok::Ge) => BinOp::Ge,
            Some(Tok::Le) => BinOp::Le,
            Some(Tok::EqEq) => BinOp::Eq,
            _ => return Ok(l),
        };
        self.pos += 1;
        let r = self.additive()?;
        Ok(Expr::Bin(op, Box::new(l), Box::new(r)))
    }

    fn additive(&mut self) -> Result<Expr> {
        let mut l = self.term()?;
        loop {
            let op = match self.peek() {
                Some(Tok::Plus) => BinOp::Add,
                Some(Tok::Minus) => BinOp::Sub,
                _ => return Ok(l),
            };
            self.pos += 1;
            let r = self.term()?;
            l = Expr::Bin(op, Box::new(l), Box::new(r));
        }
    }

    fn term(&mut self) -> Result<Expr> {
        let mut l = self.unary()?;
        loop {
            let op = match self.peek() {
                Some(Tok::Star) => BinOp::Mul,
                Some(Tok::Slash) => BinOp::Div,
                _ => return Ok(l),
            };
            self.pos += 1;
            let r = self.unary()?;
            l = Expr::Bin(op, Box::new(l), Box::new(r));
        }
    }

    fn unary(&mut self) -> Result<Expr> {
        if self.eat(&Tok::Minus) {
            let e = self.unary()?;
            return Ok(Expr::Bin(BinOp::Sub, Box::new(Expr::Num(0.0)), Box::new(e)));
        }
        self.primary()
    }

    fn primary(&mut self) -> Result<Expr> {
        let at = self.at();
        match self.peek().cloned() {
            Some(Tok::Num(n)) => {
                self.pos += 1;
                Ok(Expr::Num(n))
            }
            Some(Tok::LParen) => {
                self.pos += 1;
                let e = self.expr()?;
                self.expect(Tok::RParen)?;
                Ok(e)
            }
            Some(Tok::Ident(name)) => {
                self.pos += 1;
                self.named(name, at)
            }
            _ => err(format!("expected an expression at {at}")),
        }
    }

    fn named(&mut self, name: String, at: usize) -> Result<Expr> {
        let followed_by_paren = self.peek() == Some(&Tok::LParen);
        let agg = match name.as_str() {
            "sum" => Some(AggOp::Sum),
            "avg" => Some(AggOp::Avg),
            "min" => Some(AggOp::Min),
            "max" => Some(AggOp::Max),
            "count" => Some(AggOp::Count),
            _ => None,
        };
        // `sum by (x) (expr)` or `sum(expr) by (x)`
        if let Some(op) = agg {
            let by_first = matches!(self.peek(), Some(Tok::Ident(s)) if s == "by");
            if followed_by_paren || by_first {
                let mut by = Vec::new();
                if by_first {
                    self.pos += 1;
                    by = self.label_list()?;
                }
                self.expect(Tok::LParen)?;
                let arg = self.expr()?;
                self.expect(Tok::RParen)?;
                if matches!(self.peek(), Some(Tok::Ident(s)) if s == "by") {
                    if !by.is_empty() {
                        return err(format!("two `by` clauses at {}", self.at()));
                    }
                    self.pos += 1;
                    by = self.label_list()?;
                }
                return Ok(Expr::Agg(op, by, Box::new(arg)));
            }
        }

        if followed_by_paren && (name == "topk" || name == "bottomk") {
            self.pos += 1;
            let kn = self.number()?;
            if kn < 1.0 || kn.fract() != 0.0 {
                return err(format!("k must be a positive integer at {at}"));
            }
            self.expect(Tok::Comma)?;
            let arg = self.expr()?;
            self.expect(Tok::RParen)?;
            return Ok(Expr::TopK {
                k: kn as usize,
                largest: name == "topk",
                arg: Box::new(arg),
            });
        }

        if followed_by_paren {
            let base = name.strip_suffix("_over_time").unwrap_or(&name);
            // rate/increase/delta have no suffix; the others must carry it
            let plain = matches!(name.as_str(), "rate" | "increase" | "delta");
            if !plain && base == name {
                return err(format!("unknown function {name:?} at {at}"));
            }
            self.pos += 1;
            let param = if base == "quantile" {
                let q = self.number()?;
                self.expect(Tok::Comma)?;
                Some(q)
            } else {
                None
            };
            let Some(func) = Func::parse(base, param) else {
                return err(format!("unknown function {name:?} at {at}"));
            };
            let (sel, range) = self.selector()?;
            self.expect(Tok::RParen)?;
            return Ok(Expr::Call(func, sel, range));
        }

        self.pos -= 1; // un-eat the name, selector() reads it
        let (sel, range) = self.selector()?;
        Ok(Expr::Select(sel, range))
    }

    fn label_list(&mut self) -> Result<Vec<String>> {
        self.expect(Tok::LParen)?;
        let mut v = Vec::new();
        while !self.eat(&Tok::RParen) {
            v.push(self.ident()?);
            if !self.eat(&Tok::Comma) {
                self.expect(Tok::RParen)?;
                break;
            }
        }
        Ok(v)
    }

    fn selector(&mut self) -> Result<(Selector, Option<i64>)> {
        let mut sel = Selector::new(self.ident()?);
        if self.eat(&Tok::LBrace) {
            while !self.eat(&Tok::RBrace) {
                let k = self.ident()?;
                let at = self.at();
                let op = self.next();
                let vat = self.at();
                let v = match self.next() {
                    Some(Tok::Str(s)) => s,
                    _ => return err(format!("label value must be a quoted string at {vat}")),
                };
                sel.matchers.push(match op {
                    Some(Tok::Eq) => Matcher::Eq(k, v),
                    Some(Tok::Ne) => Matcher::Ne(k, v),
                    Some(Tok::Glob) => Matcher::Like(k, v),
                    _ => return err(format!("expected =, != or =~ at {at}")),
                });
                if !self.eat(&Tok::Comma) {
                    self.expect(Tok::RBrace)?;
                    break;
                }
            }
        }
        let mut range = None;
        if self.eat(&Tok::LBrack) {
            let at = self.at();
            match self.next() {
                Some(Tok::Dur(d)) if d > 0 => range = Some(d),
                _ => return err(format!("expected a duration like 5m at {at}")),
            }
            self.expect(Tok::RBrack)?;
        }
        Ok((sel, range))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sel(name: &str) -> Selector {
        Selector::new(name)
    }

    #[test]
    fn durations() {
        assert_eq!(parse_duration("90s").unwrap(), 90_000);
        assert_eq!(parse_duration("1h30m").unwrap(), 5_400_000);
        assert_eq!(parse_duration("250ms").unwrap(), 250);
        assert_eq!(parse_duration("1000").unwrap(), 1000);
        assert!(parse_duration("5x").is_err());
        assert!(parse_duration("").is_err());
        assert!(parse_duration("1.5").is_err());
    }

    #[test]
    fn selector_with_matchers_and_range() {
        let e = parse(r#"cpu{dc="fra", host!="web-9", rack=~"r-*",}[5m]"#).unwrap();
        let want = sel("cpu").eq("dc", "fra").ne("host", "web-9").like("rack", "r-*");
        assert_eq!(e, Expr::Select(want, Some(300_000)));
    }

    #[test]
    fn call_forms() {
        assert_eq!(
            parse("rate(reqs[1m])").unwrap(),
            Expr::Call(Func::Rate, sel("reqs"), Some(60_000))
        );
        assert_eq!(
            parse("quantile_over_time(0.9, lat[10s])").unwrap(),
            Expr::Call(Func::Quantile(0.9), sel("lat"), Some(10_000))
        );
        assert_eq!(
            parse("max_over_time(t)").unwrap(),
            Expr::Call(Func::Max, sel("t"), None)
        );
    }

    #[test]
    fn aggregation_both_by_positions() {
        let a = parse("sum(rate(r[1m])) by (host, dc)").unwrap();
        let b = parse("sum by (host, dc) (rate(r[1m]))").unwrap();
        assert_eq!(a, b);
        match a {
            Expr::Agg(AggOp::Sum, by, _) => assert_eq!(by, vec!["host", "dc"]),
            other => panic!("{other:?}"),
        }
        assert!(parse("sum by (a) (x) by (b)").is_err());
        // a metric that happens to be called `count` still parses as a selector
        assert_eq!(parse("count").unwrap(), Expr::Select(sel("count"), None));
    }

    #[test]
    fn topk() {
        let e = parse("topk(3, avg_over_time(cpu[5m]))").unwrap();
        assert!(matches!(e, Expr::TopK { k: 3, largest: true, .. }));
        assert!(parse("topk(0, x)").is_err());
        assert!(parse("topk(1.5, x)").is_err());
    }

    #[test]
    fn precedence() {
        // 1 + 2 * 3 > 6  ->  (1 + (2*3)) > 6
        let e = parse("1 + 2 * 3 > 6").unwrap();
        let Expr::Bin(BinOp::Gt, l, _) = e else { panic!() };
        let Expr::Bin(BinOp::Add, _, r) = *l else { panic!() };
        assert!(matches!(*r, Expr::Bin(BinOp::Mul, _, _)));
        let neg = parse("-x").unwrap();
        assert!(matches!(neg, Expr::Bin(BinOp::Sub, _, _)));
    }

    #[test]
    fn errors_point_somewhere() {
        for bad in [
            "",
            "cpu{",
            "cpu{a=1}",
            "cpu{a ~ \"x\"}",
            "cpu[5]",
            "cpu[0s]",
            "foo(cpu[1m])",
            "rate(cpu[1m]",
            "rate()",
            "sum(",
            "1 +",
            "cpu cpu",
            "\"str\"",
            "cpu{a=\"unterminated}",
            "@",
        ] {
            assert!(parse(bad).is_err(), "should fail: {bad:?}");
        }
        let m = parse("cpu{a=1}").unwrap_err().to_string();
        assert!(m.contains("at 6"), "{m}");
    }

    #[test]
    fn numbers() {
        assert_eq!(parse("1.5e3").unwrap(), Expr::Num(1500.0));
        assert_eq!(parse(".5").unwrap(), Expr::Num(0.5));
        assert!(parse("1.2.3").is_err());
    }
}
