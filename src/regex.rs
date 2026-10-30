//! Small regex engine for label matchers. Whole-string match (like PromQL), compiled to an NFA
//! and run as a set simulation, so matching is O(pattern * input) and a hostile pattern can't
//! blow up.
//!
//! Supported: literals, `.`, `[a-z_]` / `[^..]`, `\d \w \s` (and `\D \W \S` outside brackets),
//! `\n \t \r`, escaped metacharacters, `( )`, `|`, `* + ?`, `{n} {n,} {n,m}`. `^` and `$` are
//! accepted and ignored (matching is anchored anyway). No backreferences, no lookaround, no
//! captures, no lazy/possessive meaning: all quantifiers match the same language either way
//! when only a yes/no answer is wanted.

use crate::error::{Error, Result};

const MAX_PROG: usize = 4096;
const MAX_REPEAT: u32 = 1000;

#[derive(Clone, Debug, PartialEq)]
enum Node {
    Empty,
    Char(char),
    Any,
    Class(Vec<(char, char)>, bool),
    Cat(Vec<Node>),
    Alt(Vec<Node>),
    Star(Box<Node>),
    Plus(Box<Node>),
    Quest(Box<Node>),
    Repeat(Box<Node>, u32, Option<u32>),
}

#[derive(Clone, Debug)]
enum Inst {
    Char(char),
    Any,
    Class(Vec<(char, char)>, bool),
    Split(usize, usize),
    Jmp(usize),
    Match,
}

#[derive(Clone, Debug)]
pub struct Regex {
    prog: Vec<Inst>,
}

fn err<T>(m: impl Into<String>) -> Result<T> {
    Err(Error::Query(format!("regex: {}", m.into())))
}

const DIGIT: &[(char, char)] = &[('0', '9')];
const WORD: &[(char, char)] = &[('a', 'z'), ('A', 'Z'), ('0', '9'), ('_', '_')];
const SPACE: &[(char, char)] = &[(' ', ' '), ('\t', '\r')];

struct P {
    c: Vec<char>,
    i: usize,
}

impl P {
    fn peek(&self) -> Option<char> {
        self.c.get(self.i).copied()
    }

    fn eat(&mut self, ch: char) -> bool {
        if self.peek() == Some(ch) {
            self.i += 1;
            true
        } else {
            false
        }
    }

    fn alt(&mut self) -> Result<Node> {
        let mut arms = vec![self.cat()?];
        while self.eat('|') {
            arms.push(self.cat()?);
        }
        Ok(if arms.len() == 1 {
            arms.pop().unwrap()
        } else {
            Node::Alt(arms)
        })
    }

    fn cat(&mut self) -> Result<Node> {
        let mut items = Vec::new();
        while let Some(c) = self.peek() {
            if c == '|' || c == ')' {
                break;
            }
            items.push(self.repeat()?);
        }
        Ok(match items.len() {
            0 => Node::Empty,
            1 => items.pop().unwrap(),
            _ => Node::Cat(items),
        })
    }

    fn number(&mut self) -> Option<u32> {
        let st = self.i;
        while self.peek().is_some_and(|c| c.is_ascii_digit()) {
            self.i += 1;
        }
        if st == self.i {
            return None;
        }
        let s: String = self.c[st..self.i].iter().collect();
        s.parse().ok()
    }

    // `{` only counts as a quantifier when it parses as one, otherwise it is a literal
    fn braces(&mut self) -> Result<Option<(u32, Option<u32>)>> {
        let save = self.i;
        self.i += 1;
        let Some(lo) = self.number() else {
            self.i = save;
            return Ok(None);
        };
        let hi = if self.eat(',') {
            if self.peek() == Some('}') {
                None
            } else {
                match self.number() {
                    Some(h) => Some(h),
                    None => {
                        self.i = save;
                        return Ok(None);
                    }
                }
            }
        } else {
            Some(lo)
        };
        if !self.eat('}') {
            self.i = save;
            return Ok(None);
        }
        if hi.is_some_and(|h| h < lo) {
            return err("repeat max is below min");
        }
        if lo > MAX_REPEAT || hi.is_some_and(|h| h > MAX_REPEAT) {
            return err("repeat count too large");
        }
        Ok(Some((lo, hi)))
    }

    fn repeat(&mut self) -> Result<Node> {
        let mut n = self.atom()?;
        loop {
            n = match self.peek() {
                Some('*') => Node::Star(Box::new(n)),
                Some('+') => Node::Plus(Box::new(n)),
                Some('?') => Node::Quest(Box::new(n)),
                Some('{') => match self.braces()? {
                    Some((lo, hi)) => {
                        // braces() already moved past the closing brace
                        n = Node::Repeat(Box::new(n), lo, hi);
                        continue;
                    }
                    None => return Ok(n),
                },
                _ => return Ok(n),
            };
            self.i += 1;
        }
    }

    fn escape_class(c: char) -> Option<(&'static [(char, char)], bool)> {
        Some(match c {
            'd' => (DIGIT, false),
            'D' => (DIGIT, true),
            'w' => (WORD, false),
            'W' => (WORD, true),
            's' => (SPACE, false),
            'S' => (SPACE, true),
            _ => return None,
        })
    }

    fn escape_char(c: char) -> char {
        match c {
            'n' => '\n',
            't' => '\t',
            'r' => '\r',
            o => o,
        }
    }

    fn atom(&mut self) -> Result<Node> {
        let Some(c) = self.peek() else {
            return err("unexpected end of pattern");
        };
        self.i += 1;
        match c {
            '(' => {
                // `(?:` is accepted, groups never capture anyway
                if self.peek() == Some('?') && self.c.get(self.i + 1) == Some(&':') {
                    self.i += 2;
                }
                let n = self.alt()?;
                if !self.eat(')') {
                    return err("missing )");
                }
                Ok(n)
            }
            '[' => self.class(),
            '.' => Ok(Node::Any),
            '^' | '$' => Ok(Node::Empty),
            '*' | '+' | '?' => err("nothing to repeat"),
            '\\' => {
                let Some(e) = self.peek() else {
                    return err("trailing backslash");
                };
                self.i += 1;
                Ok(match Self::escape_class(e) {
                    Some((r, neg)) => Node::Class(r.to_vec(), neg),
                    None => Node::Char(Self::escape_char(e)),
                })
            }
            c => Ok(Node::Char(c)),
        }
    }

    fn class(&mut self) -> Result<Node> {
        let neg = self.eat('^');
        let mut ranges: Vec<(char, char)> = Vec::new();
        let mut first = true;
        loop {
            let Some(c) = self.peek() else {
                return err("missing ]");
            };
            self.i += 1;
            if c == ']' && !first {
                break;
            }
            first = false;
            let lo = if c == '\\' {
                let Some(e) = self.peek() else {
                    return err("trailing backslash");
                };
                self.i += 1;
                if let Some((r, n)) = Self::escape_class(e) {
                    if n {
                        return err("negated shorthand inside [] is not supported");
                    }
                    ranges.extend_from_slice(r);
                    continue;
                }
                Self::escape_char(e)
            } else {
                c
            };
            // a-z, but a trailing `-` is a literal
            if self.peek() == Some('-') && self.c.get(self.i + 1).is_some_and(|&n| n != ']') {
                self.i += 1;
                let mut hi = self.peek().unwrap();
                self.i += 1;
                if hi == '\\' {
                    let Some(e) = self.peek() else {
                        return err("trailing backslash");
                    };
                    self.i += 1;
                    hi = Self::escape_char(e);
                }
                if hi < lo {
                    return err("bad range in []");
                }
                ranges.push((lo, hi));
            } else {
                ranges.push((lo, lo));
            }
        }
        Ok(Node::Class(ranges, neg))
    }
}

fn emit(n: &Node, p: &mut Vec<Inst>) -> Result<()> {
    if p.len() > MAX_PROG {
        return err("pattern too large");
    }
    match n {
        Node::Empty => {}
        Node::Char(c) => p.push(Inst::Char(*c)),
        Node::Any => p.push(Inst::Any),
        Node::Class(r, neg) => p.push(Inst::Class(r.clone(), *neg)),
        Node::Cat(v) => {
            for x in v {
                emit(x, p)?;
            }
        }
        Node::Alt(arms) => {
            let mut jumps = Vec::new();
            for (k, a) in arms.iter().enumerate() {
                if k + 1 < arms.len() {
                    let split = p.len();
                    p.push(Inst::Split(split + 1, 0));
                    emit(a, p)?;
                    jumps.push(p.len());
                    p.push(Inst::Jmp(0));
                    let next = p.len();
                    p[split] = Inst::Split(split + 1, next);
                } else {
                    emit(a, p)?;
                }
            }
            let end = p.len();
            for j in jumps {
                p[j] = Inst::Jmp(end);
            }
        }
        Node::Star(x) => {
            let split = p.len();
            p.push(Inst::Split(split + 1, 0));
            emit(x, p)?;
            p.push(Inst::Jmp(split));
            let end = p.len();
            p[split] = Inst::Split(split + 1, end);
        }
        Node::Plus(x) => {
            let start = p.len();
            emit(x, p)?;
            let at = p.len();
            p.push(Inst::Split(start, at + 1));
        }
        Node::Quest(x) => {
            let split = p.len();
            p.push(Inst::Split(split + 1, 0));
            emit(x, p)?;
            let end = p.len();
            p[split] = Inst::Split(split + 1, end);
        }
        Node::Repeat(x, lo, hi) => {
            for _ in 0..*lo {
                emit(x, p)?;
            }
            match hi {
                None => emit(&Node::Star(x.clone()), p)?,
                Some(h) => {
                    for _ in *lo..*h {
                        emit(&Node::Quest(x.clone()), p)?;
                    }
                }
            }
        }
    }
    Ok(())
}

impl Regex {
    pub fn new(pat: &str) -> Result<Regex> {
        let mut p = P {
            c: pat.chars().collect(),
            i: 0,
        };
        let ast = p.alt()?;
        if p.i < p.c.len() {
            return err("unmatched )");
        }
        let mut prog = Vec::new();
        emit(&ast, &mut prog)?;
        prog.push(Inst::Match);
        if prog.len() > MAX_PROG {
            return err("pattern too large");
        }
        Ok(Regex { prog })
    }

    // follow jumps / splits, collecting the instructions that consume a char (or match)
    fn add(&self, pc: usize, list: &mut Vec<usize>, seen: &mut [u32], gen: u32) {
        let mut stack = vec![pc];
        while let Some(pc) = stack.pop() {
            if seen[pc] == gen {
                continue;
            }
            seen[pc] = gen;
            match &self.prog[pc] {
                Inst::Jmp(t) => stack.push(*t),
                Inst::Split(a, b) => {
                    stack.push(*b);
                    stack.push(*a);
                }
                _ => list.push(pc),
            }
        }
    }

    // the whole string must match
    pub fn is_match(&self, s: &str) -> bool {
        let mut seen = vec![0u32; self.prog.len()];
        let mut gen = 1;
        let mut cur = Vec::new();
        self.add(0, &mut cur, &mut seen, gen);
        let mut next = Vec::new();
        for ch in s.chars() {
            gen += 1;
            next.clear();
            for &pc in &cur {
                let ok = match &self.prog[pc] {
                    Inst::Char(c) => *c == ch,
                    Inst::Any => true,
                    Inst::Class(r, neg) => r.iter().any(|&(a, b)| a <= ch && ch <= b) != *neg,
                    _ => false,
                };
                if ok {
                    self.add(pc + 1, &mut next, &mut seen, gen);
                }
            }
            std::mem::swap(&mut cur, &mut next);
            if cur.is_empty() {
                return false;
            }
        }
        cur.iter().any(|&pc| matches!(self.prog[pc], Inst::Match))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn m(p: &str, s: &str) -> bool {
        Regex::new(p).unwrap().is_match(s)
    }

    #[test]
    fn basics() {
        for (p, s, want) in [
            ("web-1", "web-1", true),
            ("web", "web-1", false), // anchored: no substring matching
            ("web-.", "web-1", true),
            ("web-.", "web-", false),
            ("a|b|c", "b", true),
            ("a|b|c", "d", false),
            ("(ab)+", "ababab", true),
            ("(ab)+", "aba", false),
            ("a*", "", true),
            ("a+", "", false),
            ("colou?r", "color", true),
            ("colou?r", "colour", true),
            ("[a-c]+", "abcabc", true),
            ("[^a-c]+", "xyz", true),
            ("[^a-c]+", "xbz", false),
            ("[a-]+", "a-a", true),
            ("[]a]+", "]a", true),
            ("\\d+", "12345", true),
            ("\\d+", "12a45", false),
            ("\\w+-\\d", "web_1-9", true),
            ("\\s", " ", true),
            ("\\D\\S", "ab", true),
            ("a.c", "a.c", true),
            ("a\\.c", "abc", false),
            ("a\\.c", "a.c", true),
            ("a{3}", "aaa", true),
            ("a{3}", "aa", false),
            ("a{2,}", "aaaa", true),
            ("a{2,3}", "aaaa", false),
            ("a{,3}", "a{,3}", true), // not a quantifier -> literal
            ("^abc$", "abc", true),
            ("(?:ab|cd)e", "cde", true),
            ("", "", true),
            ("", "a", false),
            ("é.", "éa", true),
            ("(a|)b", "b", true),
            ("(a*)*b", "aab", true),
            ("x*y*z*", "xxzz", true),
        ] {
            assert_eq!(m(p, s), want, "{p:?} vs {s:?}");
        }
    }

    #[test]
    fn bad_patterns() {
        for p in [
            "(", ")", "a)", "(a", "[a", "[z-a]", "*a", "+", "a**b?", "\\", "a{3,1}", "a{2000}",
            "[\\D]",
        ] {
            // "a**b?" is fine (stacked quantifiers), everything else must fail
            let r = Regex::new(p);
            if p == "a**b?" {
                assert!(r.is_ok());
            } else {
                assert!(r.is_err(), "{p:?} should not compile");
            }
        }
        assert!(Regex::new("((((a{100}){100}){100}){100})").is_err());
    }

    #[test]
    fn pathological_pattern_stays_linear() {
        // exponential for a backtracking engine
        let re = Regex::new("(a*)*b").unwrap();
        let s = "a".repeat(20_000);
        assert!(!re.is_match(&s));
        let re = Regex::new("(a|aa)+$").unwrap();
        assert!(re.is_match(&s));
        let re = Regex::new("(x+x+)+y").unwrap();
        assert!(!re.is_match(&"x".repeat(5000)));
    }

    // reference: the set of positions a node can reach, straight from the AST
    fn ends(n: &Node, s: &[char], from: &[usize]) -> Vec<usize> {
        let mut out: Vec<usize> = match n {
            Node::Empty => from.to_vec(),
            Node::Char(c) => from
                .iter()
                .filter(|&&i| s.get(i) == Some(c))
                .map(|i| i + 1)
                .collect(),
            Node::Any => from
                .iter()
                .filter(|&&i| i < s.len())
                .map(|i| i + 1)
                .collect(),
            Node::Class(r, neg) => from
                .iter()
                .filter(|&&i| {
                    s.get(i)
                        .is_some_and(|ch| r.iter().any(|&(a, b)| a <= *ch && *ch <= b) != *neg)
                })
                .map(|i| i + 1)
                .collect(),
            Node::Cat(v) => v.iter().fold(from.to_vec(), |cur, x| ends(x, s, &cur)),
            Node::Alt(v) => v.iter().flat_map(|x| ends(x, s, from)).collect(),
            Node::Quest(x) => from.iter().copied().chain(ends(x, s, from)).collect(),
            Node::Star(x) | Node::Plus(x) => {
                let mut all: Vec<usize> = if matches!(n, Node::Star(_)) {
                    from.to_vec()
                } else {
                    Vec::new()
                };
                let mut frontier = ends(x, s, from);
                while !frontier.is_empty() {
                    frontier.sort_unstable();
                    frontier.dedup();
                    let fresh: Vec<usize> = frontier
                        .iter()
                        .copied()
                        .filter(|i| !all.contains(i))
                        .collect();
                    all.extend(&fresh);
                    frontier = ends(x, s, &fresh);
                }
                all
            }
            Node::Repeat(x, lo, hi) => {
                let mut cur = from.to_vec();
                for _ in 0..*lo {
                    cur = ends(x, s, &cur);
                }
                let mut all = cur.clone();
                match hi {
                    Some(h) => {
                        for _ in *lo..*h {
                            cur = ends(x, s, &cur);
                            all.extend(&cur);
                        }
                    }
                    None => return ends(&Node::Star(x.clone()), s, &cur),
                }
                all
            }
        };
        out.sort_unstable();
        out.dedup();
        out
    }

    fn gen_pat(r: &mut crate::testutil::Rng, depth: u32) -> String {
        let atom = |r: &mut crate::testutil::Rng, depth: u32| -> String {
            match r.below(if depth == 0 { 4 } else { 6 }) {
                0 => "a".into(),
                1 => "b".into(),
                2 => ".".into(),
                3 => "[ab]".into(),
                4 => format!("({})", gen_pat(r, depth - 1)),
                _ => format!("({}|{})", gen_pat(r, depth - 1), gen_pat(r, depth - 1)),
            }
        };
        let mut out = String::new();
        for _ in 0..1 + r.below(3) {
            out.push_str(&atom(r, depth));
            match r.below(8) {
                0 => out.push('*'),
                1 => out.push('+'),
                2 => out.push('?'),
                3 => out.push_str("{1,2}"),
                _ => {}
            }
        }
        out
    }

    #[test]
    fn agrees_with_ast_reference() {
        let mut r = crate::testutil::Rng::new(77);
        let mut matched = 0;
        for _ in 0..1500 {
            let pat = gen_pat(&mut r, 2);
            let mut p = P {
                c: pat.chars().collect(),
                i: 0,
            };
            let ast = p.alt().unwrap();
            let re = Regex::new(&pat).unwrap();
            for _ in 0..12 {
                let s: Vec<char> = (0..r.below(7))
                    .map(|_| if r.below(2) == 0 { 'a' } else { 'b' })
                    .collect();
                let want = ends(&ast, &s, &[0]).contains(&s.len());
                let text: String = s.iter().collect();
                assert_eq!(re.is_match(&text), want, "{pat:?} vs {text:?}");
                matched += want as u32;
            }
        }
        // make sure the comparison wasn't trivially all-false
        assert!(matched > 1000, "{matched}");
    }
}
