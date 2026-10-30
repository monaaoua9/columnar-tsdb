//! Minimal HTTP/1.1 front end on std::net. One thread per connection, `Connection: close`,
//! no TLS, no keep-alive: put a real proxy in front of it if it leaves localhost.
//!
//!   GET  /health
//!   GET  /metrics                               prometheus text
//!   POST /write[?ts=<default ts>]               body = line protocol
//!   GET  /query?query=..&start=..&end=..&step=..
//!
//! `start` / `end` take an integer, `now`, or `now-5m`; `step` takes an integer or a duration.
//! Answers are `{"data":..}` or `{"error":{"code":..,"message":..}}`.

use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::engine::Engine;
use crate::error::{Error, Result};
use crate::json;
use crate::lineproto;
use crate::query::{self, parse_duration, Ctx};

const MAX_BODY: usize = 8 << 20;
const MAX_HEAD_LINE: u64 = 8192;
const MAX_CONNS: usize = 64;

#[derive(Clone, Debug, Default)]
pub struct ServerConfig {
    // when set, everything except /health needs `Authorization: Bearer <token>`.
    // This is a shared secret over plain http, not a substitute for TLS
    pub token: Option<String>,
}

pub struct Server {
    addr: SocketAddr,
    stop: Arc<AtomicBool>,
    accept: Option<JoinHandle<()>>,
}

impl Server {
    pub fn start(engine: Engine, addr: impl ToSocketAddrs) -> Result<Server> {
        Server::start_with(engine, addr, ServerConfig::default())
    }

    pub fn start_with(
        engine: Engine,
        addr: impl ToSocketAddrs,
        cfg: ServerConfig,
    ) -> Result<Server> {
        let token = Arc::new(cfg.token);
        let listener = TcpListener::bind(addr)?;
        let addr = listener.local_addr()?;
        let stop = Arc::new(AtomicBool::new(false));
        let live = Arc::new(AtomicUsize::new(0));

        let accept = {
            let stop = stop.clone();
            thread::Builder::new()
                .name("tsdb-http".into())
                .spawn(move || {
                    for conn in listener.incoming() {
                        if stop.load(Ordering::Acquire) {
                            return;
                        }
                        let Ok(mut s) = conn else { continue };
                        if live.fetch_add(1, Ordering::AcqRel) >= MAX_CONNS {
                            live.fetch_sub(1, Ordering::AcqRel);
                            let _ = respond(&mut s, 503, "text/plain", b"busy\n");
                            continue;
                        }
                        let (e, live, token) = (engine.clone(), live.clone(), token.clone());
                        thread::spawn(move || {
                            let _ = s.set_read_timeout(Some(Duration::from_secs(5)));
                            let _ = s.set_write_timeout(Some(Duration::from_secs(5)));
                            let _ = serve(&e, s, token.as_deref());
                            live.fetch_sub(1, Ordering::AcqRel);
                        });
                    }
                })?
        };
        Ok(Server {
            addr,
            stop,
            accept: Some(accept),
        })
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub fn shutdown(mut self) {
        self.close();
    }

    fn close(&mut self) {
        self.stop.store(true, Ordering::Release);
        // accept() only returns on a connection, so make one
        let _ = TcpStream::connect(self.addr);
        if let Some(h) = self.accept.take() {
            let _ = h.join();
        }
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.close();
    }
}

struct Req {
    method: String,
    path: String,
    query: Vec<(String, String)>,
    auth: Option<String>,
    body: Vec<u8>,
}

impl Req {
    fn param(&self, k: &str) -> Option<&str> {
        self.query
            .iter()
            .find(|(n, _)| n == k)
            .map(|(_, v)| v.as_str())
    }
}

pub(crate) fn url_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'+' => out.push(b' '),
            b'%' if b
                .get(i + 1..i + 3)
                .is_some_and(|h| h.iter().all(u8::is_ascii_hexdigit)) =>
            {
                let hex = std::str::from_utf8(&b[i + 1..i + 3]).unwrap();
                out.push(u8::from_str_radix(hex, 16).unwrap());
                i += 2;
            }
            c => out.push(c),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn parse_query_string(q: &str) -> Vec<(String, String)> {
    q.split('&')
        .filter(|p| !p.is_empty())
        .map(|p| match p.split_once('=') {
            Some((k, v)) => (url_decode(k), url_decode(v)),
            None => (url_decode(p), String::new()),
        })
        .collect()
}

fn read_line(r: &mut impl BufRead) -> io::Result<String> {
    let mut line = String::new();
    let n = r.by_ref().take(MAX_HEAD_LINE).read_line(&mut line)?;
    if n as u64 >= MAX_HEAD_LINE && !line.ends_with('\n') {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "header line too long",
        ));
    }
    Ok(line.trim_end().to_string())
}

fn read_request(stream: &TcpStream) -> io::Result<Req> {
    let mut r = BufReader::new(stream);
    let first = read_line(&mut r)?;
    let mut it = first.split(' ');
    let (Some(method), Some(target), Some(_ver)) = (it.next(), it.next(), it.next()) else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "bad request line",
        ));
    };
    let (path, qs) = target.split_once('?').unwrap_or((target, ""));

    let mut len = 0usize;
    let mut auth = None;
    loop {
        let h = read_line(&mut r)?;
        if h.is_empty() {
            break;
        }
        if let Some((k, v)) = h.split_once(':') {
            if k.eq_ignore_ascii_case("authorization") {
                auth = Some(v.trim().to_string());
            }
            if k.eq_ignore_ascii_case("content-length") {
                len = v.trim().parse().map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "bad content-length")
                })?;
            }
        }
    }
    if len > MAX_BODY {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "body too large"));
    }
    let mut body = vec![0u8; len];
    r.read_exact(&mut body)?;
    Ok(Req {
        method: method.to_string(),
        path: url_decode(path),
        query: parse_query_string(qs),
        auth,
        body,
    })
}

fn status_text(code: u16) -> &'static str {
    match code {
        200 => "OK",
        204 => "No Content",
        401 => "Unauthorized",
        400 => "Bad Request",
        404 => "Not Found",
        405 => "Method Not Allowed",
        413 => "Payload Too Large",
        500 => "Internal Server Error",
        503 => "Service Unavailable",
        _ => "OK",
    }
}

fn respond(s: &mut TcpStream, code: u16, ctype: &str, body: &[u8]) -> io::Result<()> {
    let challenge = if code == 401 {
        "WWW-Authenticate: Bearer\r\n"
    } else {
        ""
    };
    let head = format!(
        "HTTP/1.1 {code} {}\r\nContent-Type: {ctype}\r\nContent-Length: {}\r\n{challenge}Connection: close\r\n\r\n",
        status_text(code),
        body.len()
    );
    s.write_all(head.as_bytes())?;
    s.write_all(body)?;
    s.flush()
}

fn error_body(code: &str, msg: &str) -> Vec<u8> {
    let mut o = String::from("{\"error\":{\"code\":");
    json::escape(code, &mut o);
    o.push_str(",\"message\":");
    json::escape(msg, &mut o);
    o.push_str("}}");
    o.into_bytes()
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as i64)
}

// 123 | now | now-5m
pub(crate) fn parse_time(s: &str, now: i64) -> Result<i64> {
    if let Some(rest) = s.strip_prefix("now") {
        if rest.is_empty() {
            return Ok(now);
        }
        if let Some(d) = rest.strip_prefix('-') {
            return Ok(now.saturating_sub(parse_duration(d)?));
        }
        return Err(Error::Query(format!("bad time {s:?}")));
    }
    s.parse()
        .map_err(|_| Error::Query(format!("bad time {s:?}")))
}

// compare without bailing at the first differing byte
fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    let mut diff = a.len() != b.len();
    for i in 0..a.len().max(b.len()) {
        diff |= a.get(i).copied().unwrap_or(0) != b.get(i).copied().unwrap_or(0);
    }
    !diff
}

fn serve(engine: &Engine, mut s: TcpStream, token: Option<&str>) -> io::Result<()> {
    let req = match read_request(&s) {
        Ok(r) => r,
        Err(e) => {
            let code = if e.to_string().contains("too large") {
                413
            } else {
                400
            };
            return respond(
                &mut s,
                code,
                "application/json",
                &error_body("BAD_REQUEST", &e.to_string()),
            );
        }
    };
    if let Some(t) = token {
        let ok = req
            .auth
            .as_deref()
            .and_then(|a| a.strip_prefix("Bearer "))
            .is_some_and(|given| ct_eq(given.trim().as_bytes(), t.as_bytes()));
        if !ok && req.path != "/health" {
            return respond(
                &mut s,
                401,
                "application/json",
                &error_body("UNAUTHORIZED", "missing or wrong bearer token"),
            );
        }
    }
    let (code, ctype, body) = route(engine, &req);
    respond(&mut s, code, ctype, &body)
}

fn route(engine: &Engine, req: &Req) -> (u16, &'static str, Vec<u8>) {
    const JSON: &str = "application/json";
    let fail = |code: u16, c: &str, m: &str| (code, JSON, error_body(c, m));
    match (req.method.as_str(), req.path.as_str()) {
        ("GET", "/health") => (200, "text/plain", b"ok\n".to_vec()),
        ("GET", "/metrics") => (
            200,
            "text/plain; version=0.0.4",
            engine.render_metrics().into_bytes(),
        ),
        ("POST", "/write") => {
            let now = now_ms();
            let default_ts = match req.param("ts").map(|t| parse_time(t, now)) {
                None => now,
                Some(Ok(t)) => t,
                Some(Err(e)) => return fail(400, "BAD_TIME", &e.to_string()),
            };
            let Ok(text) = std::str::from_utf8(&req.body) else {
                return fail(400, "BAD_ENCODING", "body is not utf-8");
            };
            match lineproto::ingest(engine, text, default_ts) {
                Ok(_) => (204, JSON, Vec::new()),
                Err(Error::TooOld { .. }) => fail(400, "TOO_OLD", "point older than retention"),
                Err(e @ (Error::Corrupt(_) | Error::BadValue(_))) => {
                    fail(400, "BAD_LINE", &e.to_string())
                }
                Err(e) => fail(500, "INTERNAL", &e.to_string()),
            }
        }
        ("GET", "/query") => {
            let now = now_ms();
            let Some(q) = req.param("query") else {
                return fail(400, "MISSING_QUERY", "query parameter is required");
            };
            let run = || -> Result<(Ctx, crate::query::Value)> {
                let end = req.param("end").map_or(Ok(now), |t| parse_time(t, now))?;
                let start = req
                    .param("start")
                    .map_or(Ok(end.saturating_sub(3_600_000)), |t| parse_time(t, now))?;
                let ctx = match req.param("step") {
                    None => Ctx::instant(start, end),
                    Some(st) => Ctx::stepped(start, end, parse_duration(st)?),
                };
                let v = query::run(engine, q, &ctx)?;
                Ok((ctx, v))
            };
            match run() {
                Ok((ctx, v)) => {
                    let mut o = String::from("{\"data\":");
                    json::value(&v, &mut o);
                    o.push_str(&format!(
                        ",\"meta\":{{\"start\":{},\"end\":{}",
                        ctx.start, ctx.end
                    ));
                    if let Some(s) = ctx.step {
                        o.push_str(&format!(",\"step\":{s}"));
                    }
                    o.push_str("}}");
                    (200, JSON, o.into_bytes())
                }
                Err(Error::Query(m)) => fail(400, "BAD_QUERY", &m),
                Err(e) => fail(500, "INTERNAL", &e.to_string()),
            }
        }
        (_, "/health" | "/metrics" | "/write" | "/query") => {
            fail(405, "METHOD_NOT_ALLOWED", "wrong method")
        }
        _ => fail(404, "NOT_FOUND", "no such route"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decoding() {
        assert_eq!(url_decode("a%20b+c"), "a b c");
        assert_eq!(url_decode("%7Bx%7D"), "{x}");
        assert_eq!(url_decode("100%"), "100%");
        assert_eq!(url_decode("%zz"), "%zz");
        assert_eq!(url_decode("%d8%b9"), "ع");
        assert_eq!(
            parse_query_string("a=1&b=x%3D%22y%22&flag&&c="),
            vec![
                ("a".into(), "1".into()),
                ("b".into(), "x=\"y\"".into()),
                ("flag".into(), "".into()),
                ("c".into(), "".into())
            ]
        );
    }

    #[test]
    fn constant_time_compare() {
        assert!(ct_eq(b"secret", b"secret"));
        assert!(!ct_eq(b"secret", b"secreT"));
        assert!(!ct_eq(b"secret", b"secre"));
        assert!(!ct_eq(b"", b"x"));
        assert!(ct_eq(b"", b""));
        assert!(!ct_eq(&[0u8; 256], b""));
    }

    #[test]
    fn times() {
        assert_eq!(parse_time("1234", 99).unwrap(), 1234);
        assert_eq!(parse_time("-5", 99).unwrap(), -5);
        assert_eq!(parse_time("now", 99_000).unwrap(), 99_000);
        assert_eq!(parse_time("now-1s", 99_000).unwrap(), 98_000);
        assert!(parse_time("nowish", 0).is_err());
        assert!(parse_time("now+5m", 0).is_err());
        assert!(parse_time("yesterday", 0).is_err());
    }

    #[test]
    fn error_bodies_are_json() {
        let b = String::from_utf8(error_body("X", "say \"hi\"")).unwrap();
        assert_eq!(
            b,
            "{\"error\":{\"code\":\"X\",\"message\":\"say \\\"hi\\\"\"}}"
        );
    }
}
