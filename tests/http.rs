use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::time::Duration;

use columnar_tsdb::server::Server;
use columnar_tsdb::{Config, Engine};

fn setup(tag: &str) -> (Engine, Server, PathBuf) {
    let dir = std::env::temp_dir().join(format!("ctsdb-http-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let mut c = Config::new(&dir);
    c.maintenance_interval = Duration::from_millis(20);
    let e = Engine::open(c).unwrap();
    let s = Server::start(e.clone(), "127.0.0.1:0").unwrap();
    (e, s, dir)
}

// returns (status, body)
fn http(srv: &Server, raw: &str) -> (u16, String) {
    let mut c = TcpStream::connect(srv.addr()).unwrap();
    c.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    c.write_all(raw.as_bytes()).unwrap();
    let mut out = String::new();
    c.read_to_string(&mut out).unwrap();
    let status = out.split(' ').nth(1).unwrap().parse().unwrap();
    let body = out.split_once("\r\n\r\n").map_or("", |x| x.1).to_string();
    (status, body)
}

fn get(srv: &Server, path: &str) -> (u16, String) {
    http(srv, &format!("GET {path} HTTP/1.1\r\nHost: x\r\n\r\n"))
}

fn post(srv: &Server, path: &str, body: &str) -> (u16, String) {
    http(
        srv,
        &format!(
            "POST {path} HTTP/1.1\r\nHost: x\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        ),
    )
}

#[test]
fn write_then_query() {
    let (e, srv, dir) = setup("wq");
    assert_eq!(get(&srv, "/health"), (200, "ok\n".into()));

    let lines = "cpu,host=a value=1 1000\ncpu,host=a value=3 2000\ncpu,host=b value=10 1500\n";
    assert_eq!(post(&srv, "/write", lines).0, 204);

    // instant sum at ts 5000 over everything
    let (code, body) = get(&srv, "/query?query=sum(cpu)&start=0&end=5000");
    assert_eq!(code, 200, "{body}");
    assert!(body.contains("\"type\":\"vector\""), "{body}");
    assert!(body.contains("[5000,13]"), "{body}");

    // percent-encoded matcher + stepped
    let q = "avg_over_time(cpu%7Bhost%3D%22a%22%7D)";
    let (code, body) = get(&srv, &format!("/query?query={q}&start=0&end=3000&step=1s"));
    assert_eq!(code, 200, "{body}");
    assert!(
        body.contains("[1000,1]") && body.contains("[2000,3]"),
        "{body}"
    );
    assert!(body.contains("\"step\":1000"), "{body}");

    assert_eq!(
        e.query_range(
            &columnar_tsdb::SeriesKey::new("cpu").label("host", "a"),
            0,
            9999
        )
        .unwrap()
        .len(),
        2
    );
    srv.shutdown();
    drop(e);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn write_without_ts_uses_default() {
    let (e, srv, dir) = setup("dts");
    assert_eq!(post(&srv, "/write?ts=777", "m v=1\nm2 v=2 5").0, 204);
    let m = columnar_tsdb::SeriesKey::new("m");
    assert_eq!(e.query_range(&m, 777, 778).unwrap().len(), 1);
    srv.shutdown();
    drop(e);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn errors_use_the_error_envelope() {
    let (e, srv, dir) = setup("err");
    let (c, b) = post(&srv, "/write", "good v=1 1\nbad line here\n");
    assert_eq!(c, 400);
    assert!(
        b.contains("\"code\":\"BAD_LINE\"") && b.contains("line 2"),
        "{b}"
    );
    // all-or-nothing: the good line before the bad one was not written
    assert!(e.select(&columnar_tsdb::Selector::new("good")).is_empty());

    let (c, b) = get(&srv, "/query?query=rate(");
    assert_eq!(c, 400);
    assert!(b.contains("BAD_QUERY"), "{b}");
    assert_eq!(get(&srv, "/query").0, 400);
    assert_eq!(get(&srv, "/query?query=x&end=nope").0, 400);
    assert_eq!(get(&srv, "/nothing").0, 404);
    assert_eq!(post(&srv, "/health", "").0, 405);
    assert_eq!(http(&srv, "garbage\r\n\r\n").0, 400);

    srv.shutdown();
    drop(e);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn metrics_endpoint_and_oversize_body() {
    let (e, srv, dir) = setup("met");
    post(&srv, "/write", "m v=1 1\n");
    let (c, b) = get(&srv, "/metrics");
    assert_eq!(c, 200);
    assert!(b.contains("tsdb_points_appended_total 1\n"), "{b}");

    let big = format!(
        "POST /write HTTP/1.1\r\nContent-Length: {}\r\n\r\n",
        9 << 20
    );
    assert_eq!(http(&srv, &big).0, 413);
    srv.shutdown();
    drop(e);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn shutdown_releases_the_port() {
    let (e, srv, dir) = setup("down");
    let addr = srv.addr();
    srv.shutdown();
    assert!(TcpStream::connect_timeout(&addr, Duration::from_millis(300)).is_err());
    drop(e);
    let _ = std::fs::remove_dir_all(dir);
}

fn setup_auth(tag: &str, token: &str) -> (Engine, Server, PathBuf) {
    let dir = std::env::temp_dir().join(format!("ctsdb-http-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let e = Engine::open(Config::new(&dir)).unwrap();
    let cfg = columnar_tsdb::server::ServerConfig {
        token: Some(token.into()),
    };
    let s = Server::start_with(e.clone(), "127.0.0.1:0", cfg).unwrap();
    (e, s, dir)
}

fn with_auth(
    srv: &Server,
    method: &str,
    path: &str,
    auth: Option<&str>,
    body: &str,
) -> (u16, String) {
    let hdr = auth.map_or(String::new(), |a| format!("Authorization: {a}\r\n"));
    http(
        srv,
        &format!(
            "{method} {path} HTTP/1.1\r\nHost: x\r\n{hdr}Content-Length: {}\r\n\r\n{body}",
            body.len()
        ),
    )
}

#[test]
fn bearer_token_guards_everything_but_health() {
    let (e, srv, dir) = setup_auth("auth", "s3cret");

    assert_eq!(get(&srv, "/health").0, 200);
    for (m, p) in [
        ("GET", "/metrics"),
        ("GET", "/query?query=x"),
        ("POST", "/write"),
    ] {
        let (c, b) = with_auth(&srv, m, p, None, "m v=1 1\n");
        assert_eq!(c, 401, "{m} {p}");
        assert!(b.contains("UNAUTHORIZED"), "{b}");
        for bad in [
            "Bearer nope",
            "Bearer s3cre",
            "Bearer s3cretx",
            "s3cret",
            "Basic s3cret",
            "Bearer ",
        ] {
            assert_eq!(
                with_auth(&srv, m, p, Some(bad), "m v=1 1\n").0,
                401,
                "{bad:?}"
            );
        }
    }
    // nothing leaked through the rejected writes
    assert!(e.list_series().is_empty());

    assert_eq!(
        with_auth(&srv, "POST", "/write", Some("Bearer s3cret"), "m v=1 1\n").0,
        204
    );
    assert_eq!(
        with_auth(&srv, "GET", "/metrics", Some("Bearer s3cret"), "").0,
        200
    );
    // header name is case-insensitive
    let raw = "GET /metrics HTTP/1.1\r\nauthorization: Bearer s3cret\r\n\r\n";
    assert_eq!(http(&srv, raw).0, 200);
    assert_eq!(e.list_series().len(), 1);

    srv.shutdown();
    drop(e);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn rejection_carries_a_challenge_header() {
    let (e, srv, dir) = setup_auth("chal", "t");
    let mut c = TcpStream::connect(srv.addr()).unwrap();
    c.write_all(b"GET /metrics HTTP/1.1\r\n\r\n").unwrap();
    let mut out = String::new();
    c.read_to_string(&mut out).unwrap();
    assert!(out.starts_with("HTTP/1.1 401 Unauthorized"), "{out}");
    assert!(out.contains("WWW-Authenticate: Bearer"), "{out}");
    srv.shutdown();
    drop(e);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn delete_over_http() {
    let (e, srv, dir) = setup("del");
    let mut lines = String::new();
    for i in 0..10 {
        lines.push_str(&format!("m,host=a value={i} {}\n", i * 1000));
        lines.push_str(&format!("m,host=b value={i} {}\n", i * 1000));
    }
    assert_eq!(post(&srv, "/write", &lines).0, 204);

    // all three parameters are required
    for q in ["", "?match=m", "?match=m&start=0", "?start=0&end=1000"] {
        let (c, b) = post(&srv, &format!("/delete{q}"), "");
        assert_eq!(c, 400, "{q}: {b}");
        assert!(b.contains("MISSING_PARAM"), "{b}");
    }
    assert_eq!(e.list_series().len(), 2);
    assert_eq!(
        e.query_range(
            &columnar_tsdb::SeriesKey::new("m").label("host", "a"),
            0,
            99_999
        )
        .unwrap()
        .len(),
        10
    );

    // only host=a, [2000, 5000)
    let q = "/delete?match=m%7Bhost%3D%22a%22%7D&start=2000&end=5000";
    let (c, b) = post(&srv, q, "");
    assert_eq!(c, 200, "{b}");
    assert!(b.contains("\"deleted\":3"), "{b}");
    let a = columnar_tsdb::SeriesKey::new("m").label("host", "a");
    let bk = columnar_tsdb::SeriesKey::new("m").label("host", "b");
    assert_eq!(e.query_range(&a, 0, 99_999).unwrap().len(), 7);
    assert_eq!(e.query_range(&bk, 0, 99_999).unwrap().len(), 10);

    // selectors only: no functions, ranges or garbage
    for bad in ["rate(m[1m])", "m[5m]", "m%20offset%205m", "m%7B", "1%2B1"] {
        let (c, _) = post(&srv, &format!("/delete?match={bad}&start=0&end=1"), "");
        assert_eq!(c, 400, "{bad}");
    }
    assert_eq!(get(&srv, "/delete").0, 405);
    srv.shutdown();
    drop(e);
    let _ = std::fs::remove_dir_all(dir);
}
