use std::io::Write;
use std::path::PathBuf;
use std::process::{Command, Output, Stdio};

fn tmp(tag: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("ctsdb-cli-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    d
}

fn tsdb(args: &[&str], stdin: Option<&str>) -> Output {
    let mut c = Command::new(env!("CARGO_BIN_EXE_tsdb"));
    c.args(args).stdout(Stdio::piped()).stderr(Stdio::piped());
    if stdin.is_some() {
        c.stdin(Stdio::piped());
    }
    let mut child = c.spawn().unwrap();
    if let Some(s) = stdin {
        child.stdin.take().unwrap().write_all(s.as_bytes()).unwrap();
    }
    child.wait_with_output().unwrap()
}

fn text(o: &Output) -> String {
    String::from_utf8_lossy(&o.stdout).into_owned()
}

#[test]
fn import_query_export_roundtrip() {
    let d = tmp("rt");
    let ds = d.to_str().unwrap();
    let input = "reqs,host=a value=10 1000\nreqs,host=a value=20 2000\nreqs,host=b value=5 1500\n";

    let o = tsdb(&["import", ds, "-"], Some(input));
    assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    assert!(text(&o).contains("imported 3 points"));

    let o = tsdb(
        &["query", ds, "sum(reqs)", "--start", "0", "--end", "3000"],
        None,
    );
    assert!(o.status.success());
    assert_eq!(text(&o).trim(), "{} 25 @ 3000");

    let o = tsdb(
        &[
            "query", ds, "reqs", "--start", "0", "--end", "3000", "--step", "1s",
        ],
        None,
    );
    let t = text(&o);
    assert!(t.contains("host=a") && t.contains("20 @ 2000"), "{t}");

    let o = tsdb(&["export", ds], None);
    let lines: Vec<String> = text(&o).lines().map(String::from).collect();
    assert_eq!(lines.len(), 3);
    assert!(
        lines.contains(&"reqs,host=b value=5 1500".to_string()),
        "{lines:?}"
    );

    let o = tsdb(&["series", ds], None);
    assert_eq!(text(&o).lines().count(), 2);

    // what we exported imports cleanly into a fresh db
    let d2 = tmp("rt2");
    let o = tsdb(
        &["import", d2.to_str().unwrap(), "-"],
        Some(&lines.join("\n")),
    );
    assert!(o.status.success());
    let o = tsdb(&["export", d2.to_str().unwrap(), "reqs"], None);
    assert_eq!(text(&o).lines().count(), 3);

    let _ = std::fs::remove_dir_all(d);
    let _ = std::fs::remove_dir_all(d2);
}

#[test]
fn stats_compact_snapshot() {
    let d = tmp("ops");
    let ds = d.to_str().unwrap();
    let mut input = String::new();
    for i in 0..500 {
        input.push_str(&format!("m value={} {}\n", i % 7, i * 1000));
    }
    assert!(tsdb(&["import", ds, "-"], Some(&input)).status.success());

    let o = tsdb(&["stats", ds], None);
    let t = text(&o);
    assert!(t.contains("sealed points     500"), "{t}");
    assert!(t.contains("ratio vs"), "{t}");

    assert!(tsdb(&["compact", ds], None).status.success());

    let snap = tmp("ops-snap");
    let o = tsdb(&["snapshot", ds, snap.to_str().unwrap()], None);
    assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    let o = tsdb(&["export", snap.to_str().unwrap()], None);
    assert_eq!(text(&o).lines().count(), 500);

    // second snapshot into the same dir is refused
    let o = tsdb(&["snapshot", ds, snap.to_str().unwrap()], None);
    assert!(!o.status.success());

    let _ = std::fs::remove_dir_all(d);
    let _ = std::fs::remove_dir_all(snap);
}

#[test]
fn bad_input_exits_nonzero() {
    let d = tmp("bad");
    let ds = d.to_str().unwrap();
    for args in [
        vec![],
        vec!["nope", ds],
        vec!["query", ds],
        vec!["query", ds, "rate("],
        vec!["query", ds, "x", "--bogus", "1"],
        vec!["query", ds, "x", "--start"],
        vec!["snapshot", ds],
    ] {
        let o = tsdb(&args, None);
        assert!(!o.status.success(), "{args:?} should fail");
        assert!(!o.stderr.is_empty(), "{args:?} printed nothing");
    }
    let o = tsdb(&["import", ds, "-"], Some("this is not a line"));
    assert!(!o.status.success());
    let _ = std::fs::remove_dir_all(d);
}

#[test]
fn serve_requires_the_token_when_given() {
    use std::io::{BufRead, BufReader, Read};
    use std::net::TcpStream;

    let d = tmp("serve");
    let mut child = Command::new(env!("CARGO_BIN_EXE_tsdb"))
        .args([
            "serve",
            d.to_str().unwrap(),
            "127.0.0.1:0",
            "--token",
            "tok",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut line = String::new();
    BufReader::new(child.stdout.as_mut().unwrap())
        .read_line(&mut line)
        .unwrap();
    let addr = line
        .trim()
        .strip_prefix("listening on http://")
        .unwrap()
        .to_string();

    let ask = |hdr: &str| {
        let mut c = TcpStream::connect(&addr).unwrap();
        c.write_all(format!("GET /metrics HTTP/1.1\r\n{hdr}\r\n").as_bytes())
            .unwrap();
        let mut out = String::new();
        c.read_to_string(&mut out).unwrap();
        out.split(' ').nth(1).unwrap().to_string()
    };
    assert_eq!(ask(""), "401");
    assert_eq!(ask("Authorization: Bearer tok\r\n"), "200");

    child.kill().unwrap();
    child.wait().unwrap();
    let _ = std::fs::remove_dir_all(d);
}

#[test]
fn delete_command() {
    let d = tmp("delete");
    let ds = d.to_str().unwrap();
    let mut input = String::new();
    for i in 0..20 {
        input.push_str(&format!("m,host=a value={i} {}\n", i * 1000));
        input.push_str(&format!("m,host=b value={i} {}\n", i * 1000));
    }
    assert!(tsdb(&["import", ds, "-"], Some(&input)).status.success());

    // bounds are mandatory
    for args in [
        vec!["delete", ds, "m"],
        vec!["delete", ds, "m", "--start", "0"],
        vec!["delete", ds, "--start", "0", "--end", "1"],
        vec!["delete", ds, "rate(m[1m])", "--start", "0", "--end", "1"],
    ] {
        let o = tsdb(&args, None);
        assert!(!o.status.success(), "{args:?} should fail");
    }
    assert_eq!(text(&tsdb(&["export", ds], None)).lines().count(), 40);

    let o = tsdb(
        &[
            "delete",
            ds,
            "m{host=\"b\"}",
            "--start",
            "5000",
            "--end",
            "15000",
        ],
        None,
    );
    assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    assert!(text(&o).contains("deleted 10 points"), "{}", text(&o));

    let out = text(&tsdb(&["export", ds], None));
    assert_eq!(out.lines().count(), 30);
    assert_eq!(out.lines().filter(|l| l.contains("host=a")).count(), 20);
    // and it is still gone after another open
    assert_eq!(text(&tsdb(&["export", ds, "m"], None)).lines().count(), 30);
    let _ = std::fs::remove_dir_all(d);
}
