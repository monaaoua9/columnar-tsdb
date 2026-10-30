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
