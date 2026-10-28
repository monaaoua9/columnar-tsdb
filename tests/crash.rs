// Real crashes: the writer runs as a separate process and is killed (TerminateProcess /
// SIGKILL, no destructors, no flush) at an arbitrary moment, possibly in the middle of a wal
// append, a chunk write, a late-point merge or a checkpoint. Then a second process reopens the
// directory and checks that nothing acknowledged was lost or corrupted.
use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};

const EXE: &str = env!("CARGO_BIN_EXE_crash_writer");

// returns how many batches the writer had acknowledged when it died
fn run_and_kill(dir: &str, first: u64, extra: u64) -> u64 {
    let mut child = Command::new(EXE)
        .args(["write", dir, &first.to_string()])
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    let mut out = BufReader::new(child.stdout.take().unwrap());

    let mut acked = first;
    let mut line = String::new();
    while acked < first + extra {
        line.clear();
        assert!(out.read_line(&mut line).unwrap() > 0, "writer exited early");
        acked = line.trim().strip_prefix("ack ").unwrap().parse().unwrap();
    }
    child.kill().unwrap();

    // acks already sitting in the pipe were printed after append returned, so they count
    for l in out.lines().map_while(|l| l.ok()) {
        if let Some(n) = l.strip_prefix("ack ") {
            acked = n.parse().unwrap();
        }
    }
    child.wait().unwrap();
    acked
}

fn verify(dir: &str, acked: u64) {
    let o = Command::new(EXE)
        .args(["verify", dir, &acked.to_string()])
        .output()
        .unwrap();
    assert!(
        o.status.success(),
        "verify failed after {acked} acked batches:\n{}",
        String::from_utf8_lossy(&o.stderr)
    );
}

#[test]
fn survives_repeated_hard_kills() {
    let dir = std::env::temp_dir().join(format!("ctsdb-crash-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let dir_s = dir.to_str().unwrap();

    // varying run lengths so kills land on different phases (plain appends, periodic flush at
    // every 40th batch, compact at every 25th, background merges in between)
    let mut acked = 0;
    for extra in [40, 90, 11, 250, 25, 150, 60, 400, 4, 100] {
        acked = run_and_kill(dir_s, acked, extra);
        verify(dir_s, acked);
    }
    assert!(acked > 1_000, "only {acked} batches acknowledged in total");
    std::fs::remove_dir_all(dir).unwrap();
}
