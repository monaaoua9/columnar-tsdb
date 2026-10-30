use std::fs::File;
use std::io::{self, BufWriter, Read, Write};
use std::process::ExitCode;
use std::time::{SystemTime, UNIX_EPOCH};

use columnar_tsdb::lineproto;
use columnar_tsdb::query::{self, parse_duration, Ctx, Value};
use columnar_tsdb::server::Server;
use columnar_tsdb::{Config, Engine, Error};

const USAGE: &str = "\
usage: tsdb <command> <data-dir> [args]

  import   <dir> [file|-]            load line protocol (stdin when no file)
  export   <dir> [metric]            dump points as line protocol
  query    <dir> <expr> [--start T] [--end T] [--step D]
  series   <dir>                     list series
  stats    <dir>                     sizes and counters
  compact  <dir>                     merge every bucket
  snapshot <dir> <dest>              online copy into an empty dir
  serve    <dir> [addr]              http on addr (default 127.0.0.1:8086)

T is an integer, `now` or `now-5m`; D is an integer or a duration like 30s.";

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as i64)
}

fn die(msg: impl std::fmt::Display) -> ExitCode {
    eprintln!("tsdb: {msg}");
    ExitCode::from(1)
}

fn time_arg(s: &str, now: i64) -> Result<i64, Error> {
    if s == "now" {
        return Ok(now);
    }
    if let Some(d) = s.strip_prefix("now-") {
        return Ok(now - parse_duration(d)?);
    }
    s.parse()
        .map_err(|_| Error::Query(format!("bad time {s:?}")))
}

// --flag value pairs after the positionals; unknown flags are an error
fn flags<'a>(args: &'a [String], known: &[&str]) -> Result<Vec<(&'a str, &'a str)>, String> {
    let mut out = Vec::new();
    let mut it = args.iter();
    while let Some(a) = it.next() {
        if !known.contains(&a.as_str()) {
            return Err(format!("unknown argument {a:?}"));
        }
        let v = it.next().ok_or_else(|| format!("{a} needs a value"))?;
        out.push((a.as_str(), v.as_str()));
    }
    Ok(out)
}

fn open(dir: &str) -> Result<Engine, Error> {
    Engine::open(Config::new(dir))
}

fn print_value(v: &Value, out: &mut impl Write) -> io::Result<()> {
    match v {
        Value::Scalar(x) => writeln!(out, "{x}"),
        Value::Vector(rows) => {
            for r in rows {
                let lbl: Vec<String> = r.labels.iter().map(|(k, v)| format!("{k}={v}")).collect();
                for p in &r.points {
                    writeln!(out, "{{{}}} {} @ {}", lbl.join(","), p.value, p.ts)?;
                }
            }
            Ok(())
        }
    }
}

fn run(args: &[String]) -> Result<(), String> {
    let (cmd, dir) = match args {
        [c, d, ..] => (c.as_str(), d.as_str()),
        _ => return Err(USAGE.into()),
    };
    let rest = &args[2..];
    let e = |x: Error| x.to_string();
    let stdout = io::stdout();
    let mut out = BufWriter::new(stdout.lock());
    let io_err = |x: io::Error| x.to_string();

    match cmd {
        "import" => {
            let mut text = String::new();
            match rest.first().map(String::as_str) {
                None | Some("-") => io::stdin().read_to_string(&mut text).map_err(io_err)?,
                Some(path) => File::open(path)
                    .map_err(io_err)?
                    .read_to_string(&mut text)
                    .map_err(io_err)?,
            };
            let eng = open(dir).map_err(e)?;
            let n = lineproto::ingest(&eng, &text, now_ms()).map_err(e)?;
            eng.flush().map_err(e)?;
            writeln!(out, "imported {n} points").map_err(io_err)?;
        }
        "export" => {
            let eng = open(dir).map_err(e)?;
            for k in eng.list_series() {
                if rest.first().is_some_and(|m| m != k.name()) {
                    continue;
                }
                for p in eng.query_range(&k, i64::MIN, i64::MAX).map_err(e)? {
                    writeln!(out, "{}", lineproto::format_line(&k, &p)).map_err(io_err)?;
                }
            }
        }
        "query" => {
            let Some(expr) = rest.first() else {
                return Err("query needs an expression".into());
            };
            let fl = flags(&rest[1..], &["--start", "--end", "--step"])?;
            let get = |n: &str| fl.iter().find(|f| f.0 == n).map(|f| f.1);
            let now = now_ms();
            let end = get("--end")
                .map_or(Ok(now), |t| time_arg(t, now))
                .map_err(e)?;
            let start = get("--start")
                .map_or(Ok(end - 3_600_000), |t| time_arg(t, now))
                .map_err(e)?;
            let ctx = match get("--step") {
                Some(s) => Ctx::stepped(start, end, parse_duration(s).map_err(e)?),
                None => Ctx::instant(start, end),
            };
            let eng = open(dir).map_err(e)?;
            let v = query::run(&eng, expr, &ctx).map_err(e)?;
            print_value(&v, &mut out).map_err(io_err)?;
        }
        "series" => {
            let eng = open(dir).map_err(e)?;
            for k in eng.list_series() {
                let l: Vec<String> = k.labels().iter().map(|(a, b)| format!("{a}={b}")).collect();
                writeln!(out, "{}{{{}}}", k.name(), l.join(",")).map_err(io_err)?;
            }
        }
        "stats" => {
            let eng = open(dir).map_err(e)?;
            let st = eng.stats();
            let raw = st.sealed_points * 16;
            writeln!(out, "series            {}", st.series).map_err(io_err)?;
            writeln!(out, "sealed chunks     {}", st.sealed_chunks).map_err(io_err)?;
            writeln!(out, "sealed points     {}", st.sealed_points).map_err(io_err)?;
            writeln!(out, "compressed bytes  {}", st.compressed_bytes).map_err(io_err)?;
            if st.compressed_bytes > 0 {
                let ratio = raw as f64 / st.compressed_bytes as f64;
                writeln!(out, "ratio vs 16 B/pt  {ratio:.2}x").map_err(io_err)?;
            }
            writeln!(out, "wal bytes         {}", st.wal_bytes).map_err(io_err)?;
        }
        "compact" => {
            let eng = open(dir).map_err(e)?;
            eng.flush().map_err(e)?;
            let n = eng.compact().map_err(e)?;
            writeln!(out, "merged {n} buckets").map_err(io_err)?;
        }
        "snapshot" => {
            let Some(dest) = rest.first() else {
                return Err("snapshot needs a destination dir".into());
            };
            let eng = open(dir).map_err(e)?;
            let i = eng.snapshot(dest).map_err(e)?;
            writeln!(
                out,
                "{} chunks, {} bytes linked, {} bytes copied",
                i.chunks, i.linked_bytes, i.copied_bytes
            )
            .map_err(io_err)?;
        }
        "serve" => {
            let addr = rest.first().map_or("127.0.0.1:8086", String::as_str);
            let eng = open(dir).map_err(e)?;
            let srv = Server::start(eng, addr).map_err(e)?;
            writeln!(out, "listening on http://{}", srv.addr()).map_err(io_err)?;
            out.flush().map_err(io_err)?;
            // no signal handling without deps: ctrl-c kills us, the wal covers the rest
            loop {
                std::thread::park();
            }
        }
        _ => return Err(USAGE.into()),
    }
    out.flush().map_err(io_err)
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match run(&args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(m) => die(m),
    }
}
