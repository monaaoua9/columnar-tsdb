use std::path::PathBuf;
use std::time::Duration;

use columnar_tsdb::query::{self, Ctx, Value};
use columnar_tsdb::{Config, Engine, SeriesKey};

fn open(tag: &str) -> (Engine, PathBuf) {
    let dir = std::env::temp_dir().join(format!("ctsdb-q-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let mut c = Config::new(&dir);
    c.bucket_width = 60_000;
    c.maintenance_interval = Duration::from_millis(20);
    (Engine::open(c).unwrap(), dir)
}

// 3 hosts, 10s scrape for 5 minutes. web-N counts up by N per scrape, load is flat N*10
fn seed(e: &Engine) {
    for n in 1..=3 {
        let host = format!("web-{n}");
        let reqs = SeriesKey::new("reqs")
            .label("host", &host)
            .label("dc", if n < 3 { "fra" } else { "ams" });
        let load = SeriesKey::new("load").label("host", &host);
        for i in 0..30 {
            e.append(&reqs, i * 10_000, (i * n) as f64).unwrap();
            e.append(&load, i * 10_000, (n * 10) as f64).unwrap();
        }
    }
}

fn vector(v: Value) -> Vec<(String, Vec<(i64, f64)>)> {
    let Value::Vector(rows) = v else {
        panic!("expected vector")
    };
    rows.into_iter()
        .map(|r| {
            let lbl = r
                .labels
                .iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect::<Vec<_>>()
                .join(",");
            (lbl, r.points.iter().map(|p| (p.ts, p.value)).collect())
        })
        .collect()
}

#[test]
fn instant_rate_per_host() {
    let (e, dir) = open("rate");
    seed(&e);
    let ctx = Ctx::instant(0, 300_000);
    let r = vector(query::run(&e, "rate(reqs[1m])", &ctx).unwrap());
    assert_eq!(r.len(), 3);
    // web-n adds n every 10s -> n/10 per second
    for (lbl, pts) in &r {
        let n: f64 = lbl.rsplit("web-").next().unwrap().parse().unwrap();
        assert_eq!(pts.len(), 1);
        assert!((pts[0].1 - n / 10.0).abs() < 1e-9, "{lbl}: {:?}", pts[0]);
    }
    drop(e);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn aggregation_grouping_and_filters() {
    let (e, dir) = open("agg");
    seed(&e);
    let ctx = Ctx::instant(0, 300_000);

    let total = vector(query::run(&e, "sum(load)", &ctx).unwrap());
    assert_eq!(total, vec![(String::new(), vec![(300_000, 60.0)])]);

    let by_dc = vector(query::run(&e, r#"sum(rate(reqs[1m])) by (dc)"#, &ctx).unwrap());
    assert_eq!(by_dc.len(), 2);
    assert_eq!(by_dc[0].0, "dc=ams");
    assert!((by_dc[0].1[0].1 - 0.3).abs() < 1e-9);
    assert!((by_dc[1].1[0].1 - 0.3).abs() < 1e-9); // fra: 0.1 + 0.2

    // =~ is a real regex now (whole-value match), =* is the old glob
    let re = vector(query::run(&e, r#"load{host=~"web-[12]"}"#, &ctx).unwrap());
    assert_eq!(re.len(), 2);
    let alt = vector(query::run(&e, r#"load{host=~"web-1|web-3"}"#, &ctx).unwrap());
    assert_eq!(alt.len(), 2);
    let partial = vector(query::run(&e, r#"load{host=~"web"}"#, &ctx).unwrap());
    assert!(partial.is_empty(), "regex must match the whole value");
    let not_re = vector(query::run(&e, r#"load{host!~"web-[12]"}"#, &ctx).unwrap());
    assert_eq!(not_re.len(), 1);
    let glob = vector(query::run(&e, r#"load{host=*"web-?", host!="web-2"}"#, &ctx).unwrap());
    assert_eq!(glob.len(), 2);
    assert!(query::run(&e, r#"load{host=~"(web"}"#, &ctx).is_err());

    let big = vector(query::run(&e, "load > 15", &ctx).unwrap());
    assert_eq!(big.len(), 2);
    let scaled = vector(query::run(&e, "max(load) * 2 + 1", &ctx).unwrap());
    assert_eq!(scaled[0].1[0].1, 61.0);

    drop(e);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn topk_and_vector_division() {
    let (e, dir) = open("topk");
    seed(&e);
    let ctx = Ctx::instant(0, 300_000);
    let top = vector(query::run(&e, "topk(2, load)", &ctx).unwrap());
    assert_eq!(top.len(), 2);
    assert!(top[0].0.contains("web-3") && top[1].0.contains("web-2"));
    let bot = vector(query::run(&e, "bottomk(1, load)", &ctx).unwrap());
    assert!(bot[0].0.contains("web-1"));

    // requests per unit of load, matched on identical labels (dc differs so use by)
    let ratio = vector(
        query::run(
            &e,
            "sum(rate(reqs[1m])) by (host) / sum(load) by (host)",
            &ctx,
        )
        .unwrap(),
    );
    assert_eq!(ratio.len(), 3);
    for (_, pts) in &ratio {
        assert!((pts[0].1 - 0.01).abs() < 1e-9);
    }
    drop(e);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn stepped_windows() {
    let (e, dir) = open("step");
    seed(&e);
    // tumbling 1m windows, 5 of them
    let ctx = Ctx::stepped(0, 300_000, 60_000);
    let r = vector(query::run(&e, "sum(max_over_time(load))", &ctx).unwrap());
    assert_eq!(r[0].1.len(), 5);
    assert!(r[0].1.iter().all(|p| p.1 == 60.0));

    // sliding 30s lookback every minute: 3 scrapes per host per window
    let r = vector(query::run(&e, "count_over_time(load{host=\"web-1\"}[30s])", &ctx).unwrap());
    assert_eq!(r[0].1.len(), 5);
    // first window only reaches back to t=0, so it holds a single scrape
    assert_eq!(r[0].1[0].1, 1.0);
    assert!(r[0].1[1..].iter().all(|p| p.1 == 3.0), "{:?}", r[0].1);
    drop(e);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn eval_errors() {
    let (e, dir) = open("err");
    seed(&e);
    let ctx = Ctx::instant(0, 300_000);
    assert!(query::run(&e, "sum(3)", &ctx).is_err());
    assert!(query::run(&e, "3 > 2", &ctx).is_err());
    assert!(query::run(&e, "load > ", &ctx).is_err());
    assert!(query::run(&e, "load", &Ctx::instant(5, 5)).is_err());
    assert!(query::run(&e, "load", &Ctx::stepped(0, 300_000, 0)).is_err());
    assert!(query::run(&e, "load", &Ctx::stepped(0, i64::MAX / 2, 1)).is_err());
    // unknown metric is just empty, not an error
    assert!(vector(query::run(&e, "nope", &ctx).unwrap()).is_empty());
    assert_eq!(query::run(&e, "2 * 3", &ctx).unwrap(), Value::Scalar(6.0));
    drop(e);
    let _ = std::fs::remove_dir_all(dir);
}
