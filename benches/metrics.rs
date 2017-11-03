#![feature(test)]

extern crate cernan;
extern crate test;

use self::test::Bencher;
use cernan::metric::{TagMap, Telemetry};
use cernan::protocols::graphite::parse_graphite;
use cernan::protocols::statsd::parse_statsd;
use cernan::source::StatsdParseConfig;
use std::sync;

#[bench]
fn bench_merge_tags_from_map(b: &mut Bencher) {
    b.iter(|| {
        let m0 = Telemetry::new()
            .name("one")
            .value(1.0)
            .harden()
            .unwrap()
            .overlay_tag("one", "1")
            .overlay_tag("two", "2")
            .overlay_tag("three", "3");
        let mut tm: TagMap = Default::default();
        tm.insert(String::from("two"), String::from("22"));
        tm.insert(String::from("four"), String::from("4"));
        m0.merge_tags_from_map(&tm);
    });
}

#[bench]
fn bench_statsd_incr_gauge_no_sample(b: &mut Bencher) {
    b.iter(|| {
        let metric = sync::Arc::new(Some(Telemetry::default()));
        let mut res = Vec::new();
        let config = sync::Arc::new(StatsdParseConfig::default());
        parse_statsd("a.b:+12.1|g\n", &mut res, &metric, &config);
    });
}

#[bench]
fn bench_statsd_incr_gauge_with_sample(b: &mut Bencher) {
    b.iter(|| {
        let metric = sync::Arc::new(Some(Telemetry::default()));
        let mut res = Vec::new();
        let config = sync::Arc::new(StatsdParseConfig::default());
        parse_statsd("a.b:+12.1|g@2.2\n", &mut res, &metric, &config);
    });
}

#[bench]
fn bench_statsd_gauge_no_sample(b: &mut Bencher) {
    b.iter(|| {
        let metric = sync::Arc::new(Some(Telemetry::default()));
        let mut res = Vec::new();
        let config = sync::Arc::new(StatsdParseConfig::default());
        parse_statsd("a.b:12.1|g\n", &mut res, &metric, &config);
    });
}

#[bench]
fn bench_statsd_gauge_mit_sample(b: &mut Bencher) {
    b.iter(|| {
        let metric = sync::Arc::new(Some(Telemetry::default()));
        let mut res = Vec::new();
        let config = sync::Arc::new(StatsdParseConfig::default());
        parse_statsd("a.b:12.1|g@0.22\n", &mut res, &metric, &config);
    });
}

#[bench]
fn bench_statsd_counter_no_sample(b: &mut Bencher) {
    b.iter(|| {
        let metric = sync::Arc::new(Some(Telemetry::default()));
        let mut res = Vec::new();
        let config = sync::Arc::new(StatsdParseConfig::default());
        parse_statsd("a.b:12.1|c\n", &mut res, &metric, &config);
    });
}

#[bench]
fn bench_statsd_counter_with_sample(b: &mut Bencher) {
    b.iter(|| {
        let metric = sync::Arc::new(Some(Telemetry::default()));
        let mut res = Vec::new();
        let config = sync::Arc::new(StatsdParseConfig::default());
        parse_statsd("a.b:12.1|c@1.0\n", &mut res, &metric, &config);
    });
}

#[bench]
fn bench_statsd_timer(b: &mut Bencher) {
    b.iter(|| {
        let metric = sync::Arc::new(Some(Telemetry::default()));
        let mut res = Vec::new();
        let config = sync::Arc::new(StatsdParseConfig::default());
        parse_statsd("a.b:12.1|ms\n", &mut res, &metric, &config);
    });
}

#[bench]
fn bench_statsd_histogram(b: &mut Bencher) {
    b.iter(|| {
        let metric = sync::Arc::new(Some(Telemetry::default()));
        let mut res = Vec::new();
        let config = sync::Arc::new(StatsdParseConfig::default());
        parse_statsd("a.b:12.1|h\n", &mut res, &metric, &config);
    });
}

#[bench]
fn bench_graphite(b: &mut Bencher) {
    b.iter(|| {
        let metric = sync::Arc::new(Some(Telemetry::default()));
        let mut res = Vec::new();
        parse_graphite("fst 1 101\n", &mut res, &metric);
    });
}
