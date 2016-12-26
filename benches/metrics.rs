#![feature(test)]

extern crate test;
extern crate cernan;

use cernan::metric::{Metric, TagMap};
use self::test::Bencher;

#[bench]
fn bench_merge_tags_from_map(b: &mut Bencher) {
    b.iter(|| {
        let m0 = Metric::new("one", 1.0)
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
        Metric::parse_statsd("a.b:+12.1|g\n", Default::default()).unwrap();
    });
}

#[bench]
fn bench_statsd_incr_gauge_with_sample(b: &mut Bencher) {
    b.iter(|| {
        Metric::parse_statsd("a.b:+12.1|g@2.2\n", Default::default()).unwrap();
    });
}

#[bench]
fn bench_statsd_gauge_no_sample(b: &mut Bencher) {
    b.iter(|| {
        Metric::parse_statsd("a.b:12.1|g\n", Default::default()).unwrap();
    });
}

#[bench]
fn bench_statsd_gauge_mit_sample(b: &mut Bencher) {
    b.iter(|| {
        Metric::parse_statsd("a.b:12.1|g@0.22\n", Default::default()).unwrap();
    });
}

#[bench]
fn bench_statsd_counter_no_sample(b: &mut Bencher) {
    b.iter(|| {
        Metric::parse_statsd("a.b:12.1|c\n", Default::default()).unwrap();
    });
}

#[bench]
fn bench_statsd_counter_with_sample(b: &mut Bencher) {
    b.iter(|| {
        Metric::parse_statsd("a.b:12.1|c@1.0\n", Default::default()).unwrap();
    });
}

#[bench]
fn bench_statsd_timer(b: &mut Bencher) {
    b.iter(|| {
        Metric::parse_statsd("a.b:12.1|ms\n", Default::default()).unwrap();
    });
}

#[bench]
fn bench_statsd_histogram(b: &mut Bencher) {
    b.iter(|| {
        Metric::parse_statsd("a.b:12.1|h\n", Default::default()).unwrap();
    });
}

#[bench]
fn bench_graphite(b: &mut Bencher) {
    b.iter(|| {
        Metric::parse_graphite("fst 1 101\n").unwrap();
    });
}
