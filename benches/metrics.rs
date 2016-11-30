#![feature(test)]

extern crate test;
extern crate cernan;

use self::test::Bencher;

use cernan::metric::Metric;

#[bench]
fn bench_statsd_incr_gauge_no_sample(b: &mut Bencher) {
    b.iter(|| {
        Metric::parse_statsd("a.b:+12.1|g\n").unwrap();
    });
}

#[bench]
fn bench_statsd_incr_gauge_with_sample(b: &mut Bencher) {
    b.iter(|| {
        Metric::parse_statsd("a.b:+12.1|g@2.2\n").unwrap();
    });
}

#[bench]
fn bench_statsd_gauge_no_sample(b: &mut Bencher) {
    b.iter(|| {
        Metric::parse_statsd("a.b:12.1|g\n").unwrap();
    });
}

#[bench]
fn bench_statsd_gauge_mit_sample(b: &mut Bencher) {
    b.iter(|| {
        Metric::parse_statsd("a.b:12.1|g@0.22\n").unwrap();
    });
}

#[bench]
fn bench_statsd_counter_no_sample(b: &mut Bencher) {
    b.iter(|| {
        Metric::parse_statsd("a.b:12.1|c\n").unwrap();
    });
}

#[bench]
fn bench_statsd_counter_with_sample(b: &mut Bencher) {
    b.iter(|| {
        Metric::parse_statsd("a.b:12.1|c@1.0\n").unwrap();
    });
}

#[bench]
fn bench_statsd_timer(b: &mut Bencher) {
    b.iter(|| {
        Metric::parse_statsd("a.b:12.1|ms\n").unwrap();
    });
}

#[bench]
fn bench_statsd_histogram(b: &mut Bencher) {
    b.iter(|| {
        Metric::parse_statsd("a.b:12.1|h\n").unwrap();
    });
}

#[bench]
fn bench_graphite(b: &mut Bencher) {
    b.iter(|| {
        Metric::parse_graphite("fst 1 101\n").unwrap();
    });
}
