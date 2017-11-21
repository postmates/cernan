#![feature(test)]

extern crate cernan;
extern crate chrono;
extern crate test;

use self::test::Bencher;
use cernan::buckets;
use cernan::metric::{AggregationMethod, Telemetry};
use chrono::{TimeZone, Utc};

#[bench]
fn bench_single_timer(b: &mut Bencher) {
    let dt_0 = Utc.ymd(1972, 12, 11)
        .and_hms_milli(11, 59, 49, 0)
        .timestamp();

    b.iter(|| {
        let mut bucket = buckets::Buckets::default();

        bucket.add(
            Telemetry::new()
                .name("a")
                .value(1.0)
                .kind(AggregationMethod::Summarize)
                .harden()
                .unwrap()
                .timestamp(dt_0),
        );
    });
}

#[bench]
fn bench_single_timer_100(b: &mut Bencher) {
    let dt_0 = Utc.ymd(1972, 12, 11)
        .and_hms_milli(11, 59, 49, 0)
        .timestamp();

    b.iter(|| {
        let mut bucket = buckets::Buckets::default();

        for _ in 0..100 {
            bucket.add(
                Telemetry::new()
                    .name("a")
                    .value(1.0)
                    .kind(AggregationMethod::Summarize)
                    .harden()
                    .unwrap()
                    .timestamp(dt_0),
            );
        }
    });
}

#[bench]
fn bench_single_timer_1000(b: &mut Bencher) {
    let dt_0 = Utc.ymd(1972, 12, 11)
        .and_hms_milli(11, 59, 49, 0)
        .timestamp();

    b.iter(|| {
        let mut bucket = buckets::Buckets::default();

        for _ in 0..1000 {
            bucket.add(
                Telemetry::new()
                    .name("a")
                    .value(1.0)
                    .kind(AggregationMethod::Summarize)
                    .harden()
                    .unwrap()
                    .timestamp(dt_0),
            );
        }
    });
}

#[bench]
fn bench_single_timer_10000(b: &mut Bencher) {
    let dt_0 = Utc.ymd(1972, 12, 11)
        .and_hms_milli(11, 59, 49, 0)
        .timestamp();

    b.iter(|| {
        let mut bucket = buckets::Buckets::default();

        for _ in 0..10_000 {
            bucket.add(
                Telemetry::new()
                    .name("a")
                    .value(1.0)
                    .kind(AggregationMethod::Summarize)
                    .harden()
                    .unwrap()
                    .timestamp(dt_0),
            );
        }
    });
}

#[bench]
fn bench_single_histogram(b: &mut Bencher) {
    let dt_0 = Utc.ymd(1972, 12, 11)
        .and_hms_milli(11, 59, 49, 0)
        .timestamp();

    b.iter(|| {
        let mut bucket = buckets::Buckets::default();

        bucket.add(
            Telemetry::new()
                .name("a")
                .value(1.0)
                .kind(AggregationMethod::Summarize)
                .harden()
                .unwrap()
                .timestamp(dt_0),
        );
    });
}

#[bench]
fn bench_single_histogram_100(b: &mut Bencher) {
    let dt_0 = Utc.ymd(1972, 12, 11)
        .and_hms_milli(11, 59, 49, 0)
        .timestamp();

    b.iter(|| {
        let mut bucket = buckets::Buckets::default();

        for _ in 0..100 {
            bucket.add(
                Telemetry::new()
                    .name("a")
                    .value(1.0)
                    .kind(AggregationMethod::Summarize)
                    .harden()
                    .unwrap()
                    .timestamp(dt_0),
            );
        }
    });
}

#[bench]
fn bench_single_histogram_1000(b: &mut Bencher) {
    let dt_0 = Utc.ymd(1972, 12, 11)
        .and_hms_milli(11, 59, 49, 0)
        .timestamp();

    b.iter(|| {
        let mut bucket = buckets::Buckets::default();

        for _ in 0..1_000 {
            bucket.add(
                Telemetry::new()
                    .name("a")
                    .value(1.0)
                    .kind(AggregationMethod::Summarize)
                    .harden()
                    .unwrap()
                    .timestamp(dt_0),
            );
        }
    });
}

#[bench]
fn bench_single_histogram_10000(b: &mut Bencher) {
    let dt_0 = Utc.ymd(1972, 12, 11)
        .and_hms_milli(11, 59, 49, 0)
        .timestamp();

    b.iter(|| {
        let mut bucket = buckets::Buckets::default();

        for _ in 0..10_000 {
            bucket.add(
                Telemetry::new()
                    .name("a")
                    .value(1.0)
                    .kind(AggregationMethod::Summarize)
                    .harden()
                    .unwrap()
                    .timestamp(dt_0),
            );
        }
    });
}

#[bench]
fn bench_multi_counters(b: &mut Bencher) {
    let dt_0 = Utc.ymd(1972, 12, 11)
        .and_hms_milli(11, 59, 49, 0)
        .timestamp();
    let dt_1 = Utc.ymd(1972, 12, 14)
        .and_hms_milli(5, 40, 56, 0)
        .timestamp();


    b.iter(|| {
        let mut bucket = buckets::Buckets::default();

        for name in &["a", "aa", "aaa", "aaaa", "aaaaa", "aaaaaa", "aaaaaa"] {
            // 7
            for i in &[-5.0, -4.0, -3.0, -2.0, -1.0, 0.0, 1.0, 2.0, 3.0, 4.0, 5.0] {
                // 11
                bucket.add(
                    Telemetry::new()
                        .name(*name)
                        .value(*i)
                        .kind(AggregationMethod::Sum)
                        .harden()
                        .unwrap()
                        .timestamp(dt_0),
                );
                bucket.add(
                    Telemetry::new()
                        .name(*name)
                        .value(*i)
                        .kind(AggregationMethod::Sum)
                        .harden()
                        .unwrap()
                        .timestamp(dt_1),
                );
            }
        }
        // total inserts 7 * 11 * 2 * 3 = 462
    });
}

#[bench]
fn bench_single_counter(b: &mut Bencher) {
    let dt_0 = Utc.ymd(1972, 12, 11)
        .and_hms_milli(11, 59, 49, 0)
        .timestamp();

    b.iter(|| {
        let mut bucket = buckets::Buckets::default();

        bucket.add(
            Telemetry::new()
                .name("a")
                .value(1.0)
                .kind(AggregationMethod::Sum)
                .harden()
                .unwrap()
                .timestamp(dt_0),
        );
    });
}

#[bench]
fn bench_multi_gauges(b: &mut Bencher) {
    let dt_0 = Utc.ymd(1972, 12, 11)
        .and_hms_milli(11, 59, 49, 0)
        .timestamp();
    let dt_1 = Utc.ymd(1972, 12, 14)
        .and_hms_milli(5, 40, 56, 0)
        .timestamp();

    b.iter(|| {
        let mut bucket = buckets::Buckets::default();

        for name in &["a", "aa", "aaa", "aaaa", "aaaaa", "aaaaaa", "aaaaaa"] {
            // 7
            for i in &[-5.0, -4.0, -3.0, -2.0, -1.0, 0.0, 1.0, 2.0, 3.0, 4.0, 5.0] {
                // 11
                bucket.add(
                    Telemetry::new()
                        .name(*name)
                        .value(*i)
                        .kind(AggregationMethod::Set)
                        .harden()
                        .unwrap()
                        .timestamp(dt_0),
                );
                bucket.add(
                    Telemetry::new()
                        .name(*name)
                        .value(*i)
                        .kind(AggregationMethod::Set)
                        .harden()
                        .unwrap()
                        .timestamp(dt_1),
                );
            }
        }
        // total inserts 7 * 11 * 2 = 154
    });
}

#[bench]
fn bench_single_gauge(b: &mut Bencher) {
    let dt_0 = Utc.ymd(1972, 12, 11)
        .and_hms_milli(11, 59, 49, 0)
        .timestamp();

    b.iter(|| {
        let mut bucket = buckets::Buckets::default();

        bucket.add(
            Telemetry::new()
                .name("a")
                .value(1.0)
                .kind(AggregationMethod::Set)
                .harden()
                .unwrap()
                .timestamp(dt_0),
        );
    });
}
