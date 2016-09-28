#![feature(test)]

extern crate test;
extern crate cernan;
extern crate chrono;
extern crate string_cache;

use self::test::Bencher;

use chrono::{UTC,TimeZone};
use string_cache::Atom;
use cernan::buckets;
use cernan::metric::{Metric,MetricKind};

#[bench]
fn bench_single_timer(b: &mut Bencher) {
    let dt_0 = UTC.ymd(1972, 12, 11).and_hms_milli(11, 59, 49, 0).timestamp();

    b.iter(|| {
        let mut bucket = buckets::Buckets::default();

        bucket.add(Metric::new_with_time(Atom::from("a"),
                                         1.0,
                                         Some(dt_0),
                                         MetricKind::Timer,
                                         None));
    });
}

#[bench]
fn bench_single_histogram(b: &mut Bencher) {
    let dt_0 = UTC.ymd(1972, 12, 11).and_hms_milli(11, 59, 49, 0).timestamp();

    b.iter(|| {
        let mut bucket = buckets::Buckets::default();

        bucket.add(Metric::new_with_time(Atom::from("a"),
                                         1.0,
                                         Some(dt_0),
                                         MetricKind::Histogram,
                                         None));
    });
}

#[bench]
fn bench_multi_counters(b: &mut Bencher) {
    let dt_0 = UTC.ymd(1972, 12, 11).and_hms_milli(11, 59, 49, 0).timestamp();
    let dt_1 = UTC.ymd(1972, 12, 14).and_hms_milli(5, 40, 56, 0).timestamp();


    b.iter(|| {
        let mut bucket = buckets::Buckets::default();

        for name in &["a", "aa", "aaa", "aaaa", "aaaaa", "aaaaaa", "aaaaaa"] {       // 7
            for i in &[-5.0, -4.0, -3.0, -2.0, -1.0, 0.0, 1.0, 2.0, 3.0, 4.0, 5.0] { // 11
                for r in &[-1.0, 0.0, 1.0] {                                         // 3
                    bucket.add(Metric::new_with_time(Atom::from(*name),
                                                     *i,
                                                     Some(dt_0),
                                                     MetricKind::Counter(*r),
                                                     None));
                    bucket.add(Metric::new_with_time(Atom::from(*name),
                                                     *i,
                                                     Some(dt_1),
                                                     MetricKind::Counter(*r),
                                                     None));
                }
            }
        }
        // total inserts 7 * 11 * 2 * 3 = 462
    });
}

#[bench]
fn bench_single_counter(b: &mut Bencher) {
    let dt_0 = UTC.ymd(1972, 12, 11).and_hms_milli(11, 59, 49, 0).timestamp();

    b.iter(|| {
        let mut bucket = buckets::Buckets::default();

        bucket.add(Metric::new_with_time(Atom::from("a"),
                                         1.0,
                                         Some(dt_0),
                                         MetricKind::Counter(1.0),
                                         None));
    });
}

#[bench]
fn bench_multi_gauges(b: &mut Bencher) {
    let dt_0 = UTC.ymd(1972, 12, 11).and_hms_milli(11, 59, 49, 0).timestamp();
    let dt_1 = UTC.ymd(1972, 12, 14).and_hms_milli(5, 40, 56, 0).timestamp();

    b.iter(|| {
        let mut bucket = buckets::Buckets::default();

        for name in &["a", "aa", "aaa", "aaaa", "aaaaa", "aaaaaa", "aaaaaa"] {       // 7
            for i in &[-5.0, -4.0, -3.0, -2.0, -1.0, 0.0, 1.0, 2.0, 3.0, 4.0, 5.0] { // 11
                bucket.add(Metric::new_with_time(Atom::from(*name),
                                                 *i,
                                                 Some(dt_0),
                                                 MetricKind::Gauge,
                                                 None));
                bucket.add(Metric::new_with_time(Atom::from(*name),
                                                 *i,
                                                 Some(dt_1),
                                                 MetricKind::Gauge,
                                                 None));
            }
        }
        // total inserts 7 * 11 * 2 = 154
    });
}

#[bench]
fn bench_single_gauge(b: &mut Bencher) {
    let dt_0 = UTC.ymd(1972, 12, 11).and_hms_milli(11, 59, 49, 0).timestamp();

    b.iter(|| {
        let mut bucket = buckets::Buckets::default();

        bucket.add(Metric::new_with_time(Atom::from("a"),
                                         1.0,
                                         Some(dt_0),
                                         MetricKind::Gauge,
                                         None));
    });
}
