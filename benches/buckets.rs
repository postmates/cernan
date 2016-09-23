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
fn bench_counters(b: &mut Bencher) {
    let dt_0 = UTC.ymd(1972, 12, 11).and_hms_milli(11, 59, 49, 0).timestamp();
    let dt_1 = UTC.ymd(1972, 12, 14).and_hms_milli(5, 40, 56, 0).timestamp();


    b.iter(|| {
        let mut bucket = buckets::Buckets::default();

        for name in &["a", "aa", "aaa", "aaaa", "aaaaa"] {
            for i in &[-1.0, 0.0, 1.0] {
                for r in &[-1.0, 0.0, 1.0] {
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
    });
}

#[bench]
fn bench_gauges(b: &mut Bencher) {
    let dt_0 = UTC.ymd(1972, 12, 11).and_hms_milli(11, 59, 49, 0).timestamp();
    let dt_1 = UTC.ymd(1972, 12, 14).and_hms_milli(5, 40, 56, 0).timestamp();

    b.iter(|| {
        let mut bucket = buckets::Buckets::default();

        for name in &["a", "aa", "aaa", "aaaa", "aaaaa"] {
            for i in &[-1.0, 0.0, 1.0] {
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
    });
}
