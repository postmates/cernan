#![feature(test)]

extern crate test;
extern crate cernan;
extern crate chrono;
extern crate rand;

use self::test::Bencher;

use chrono::{UTC,TimeZone};
use cernan::buckets;
use cernan::metric::Metric;
use rand::Rng;

#[bench]
fn bench_single_timer(b: &mut Bencher) {
    let dt_0 = UTC.ymd(1972, 12, 11).and_hms_milli(11, 59, 49, 0).timestamp();

    b.iter(|| {
        let mut bucket = buckets::Buckets::default();

        bucket.add(Metric::new("a", 1.0).time(dt_0).timer());
    });
}

#[bench]
fn bench_single_timer_100(b: &mut Bencher) {
    let dt_0 = UTC.ymd(1972, 12, 11).and_hms_milli(11, 59, 49, 0).timestamp();

    b.iter(|| {
        let mut bucket = buckets::Buckets::default();

        for _ in 0..100 {
            bucket.add(Metric::new("a", 1.0).time(dt_0).timer());
        }
    });
}

#[bench]
fn bench_single_timer_rand_100(b: &mut Bencher) {
    let dt_0 = UTC.ymd(1972, 12, 11).and_hms_milli(11, 59, 49, 0).timestamp();

    b.iter(|| {
        let mut bucket = buckets::Buckets::default();
        let mut rng = rand::thread_rng();

        for _ in 0..100 {
            let i: usize = rng.gen_range(0, 100);
            bucket.add(Metric::new("a", i as f64).time(dt_0).timer());
        }
    });
}

#[bench]
fn bench_single_timer_1000(b: &mut Bencher) {
    let dt_0 = UTC.ymd(1972, 12, 11).and_hms_milli(11, 59, 49, 0).timestamp();

    b.iter(|| {
        let mut bucket = buckets::Buckets::default();

        for _ in 0..1000 {
            bucket.add(Metric::new("a", 1.0).time(dt_0).timer());
        }
    });
}

#[bench]
fn bench_single_timer_rand_1000(b: &mut Bencher) {
    let dt_0 = UTC.ymd(1972, 12, 11).and_hms_milli(11, 59, 49, 0).timestamp();

    b.iter(|| {
        let mut bucket = buckets::Buckets::default();
        let mut rng = rand::thread_rng();

        for _ in 0..1000 {
            let i: usize = rng.gen_range(0, 1000);
            bucket.add(Metric::new("a", i as f64).time(dt_0).timer());
        }
    });
}

#[bench]
fn bench_single_timer_10000(b: &mut Bencher) {
    let dt_0 = UTC.ymd(1972, 12, 11).and_hms_milli(11, 59, 49, 0).timestamp();

    b.iter(|| {
        let mut bucket = buckets::Buckets::default();

        for _ in 0..10000 {
            bucket.add(Metric::new("a", 1.0).time(dt_0).timer());
        }
    });
}

#[bench]
fn bench_single_timer_rand_10000(b: &mut Bencher) {
    let dt_0 = UTC.ymd(1972, 12, 11).and_hms_milli(11, 59, 49, 0).timestamp();

    b.iter(|| {
        let mut bucket = buckets::Buckets::default();
        let mut rng = rand::thread_rng();

        for _ in 0..10000 {
            let i: usize = rng.gen_range(0, 10000);
            bucket.add(Metric::new("a", i as f64).time(dt_0).timer());
        }
    });
}

#[bench]
fn bench_single_histogram(b: &mut Bencher) {
    let dt_0 = UTC.ymd(1972, 12, 11).and_hms_milli(11, 59, 49, 0).timestamp();

    b.iter(|| {
        let mut bucket = buckets::Buckets::default();

        bucket.add(Metric::new("a", 1.0).time(dt_0).histogram());
    });
}

#[bench]
fn bench_single_histogram_100(b: &mut Bencher) {
    let dt_0 = UTC.ymd(1972, 12, 11).and_hms_milli(11, 59, 49, 0).timestamp();

    b.iter(|| {
        let mut bucket = buckets::Buckets::default();

        for _ in 0..100 {
            bucket.add(Metric::new("a", 1.0).time(dt_0).histogram());
        }
    });
}

#[bench]
fn bench_single_histogram_rand_100(b: &mut Bencher) {
    let dt_0 = UTC.ymd(1972, 12, 11).and_hms_milli(11, 59, 49, 0).timestamp();

    b.iter(|| {
        let mut bucket = buckets::Buckets::default();
        let mut rng = rand::thread_rng();

        for _ in 0..100 {
            let i: usize = rng.gen_range(0, 100);
            bucket.add(Metric::new("a", i as f64).time(dt_0).histogram());
        }
    });
}

#[bench]
fn bench_single_histogram_1000(b: &mut Bencher) {
    let dt_0 = UTC.ymd(1972, 12, 11).and_hms_milli(11, 59, 49, 0).timestamp();

    b.iter(|| {
        let mut bucket = buckets::Buckets::default();

        for _ in 0..1_000 {
            bucket.add(Metric::new("a", 1.0).time(dt_0).histogram());
        }
    });
}

#[bench]
fn bench_single_histogram_rand_1000(b: &mut Bencher) {
    let dt_0 = UTC.ymd(1972, 12, 11).and_hms_milli(11, 59, 49, 0).timestamp();

    b.iter(|| {
        let mut bucket = buckets::Buckets::default();
        let mut rng = rand::thread_rng();

        for _ in 0..1_000 {
            let i: usize = rng.gen_range(0, 1_000);
            bucket.add(Metric::new("a", i as f64).time(dt_0).histogram());
        }
    });
}

#[bench]
fn bench_single_histogram_10000(b: &mut Bencher) {
    let dt_0 = UTC.ymd(1972, 12, 11).and_hms_milli(11, 59, 49, 0).timestamp();

    b.iter(|| {
        let mut bucket = buckets::Buckets::default();

        for _ in 0..10_000 {
            bucket.add(Metric::new("a", 1.0).time(dt_0).histogram());
        }
    });
}

#[bench]
fn bench_single_histogram_rand_10000(b: &mut Bencher) {
    let dt_0 = UTC.ymd(1972, 12, 11).and_hms_milli(11, 59, 49, 0).timestamp();

    b.iter(|| {
        let mut bucket = buckets::Buckets::default();
        let mut rng = rand::thread_rng();

        for _ in 0..10_000 {
            let i: usize = rng.gen_range(0, 10_000);
            bucket.add(Metric::new("a", i as f64).time(dt_0).histogram());
        }
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
                bucket.add(Metric::new(*name, *i).time(dt_0).counter());
                bucket.add(Metric::new(*name, *i).time(dt_1).counter());
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

        bucket.add(Metric::new("a", 1.0).time(dt_0).counter());
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
                bucket.add(Metric::new(*name, *i).time(dt_0).gauge());
                bucket.add(Metric::new(*name, *i).time(dt_1).gauge());
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

        bucket.add(Metric::new("a", 1.0).time(dt_0).gauge());
    });
}
