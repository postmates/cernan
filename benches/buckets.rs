#[macro_use]
extern crate criterion;

use criterion::Criterion;

extern crate cernan;
extern crate chrono;
extern crate rand;

use cernan::buckets;
use cernan::metric::{AggregationMethod, Telemetry};
use chrono::{TimeZone, Utc};
use rand::{Rng, SeedableRng, XorShiftRng};

fn experiment(input: &ExperimentInput) {
    let total_adds = input.total_adds;
    let name_pool_size = input.name_pool_size;
    let mut rng: XorShiftRng = SeedableRng::from_seed([1, 2, 3, 4]);
    let aggregations = [
        AggregationMethod::Histogram,
        AggregationMethod::Set,
        AggregationMethod::Sum,
        AggregationMethod::Summarize,
    ];
    let times = [
        Utc.ymd(1972, 12, 11)
            .and_hms_milli(11, 59, 49, 0)
            .timestamp(),
        Utc.ymd(1972, 12, 11)
            .and_hms_milli(11, 59, 50, 0)
            .timestamp(),
        Utc.ymd(1972, 12, 11)
            .and_hms_milli(11, 59, 51, 0)
            .timestamp(),
        Utc.ymd(1972, 12, 11)
            .and_hms_milli(11, 59, 52, 0)
            .timestamp(),
        Utc.ymd(1972, 12, 11)
            .and_hms_milli(11, 59, 52, 0)
            .timestamp(),
    ];
    let mut pool: Vec<String> = Vec::with_capacity(name_pool_size);
    for _ in 0..name_pool_size {
        pool.push(rng.gen_ascii_chars().take(10).collect());
    }
    let mut bucket = buckets::Buckets::default();

    for _ in 0..total_adds {
        bucket.add(
            Telemetry::new()
                .value(rng.gen::<f64>())
                .name(rng.choose(&pool).unwrap().clone())
                .kind(*rng.choose(&aggregations).unwrap())
                .harden()
                .unwrap()
                .timestamp(*rng.choose(&times).unwrap()),
        );
    }
}

#[derive(Debug)]
struct ExperimentInput {
    total_adds: usize,
    name_pool_size: usize,
}

impl ::std::fmt::Display for ExperimentInput {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        write!(f, "({}, {})", self.total_adds, self.name_pool_size)
    }
}

fn benchmark(c: &mut Criterion) {
    let mut inputs = Vec::with_capacity(32);
    for i in 6..8 {
        for j in 6..10 {
            inputs.push(ExperimentInput {
                total_adds: 2usize.pow(i),
                name_pool_size: 2usize.pow(j),
            });
        }
    }

    c.bench_function_over_inputs(
        "bucket_add",
        |b, input| {
            b.iter(|| experiment(input));
        },
        inputs,
    );
}

criterion_group!(benches, benchmark);
criterion_main!(benches);
