#[macro_use]
extern crate criterion;

use criterion::Criterion;

extern crate cernan;

use cernan::metric::Telemetry;
use cernan::protocols::graphite::parse_graphite;
use std::sync;

fn experiment() {
    let packet = "fst 1 101
snd -2.0 202
thr 3 303
fth@fth 4 404
fv%fv 5 505
s-th 6 606";

    let mut res = Vec::new();
    let metric = sync::Arc::new(Some(Telemetry::default()));
    assert!(parse_graphite(packet, &mut res, &metric));
}

fn benchmark(c: &mut Criterion) {
    c.bench_function("parse_graphite", |b| {
        b.iter(|| experiment());
    });
}

criterion_group!(benches, benchmark);
criterion_main!(benches);
