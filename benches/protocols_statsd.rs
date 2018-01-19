#[macro_use]
extern crate criterion;

use criterion::Criterion;

extern crate cernan;

use cernan::metric::Telemetry;
use cernan::protocols::statsd::parse_statsd;
use cernan::source::StatsdParseConfig;
use std::sync;

fn experiment() {
    let packet = "zrth:0|g
fst:-1.1|ms
snd:+2.2|g
thd:3.3|h
fth:4|c
fvth:5.5|c|@0.1
sxth:-6.6|g
svth:+7.77|g";

    let metric = sync::Arc::new(Some(Telemetry::default()));
    let config = sync::Arc::new(StatsdParseConfig::default());
    let mut res = Vec::new();
    assert!(parse_statsd(packet, &mut res, &metric, &config));
}

fn benchmark(c: &mut Criterion) {
    c.bench_function("parse_statsd", |b| {
        b.iter(|| experiment());
    });
}

criterion_group!(benches, benchmark);
criterion_main!(benches);
