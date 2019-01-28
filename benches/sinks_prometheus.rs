#[macro_use]
extern crate criterion;

use criterion::Criterion;

extern crate cernan;

use cernan::metric::TagMap;
use cernan::sink::prometheus::{PrometheusAggr, write_text};
use flate2::write::GzEncoder;
use flate2::Compression;
use cernan::metric::Telemetry;
use cernan::metric::AggregationMethod;

struct TestData {
    aggr: PrometheusAggr,
    tags: TagMap,
}

fn setup() -> TestData {
    let mut aggr = PrometheusAggr::new(1);
    aggr.insert(Telemetry::new()
        .name("test.timer")
        .value(12.101)
        .kind(AggregationMethod::Summarize)
        .harden()
        .unwrap());
    let mut tags = TagMap::default();
    tags.insert("source".into(), "test-source".into());
    TestData{ aggr, tags }
}

fn experiment() {
    let td = setup();
    let mut buffer = Vec::with_capacity(2048);
    write_text(td.aggr.reportable(), &td.tags, &mut buffer).unwrap();
}

fn experiment_gzip() {
    let td = setup();
    let mut buffer = Vec::with_capacity(2048);
    let mut enc = GzEncoder::new(buffer, Compression::fast());
    enc = write_text(td.aggr.reportable(), &td.tags, enc).unwrap();
    enc.finish().unwrap();
}

fn benchmark(c: &mut Criterion) {
    c.bench_function("sinks_prometheus_write_text", |b| {
        b.iter(experiment);
    });
}

fn benchmark_gzip(c: &mut Criterion) {
    c.bench_function("sinks_prometheus_write_text_gzip", |b| {
        b.iter(experiment_gzip);
    });
}

criterion_group!(benches, benchmark, benchmark_gzip);
criterion_main!(benches);

