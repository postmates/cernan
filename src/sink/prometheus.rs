//! Prometheus is a pull-based aggregation server
//!
//! This sink emits all cernan aggregations into prometheus according to the
//! following method:
//!
//!   - SET -> gauge
//!   - SUM -> counter
//!   - QUANTILES -> summary
//!   - HISTOGRAM -> histogram
//!
//! All points are retained indefinitely in their aggregation.

extern crate log;

use flate2::write::GzEncoder;
use http;
use metric;
use metric::{AggregationMethod, TagIter, TagMap};
use quantiles::histogram::Bound;
use sink::Sink;
use std::collections::hash_map;
use std::f64;
use std::io;
use std::io::Write;
use std::str;
use std::sync;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;
use thread::Stoppable;
use time;
use util;

lazy_static! {
    /// Total reportable metrics
    pub static ref PROMETHEUS_AGGR_REPORTABLE: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    /// Total remaining metrics in aggr
    pub static ref PROMETHEUS_AGGR_REMAINING: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    /// Total report successes
    pub static ref PROMETHEUS_REPORT_SUCCESS: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    /// Total report errors
    pub static ref PROMETHEUS_REPORT_ERROR: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    /// Sum of delays in reporting (microseconds)
    pub static ref PROMETHEUS_RESPONSE_DELAY_SUM: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    /// Total stale perpetual elements purged
    pub static ref PROMETHEUS_AGGR_WINDOWED_PURGED: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
}

/// The prometheus sink
///
/// Prometheus is an open-source aggregation server which pulls from its
/// configured sources. Cernan overlaps some with Prometheus but is focused on
/// push-based metrics. This sink allows for bridging push-based systems with
/// pull-based.
#[allow(dead_code)]
pub struct Prometheus {
    aggrs: sync::Arc<sync::Mutex<PrometheusAggr>>,
    age_threshold: Option<u64>,
    http_srv: http::Server,
    tags: TagMap,
}

/// The configuration for Prometheus sink
#[derive(Clone, Debug, Deserialize)]
pub struct PrometheusConfig {
    /// The host to listen for prometheus. This will be used to bind an HTTP
    /// server to the given host. Host may be an IP address or a DNS hostname.
    pub host: String,
    /// The port to bind the listening host to.
    pub port: u16,
    /// The unique name of the sink in the routing topology.
    pub config_path: Option<String>,
    /// The maximum size of the sample window for Summarize, in seconds.
    pub capacity_in_seconds: usize,
    /// Determine the age at which a Telemetry point will be ejected. If the
    /// value is None no points will ever be rejected. Units are seconds.
    pub age_threshold: Option<u64>,
    /// The tags to be applied to all `metric::Event`s streaming through this
    /// sink. These tags will overwrite any tags carried by the `metric::Event`
    /// itself.
    pub tags: TagMap,
}

impl Default for PrometheusConfig {
    fn default() -> Self {
        PrometheusConfig {
            host: "localhost".to_string(),
            port: 8086,
            config_path: None,
            capacity_in_seconds: 600, // ten minutes
            age_threshold: None,
            tags: TagMap::default(),
        }
    }
}

#[derive(Clone, Debug)]
enum Accumulator {
    Perpetual(metric::Telemetry),
    Windowed {
        cap: usize,
        sum: f64,
        count: u64,
        samples: Vec<metric::Telemetry>,
    },
}

impl Accumulator {
    pub fn kind(&self) -> AggregationMethod {
        match *self {
            Accumulator::Perpetual(ref t) => t.kind(),
            Accumulator::Windowed { .. } => AggregationMethod::Summarize,
        }
    }

    #[cfg(test)]
    pub fn total_stored_samples(&self) -> usize {
        match *self {
            Accumulator::Perpetual(_) => 1,
            Accumulator::Windowed {
                cap: _,
                ref samples,
                sum: _,
                count: _,
            } => samples.len(),
        }
    }

    pub fn purge(&mut self) -> usize {
        match *self {
            Accumulator::Perpetual(_) => 0,
            Accumulator::Windowed {
                cap,
                ref mut samples,
                sum: _,
                count: _,
            } => {
                let mut purged = 0;
                let mut i = 0;
                while i != samples.len() {
                    if (samples[i].timestamp - time::now()).abs() > (cap as i64) {
                        samples.remove(i);
                        purged += 1;
                    } else {
                        i += 1;
                    }
                }
                purged
            }
        }
    }

    pub fn insert(&mut self, telem: metric::Telemetry) {
        match *self {
            Accumulator::Perpetual(ref mut t) => *t += telem,
            Accumulator::Windowed {
                cap,
                ref mut sum,
                ref mut count,
                ref mut samples,
            } => {
                use std::u64;
                assert_eq!(telem.kind(), AggregationMethod::Summarize);
                // u64::wrapping_add makes a new u64. We need this to be
                // in-place. Oops!
                if (u64::MAX - *count) <= 1 {
                    *count = 1;
                } else {
                    *count += 1;
                }
                let val = telem.query(1.0).unwrap();
                // There's no wrapping_add for f64. Since it's rude to crash
                // cernan because we've been summing for too long we knock
                // together our own wrap.
                if (f64::MAX - val) <= *sum {
                    *sum = val - (f64::MAX - *sum);
                } else {
                    *sum += val;
                }
                match samples
                    .binary_search_by(|probe| probe.timestamp.cmp(&telem.timestamp))
                {
                    Ok(idx) => {
                        samples[idx] += telem;
                    }
                    Err(idx) => {
                        samples.insert(idx, telem);
                        if samples.len() > cap {
                            let top_idx = samples.len() - cap;
                            samples.drain(0..top_idx);
                        }
                    }
                }
                assert!(samples.len() <= cap);
            }
        }
    }
}

/// The specialized aggr for Prometheus
///
/// Prometheus is a weirdo. We have to make sure the following properties are
/// held:
///
///   * If prometheus hangs up on us we _do not_ lose data.
///   * We _never_ resubmit points for a time-series.
///   * We _never_ submit new points for an already reported bin.
///
/// To help demonstrate this we have a special aggregation for Prometheus:
/// `PrometheusAggr`. It's job is to encode these operations and allow us to
/// establish the above invariants.
#[derive(Clone, Debug)]
struct PrometheusAggr {
    // The approach we take is unique in cernan: we drop all timestamps and _do
    // not bin_. This is driven by the following comment in Prometheus' doc /
    // our own experience trying to set explict timestamps:
    //
    //     Accordingly you should not set timestamps on the metric you expose,
    //     let Prometheus take care of that. If you think you need timestamps,
    //     then you probably need the pushgateway (without timestamps) instead.
    //
    // The following AggregationMethods we keep forever and ever:
    //
    //     - SET
    //     - SUM
    //     - HISTOGRAM
    //
    // The idea being that there's no good reason to flush these things,
    // according to this conversation:
    // https://github.com/postmates/cernan/pull/306#discussion_r139770087
    data: util::HashMap<String, Accumulator>,
    // Summarize metrics are kept in a time-based sliding window. The
    // capacity_in_seconds determines how wide this window is.
    capacity_in_seconds: usize,
}

impl PrometheusAggr {
    /// Return a reference to the stored Telemetry if it matches the passed
    /// Telemetry
    #[cfg(test)]
    fn find_match(&self, telem: &metric::Telemetry) -> Option<metric::Telemetry> {
        if let Some(accum) = self.data.get(&telem.name) {
            match *accum {
                Accumulator::Perpetual(ref t) => Some(t.clone()),
                Accumulator::Windowed {
                    cap: _,
                    ref samples,
                    sum: _,
                    count: _,
                } => {
                    let mut start = samples[0].clone();
                    for t in &samples[1..] {
                        start += t.clone();
                    }
                    return Some(start);
                }
            }
        } else {
            None
        }
    }

    /// Return all 'reportable' Telemetry
    ///
    /// This function returns all the stored Telemetry points that are available
    /// for shipping to Prometheus.
    fn reportable(&self) -> Iter {
        Iter {
            samples: self.data.values(),
        }
    }

    /// Insert a Telemetry into the aggregation
    ///
    /// This function inserts the given Telemetry into the inner aggregation of
    /// PrometheusAggr. Timestamps are _not_ respected. Distinctions between
    /// Telemetry of the same name are only made if their tagmaps are distinct.
    ///
    /// We DO NOT allow aggregation kinds to change in Prometheus sink. To that
    /// end, this function returns False if a Telemetry is inserted that differs
    /// only in its aggregation method from a Telemetry stored previously.
    fn insert(&mut self, telem: metric::Telemetry) -> bool {
        use std::collections::hash_map::*;
        let entry: Entry<String, Accumulator> = self.data.entry(telem.name.clone());
        match entry {
            Entry::Occupied(mut oe) => {
                let prev = oe.get_mut();
                if prev.kind() == telem.kind() {
                    prev.insert(telem);
                } else {
                    return false;
                }
            }
            Entry::Vacant(ve) => {
                ve.insert(match telem.kind() {
                    metric::AggregationMethod::Set
                    | metric::AggregationMethod::Sum
                    | metric::AggregationMethod::Histogram => {
                        Accumulator::Perpetual(telem)
                    }
                    metric::AggregationMethod::Summarize => {
                        let mut samples = Vec::new();
                        let sum = telem.query(1.0).unwrap();
                        samples.push(telem);
                        Accumulator::Windowed {
                            cap: self.capacity_in_seconds,
                            count: 1,
                            sum: sum,
                            samples: samples,
                        }
                    }
                });
            }
        }
        true
    }

    fn purge(&mut self) -> usize {
        let mut total_purged = 0;
        for aggr in self.data.values_mut() {
            total_purged += aggr.purge();
        }
        total_purged
    }

    /// Return the total points stored by this aggregation
    fn count(&self) -> usize {
        self.data.len()
    }

    /// Create a new PrometheusAggr
    fn new(capacity_in_seconds: usize) -> PrometheusAggr {
        PrometheusAggr {
            data: Default::default(),
            capacity_in_seconds: capacity_in_seconds,
        }
    }
}

/// Iteration struct for the bucket. Created by `Buckets.iter()`.
pub struct Iter<'a> {
    samples: hash_map::Values<'a, String, Accumulator>,
}

impl<'a> Iterator for Iter<'a> {
    type Item = metric::Telemetry;

    fn next(&mut self) -> Option<metric::Telemetry> {
        match self.samples.next() {
            Some(&Accumulator::Perpetual(ref t)) => {
                return Some(t.clone());
            }
            Some(&Accumulator::Windowed {
                ref samples,
                count,
                sum,
                ..
            }) => match samples.len() {
                0 => unreachable!(),
                1 => {
                    return Some(
                        samples[0]
                            .clone()
                            .thaw()
                            .sample_sum(sum)
                            .count(count)
                            .harden()
                            .unwrap(),
                    );
                }
                _ => {
                    let mut start = samples[0].clone();
                    for t in &samples[1..] {
                        start += t.clone();
                    }
                    return Some(
                        start.thaw().sample_sum(sum).count(count).harden().unwrap(),
                    );
                }
            },
            None => None,
        }
    }
}

struct PrometheusHandler {
    tags: TagMap,
    aggr: sync::Arc<Mutex<PrometheusAggr>>,
}

impl http::Handler for PrometheusHandler {
    fn handle(&self, request: http::Request) -> () {
        let mut buffer = Vec::with_capacity(2048);
        if let Ok(ref aggr) = self.aggr.try_lock() {
            PROMETHEUS_AGGR_REPORTABLE.store(aggr.count(), Ordering::Relaxed);
            let reportable = aggr.reportable();
            let now = Instant::now();
            buffer =
                write_text(reportable, &self.tags, Vec::with_capacity(2048)).unwrap();
            let elapsed = now.elapsed();
            let us = ((elapsed.as_secs() as f64) * 10_000.0)
                + (f64::from(elapsed.subsec_nanos()) / 100_000.0);
            PROMETHEUS_RESPONSE_DELAY_SUM.fetch_add(us as usize, Ordering::Relaxed);
        }
        if !buffer.is_empty() {
            let content_encoding = "gzip";
            let content_type = "text/plain; version=0.0.4";
            let headers = [
                http::Header::from_bytes(&b"Content-Type"[..], content_type).unwrap(),
                http::Header::from_bytes(&b"Content-Encoding"[..], content_encoding)
                    .unwrap(),
            ];

            let response = http::Response::new(
                http::StatusCode::from(200),
                headers.to_vec(),
                &buffer[..],
                Some(buffer.len()),
                None,
            );

            match request.respond(response) {
                Ok(_) => {
                    PROMETHEUS_REPORT_SUCCESS.fetch_add(1, Ordering::Relaxed);
                }
                Err(e) => {
                    PROMETHEUS_REPORT_ERROR.fetch_add(1, Ordering::Relaxed);
                    warn!("Failed to send prometheus response! {:?}", e);
                }
            };
        }
    }
}

#[allow(cyclomatic_complexity)]
#[inline]
fn fmt_tags<W>(mut iter: TagIter, s: &mut GzEncoder<W>) -> ()
where
    W: Write,
{
    // TODO this is wrong for a single tag
    //  afgIPQC{source="cernan""} 1422522
    if let Some((ref fk, ref fv)) = iter.next() {
        let _ = s.write(fk.as_bytes());
        let _ = s.write(b"=\"");
        let _ = s.write(fv.as_bytes());
        let _ = s.write(b"\"");
        for (ref k, ref v) in iter {
            let _ = s.write(b", ");
            let _ = s.write(k.as_bytes());
            let _ = s.write(b"=\"");
            let _ = s.write(v.as_bytes());
            let _ = s.write(b"\"");
        }
    } else {
        let _ = s.write(b"");
    }
}

fn write_text<W>(aggrs: Iter, default: &TagMap, buffer: W) -> io::Result<W>
where
    W: Write,
{
    use flate2::Compression;
    use std::collections::HashSet;

    let mut seen: HashSet<String> = HashSet::new();
    let mut enc = GzEncoder::new(buffer, Compression::fast());
    for value in aggrs {
        let sanitized_name: String = sanitize(&value.name);
        match value.kind() {
            AggregationMethod::Sum => if let Some(v) = value.sum() {
                if seen.insert(value.name.clone()) {
                    enc.write_all(b"# TYPE ")?;
                    enc.write_all(sanitized_name.as_bytes())?;
                    enc.write_all(b" counter\n")?;
                }
                enc.write_all(sanitized_name.as_bytes())?;
                enc.write_all(b"{")?;
                fmt_tags(value.tags(default), &mut enc);
                enc.write_all(b"} ")?;
                enc.write_all(v.to_string().as_bytes())?;
                enc.write_all(b"\n")?;
            },
            AggregationMethod::Set => if let Some(v) = value.set() {
                if seen.insert(value.name.clone()) {
                    enc.write_all(b"# TYPE ")?;
                    enc.write_all(sanitized_name.as_bytes())?;
                    enc.write_all(b" gauge\n")?;
                }
                enc.write_all(sanitized_name.as_bytes())?;
                enc.write_all(b"{")?;
                fmt_tags(value.tags(default), &mut enc);
                enc.write_all(b"} ")?;
                enc.write_all(v.to_string().as_bytes())?;
                enc.write_all(b"\n")?;
            },
            AggregationMethod::Histogram => if let Some(bin_iter) = value.bins() {
                if seen.insert(value.name.clone()) {
                    enc.write_all(b"# TYPE ")?;
                    enc.write_all(sanitized_name.as_bytes())?;
                    enc.write_all(b" histogram\n")?;
                }
                let mut running_sum = 0;
                for &(bound, val) in bin_iter {
                    enc.write_all(sanitized_name.as_bytes())?;
                    enc.write_all(b"{le=\"")?;
                    match bound {
                        Bound::Finite(bnd) => {
                            enc.write_all(bnd.to_string().as_bytes())?;
                        }
                        Bound::PosInf => {
                            enc.write_all(b"+Inf")?;
                        }
                    }
                    for (k, v) in value.tags(default) {
                        enc.write_all(b"\", ")?;
                        enc.write_all(k.as_bytes())?;
                        enc.write_all(b"=\"")?;
                        enc.write_all(v.as_bytes())?;
                    }
                    enc.write_all(b"\"} ")?;
                    enc.write_all((val + running_sum).to_string().as_bytes())?;
                    running_sum += val;
                    enc.write_all(b"\n")?;
                }
                enc.write_all(sanitized_name.as_bytes())?;
                enc.write_all(b"_sum ")?;
                enc.write_all(b"{")?;
                fmt_tags(value.tags(default), &mut enc);
                enc.write_all(b"} ")?;
                enc.write_all(
                    value.samples_sum().unwrap_or(0.0).to_string().as_bytes(),
                )?;
                enc.write_all(b"\n")?;
                enc.write_all(sanitized_name.as_bytes())?;
                enc.write_all(b"_count ")?;
                enc.write_all(b"{")?;
                fmt_tags(value.tags(default), &mut enc);
                enc.write_all(b"} ")?;
                enc.write_all(value.count().to_string().as_bytes())?;
                enc.write_all(b"\n")?;
            },
            AggregationMethod::Summarize => {
                if seen.insert(value.name.clone()) {
                    enc.write_all(b"# TYPE ")?;
                    enc.write_all(sanitized_name.as_bytes())?;
                    enc.write_all(b" summary\n")?;
                }
                for q in &[0.0, 1.0, 0.25, 0.5, 0.75, 0.90, 0.95, 0.99, 0.999] {
                    enc.write_all(sanitized_name.as_bytes())?;
                    enc.write_all(b"{quantile=\"")?;
                    enc.write_all(q.to_string().as_bytes())?;
                    for (k, v) in value.tags(default) {
                        enc.write_all(b"\", ")?;
                        enc.write_all(k.as_bytes())?;
                        enc.write_all(b"=\"")?;
                        enc.write_all(v.as_bytes())?;
                    }
                    enc.write_all(b"\"} ")?;

                    enc.write_all(value.query(*q).unwrap().to_string().as_bytes())?;
                    enc.write_all(b"\n")?;
                }
                enc.write_all(sanitized_name.as_bytes())?;
                enc.write_all(b"_sum ")?;
                enc.write_all(b"{")?;
                fmt_tags(value.tags(default), &mut enc);
                enc.write_all(b"} ")?;
                enc.write_all(
                    value.samples_sum().unwrap_or(0.0).to_string().as_bytes(),
                )?;
                enc.write_all(b"\n")?;
                enc.write_all(sanitized_name.as_bytes())?;
                enc.write_all(b"_count ")?;
                enc.write_all(b"{")?;
                fmt_tags(value.tags(default), &mut enc);
                enc.write_all(b"} ")?;
                enc.write_all((value.count()).to_string().as_bytes())?;
                enc.write_all(b"\n")?;
            }
        }
    }
    enc.finish()
}

/// Sanitize cernan Telemetry into prometheus' notion
///
/// Prometheus is pretty strict about the names of its ingested metrics.
/// According to [Writing
/// Exporters](https://prometheus.io/docs/instrumenting/writing_exporters/)
/// "Only [a-zA-Z0-9:_] are valid in metric names, any other characters should
/// be sanitized to an underscore."
///
/// Metrics coming into cernan can have full utf8 names, save for some ingestion
/// protocols that special-case certain characters. To cope with this we just
/// mangle the mess out of names and hope for forgiveness in the hereafter.
fn sanitize(name: &str) -> String {
    let name: String = name.to_string();
    let mut new_name: Vec<u8> = Vec::with_capacity(128);
    for c in name.as_bytes() {
        match *c {
            b'a'...b'z' | b'A'...b'Z' | b'0'...b'9' | b':' | b'_' => new_name.push(*c),
            _ => new_name.push(b'_'),
        }
    }
    String::from_utf8(new_name).unwrap()
}

impl Sink<PrometheusConfig> for Prometheus {
    fn init(config: PrometheusConfig) -> Self {
        let aggrs = PrometheusAggr::new(config.capacity_in_seconds);
        let aggrs = sync::Arc::new(sync::Mutex::new(aggrs));

        let host_port =
            format!("{}:{}", config.host.as_str(), config.port.to_string());
        Prometheus {
            aggrs: sync::Arc::clone(&aggrs),
            age_threshold: config.age_threshold,
            http_srv: http::Server::new(
                host_port,
                PrometheusHandler {
                    tags: config.tags.clone(),
                    aggr: aggrs,
                },
            ),
            tags: config.tags,
        }
    }

    fn flush_interval(&self) -> Option<u64> {
        Some(10)
    }

    fn flush(&mut self) {
        let total_purged = self.aggrs.lock().unwrap().purge();
        PROMETHEUS_AGGR_WINDOWED_PURGED.store(total_purged, Ordering::Relaxed);
    }

    fn deliver(&mut self, telem: metric::Telemetry) -> () {
        if let Some(age_threshold) = self.age_threshold {
            if (telem.timestamp - time::now()).abs() <= (age_threshold as i64) {
                self.aggrs.lock().unwrap().insert(telem);
            }
        } else {
            self.aggrs.lock().unwrap().insert(telem);
        }
    }

    fn shutdown(mut self) -> () {
        self.flush();
        self.http_srv.shutdown();
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use metric;
    use quickcheck::{Arbitrary, Gen, QuickCheck, TestResult};

    impl Arbitrary for PrometheusAggr {
        fn arbitrary<G>(g: &mut G) -> Self
        where
            G: Gen,
        {
            use std::collections::hash_map::*;

            let limit: usize = Arbitrary::arbitrary(g);
            let capacity_in_seconds: usize = match Arbitrary::arbitrary(g) {
                0 => 1,
                i => i,
            };
            let mut data: util::HashMap<String, Accumulator> = Default::default();
            for _ in 0..limit {
                let telem: metric::Telemetry = Arbitrary::arbitrary(g);
                let entry: Entry<String, Accumulator> = data.entry(telem.name.clone());
                match entry {
                    Entry::Occupied(mut oe) => {
                        let prev = oe.get_mut();
                        if prev.kind() == telem.kind() {
                            prev.insert(telem);
                        } else {
                            continue;
                        }
                    }
                    Entry::Vacant(ve) => {
                        ve.insert(match telem.kind() {
                            metric::AggregationMethod::Set
                            | metric::AggregationMethod::Sum
                            | metric::AggregationMethod::Histogram => {
                                Accumulator::Perpetual(telem)
                            }
                            metric::AggregationMethod::Summarize => {
                                let mut samples = Vec::new();
                                let sum = telem.query(1.0).unwrap();
                                samples.push(telem);
                                Accumulator::Windowed {
                                    cap: capacity_in_seconds,
                                    count: 1,
                                    sum: sum,
                                    samples: samples,
                                }
                            }
                        });
                    }
                }
            }
            PrometheusAggr {
                data: data,
                capacity_in_seconds: capacity_in_seconds,
            }
        }
    }

    #[test]
    fn test_accumlator_window_boundary_obeyed() {
        fn inner(cap: usize, telems: Vec<metric::Telemetry>) -> TestResult {
            let mut windowed = Accumulator::Windowed {
                cap: cap,
                samples: Vec::new(),
                sum: 0.0,
                count: 0,
            };

            for t in telems.into_iter() {
                if t.kind() != AggregationMethod::Summarize {
                    continue;
                }
                windowed.insert(t);
            }

            assert!(windowed.total_stored_samples() <= cap);

            TestResult::passed()
        }
        QuickCheck::new()
            .quickcheck(inner as fn(usize, Vec<metric::Telemetry>) -> TestResult);
    }

    #[test]
    fn test_reportable() {
        fn inner(aggr: PrometheusAggr) -> TestResult {
            let cur_cnt = aggr.count();

            let mut reportable_cnt = 0;
            for _ in aggr.reportable() {
                reportable_cnt += 1;
            }

            assert_eq!(cur_cnt, reportable_cnt);

            TestResult::passed()
        }
        QuickCheck::new().quickcheck(inner as fn(PrometheusAggr) -> TestResult);
    }

    // insertion must obey two properties, based on existence or not
    //
    //  IF EXISTS
    //    - insertion should NOT increase total count
    //    - insertion WILL modify existing telemetry in aggregation
    //      - insertion WILL NOT change telemetry aggregation kind
    //
    //  IF NOT EXISTS
    //    - insertion WILL increase count by 1
    //    - insertion WILL make telemetry exist after the insertion
    #[test]
    fn test_insertion_exists_property() {
        fn inner(telem: metric::Telemetry, mut aggr: PrometheusAggr) -> TestResult {
            let cur_cnt = aggr.count();
            match aggr.find_match(&telem) {
                Some(other) => if aggr.insert(telem.clone()) {
                    assert_eq!(other.kind(), telem.kind());
                    assert_eq!(cur_cnt, aggr.count());
                    let new_t =
                        aggr.find_match(&telem).expect("could not find in test");
                    assert_eq!(other.name, new_t.name);
                    assert_eq!(new_t.kind(), telem.kind());
                } else {
                    assert!(other.kind() != telem.kind());
                    assert_eq!(cur_cnt, aggr.count());
                    return TestResult::discard();
                },
                None => return TestResult::discard(),
            }
            TestResult::passed()
        }
        QuickCheck::new()
            .quickcheck(inner as fn(metric::Telemetry, PrometheusAggr) -> TestResult);
    }

    #[test]
    fn test_insertion_not_exists_property() {
        fn inner(telem: metric::Telemetry, mut aggr: PrometheusAggr) -> TestResult {
            let cur_cnt = aggr.count();
            match aggr.find_match(&telem) {
                Some(_) => return TestResult::discard(),
                None => {
                    assert!(aggr.insert(telem.clone()));
                    assert_eq!(cur_cnt + 1, aggr.count());
                    aggr.find_match(&telem).expect("could not find");
                }
            }
            TestResult::passed()
        }
        QuickCheck::new()
            .quickcheck(inner as fn(metric::Telemetry, PrometheusAggr) -> TestResult);
    }

    #[test]
    fn test_sanitization() {
        fn inner(metric: metric::Telemetry) -> TestResult {
            let name: String = sanitize(&metric.name);
            for c in name.chars() {
                match c {
                    'a'...'z' | 'A'...'Z' | '0'...'9' | ':' | '_' => continue,
                    other => {
                        println!("OTHER: {}", other);
                        return TestResult::failed();
                    }
                }
            }
            TestResult::passed()
        }
        QuickCheck::new().quickcheck(inner as fn(metric::Telemetry) -> TestResult);
    }
}
