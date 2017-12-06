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
use metric::{AggregationMethod, TagMap};
use quantiles::histogram::Bound;
use sink::{Sink, Valve};
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

lazy_static! {
    /// Total reportable metrics
    pub static ref PROMETHEUS_AGGR_REPORTABLE: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    /// Total inside baseball perpetual metrics
    pub static ref PROMETHEUS_AGGR_PERPETUAL_LEN: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    /// Total inside baseball windowed metrics
    pub static ref PROMETHEUS_AGGR_WINDOWED_LEN: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    /// Total remaining metrics in aggr
    pub static ref PROMETHEUS_AGGR_REMAINING: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    /// Total report successes
    pub static ref PROMETHEUS_REPORT_SUCCESS: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    /// Total report errors
    pub static ref PROMETHEUS_REPORT_ERROR: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    /// Sum of delays in reporting (microseconds)
    pub static ref PROMETHEUS_RESPONSE_DELAY_SUM: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
}

/// The prometheus sink
///
/// Prometheus is an open-source aggregation server which pulls from its
/// configured sources. Cernan overlaps some with Prometheus but is focused on
/// push-based metrics. This sink allows for bridging push-based systems with
/// pull-based.
#[allow(dead_code)]
pub struct Prometheus {
    thrd_aggr: sync::Arc<sync::Mutex<Option<PrometheusAggr>>>,
    aggrs: PrometheusAggr,
    age_threshold: Option<u64>,
    http_srv: http::Server,
}

/// The configuration for Prometheus sink
#[derive(Debug, Deserialize)]
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
}

impl Default for PrometheusConfig {
    fn default() -> Self {
        PrometheusConfig {
            host: "localhost".to_string(),
            port: 8086,
            config_path: None,
            capacity_in_seconds: 600, // ten minutes
            age_threshold: None,
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
    keys: Vec<u64>,
    values: Vec<Accumulator>,
    // Summarize metrics are kept in a time-based sliding window. The
    // capacity_in_seconds determines how wide this window is.
    capacity_in_seconds: usize,
}

impl PrometheusAggr {
    /// Return a reference to the stored Telemetry if it matches the passed
    /// Telemetry
    #[cfg(test)]
    fn find_match(&self, telem: &metric::Telemetry) -> Option<metric::Telemetry> {
        use std::ops::Index;

        match self.keys.binary_search_by(|probe| {
            probe.partial_cmp(&telem.name_tag_hash()).unwrap()
        }) {
            Ok(hsh_idx) => {
                let accum = self.values.index(hsh_idx).clone();
                match accum {
                    Accumulator::Perpetual(t) => Some(t),
                    Accumulator::Windowed {
                        cap: _,
                        samples,
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
            }
            Err(_) => None,
        }
    }

    /// Return all 'reportable' Telemetry
    ///
    /// This function returns all the stored Telemetry points that are available
    /// for shipping to Prometheus.
    fn reportable(&self) -> Iter {
        Iter {
            samples: &self.values,
            idx: 0,
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
        use std::ops::IndexMut;
        {
            match self.keys.binary_search_by(|probe| {
                probe.partial_cmp(&telem.name_tag_hash()).unwrap()
            }) {
                Ok(hsh_idx) => {
                    let prev = self.values.index_mut(hsh_idx);
                    if prev.kind() == telem.kind() {
                        prev.insert(telem);
                    } else {
                        return false;
                    }
                }
                Err(hsh_idx) => {
                    self.keys.insert(hsh_idx, telem.name_tag_hash());
                    let accum = match telem.kind() {
                        metric::AggregationMethod::Set
                        | metric::AggregationMethod::Sum
                        | metric::AggregationMethod::Histogram => {
                            Accumulator::Perpetual(telem)
                        }
                        metric::AggregationMethod::Summarize => {
                            let mut samples =
                                Vec::with_capacity(self.capacity_in_seconds);
                            let sum = telem.query(1.0).unwrap();
                            samples.push(telem);
                            Accumulator::Windowed {
                                cap: self.capacity_in_seconds,
                                count: 1,
                                sum: sum,
                                samples: samples,
                            }
                        }
                    };
                    self.values.insert(hsh_idx, accum);
                }
            }
        }
        assert_eq!(self.keys.len(), self.values.len());
        PROMETHEUS_AGGR_PERPETUAL_LEN.store(self.values.len(), Ordering::Relaxed);
        true
    }

    /// Return the total points stored by this aggregation
    fn count(&self) -> usize {
        self.values.len()
    }

    /// Create a new PrometheusAggr
    fn new(capacity_in_seconds: usize) -> PrometheusAggr {
        PrometheusAggr {
            keys: Vec::with_capacity(128),
            values: Vec::with_capacity(128),
            capacity_in_seconds: capacity_in_seconds,
        }
    }
}

/// Iteration struct for the bucket. Created by `Buckets.iter()`.
pub struct Iter<'a> {
    samples: &'a Vec<Accumulator>,
    idx: usize,
}

impl<'a> Iterator for Iter<'a> {
    type Item = metric::Telemetry;

    fn next(&mut self) -> Option<metric::Telemetry> {
        while self.idx < self.samples.len() {
            match self.samples[self.idx] {
                Accumulator::Perpetual(ref t) => {
                    self.idx += 1;
                    return Some(t.clone());
                }
                Accumulator::Windowed {
                    ref samples,
                    count,
                    sum,
                    ..
                } => {
                    self.idx += 1;
                    match samples.len() {
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
                                start
                                    .thaw()
                                    .sample_sum(sum)
                                    .count(count)
                                    .harden()
                                    .unwrap(),
                            );
                        }
                    }
                }
            }
        }
        None
    }
}

struct PrometheusHandler {
    aggr: sync::Arc<Mutex<Option<PrometheusAggr>>>,
}

impl http::Handler for PrometheusHandler {
    fn handle(&self, request: http::Request) -> () {
        match *self.aggr.lock().unwrap() {
            None => {}
            Some(ref aggr) => {
                PROMETHEUS_AGGR_REPORTABLE.store(aggr.count(), Ordering::Relaxed);
                PROMETHEUS_AGGR_REMAINING.store(aggr.count(), Ordering::Relaxed);
                let reportable = aggr.reportable();
                let now = Instant::now();
                let mut buffer = Vec::new();
                let buffer = write_text(reportable, buffer).unwrap();
                let elapsed = now.elapsed();
                let us = ((elapsed.as_secs() as f64) * 10_000.0)
                    + (f64::from(elapsed.subsec_nanos()) / 100_000.0);
                PROMETHEUS_RESPONSE_DELAY_SUM
                    .fetch_add(us as usize, Ordering::Relaxed);
                let content_encoding = "gzip";
                let content_type = "text/plain; version=0.0.4";
                let headers = [
                    http::Header::from_bytes(&b"Content-Type"[..], content_type)
                        .unwrap(),
                    http::Header::from_bytes(
                        &b"Content-Encoding"[..],
                        content_encoding,
                    ).unwrap(),
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
}

impl Prometheus {
    /// Create a new prometheus sink
    ///
    /// Please see documentation on `PrometheusConfig` for more details.
    pub fn new(config: &PrometheusConfig) -> Self {
        let aggrs = PrometheusAggr::new(config.capacity_in_seconds);
        let thrd_aggrs = sync::Arc::new(sync::Mutex::new(None));
        let srv_aggrs = sync::Arc::clone(&thrd_aggrs);

        let host_port =
            format!("{}:{}", config.host.as_str(), config.port.to_string());
        Prometheus {
            aggrs: aggrs,
            thrd_aggr: thrd_aggrs,
            age_threshold: config.age_threshold,
            http_srv: http::Server::new(
                host_port,
                PrometheusHandler { aggr: srv_aggrs },
            ),
        }
    }
}

#[allow(cyclomatic_complexity)]
#[inline]
fn fmt_tags<W>(tags: &TagMap, s: &mut GzEncoder<W>) -> ()
where
    W: Write,
{
    if tags.is_empty() {
        let _ = s.write(b"");
    } else {
        let mut iter = tags.iter();
        if let Some(&(ref fk, ref fv)) = iter.next() {
            let _ = s.write(fk.as_bytes());
            let _ = s.write(b"=\"");
            let _ = s.write(fv.as_bytes());
            let _ = s.write(b"\"");
            let mut empty = true;
            for &(ref k, ref v) in iter {
                empty = false;
                let _ = s.write(b", ");
                let _ = s.write(k.as_bytes());
                let _ = s.write(b"=\"");
                let _ = s.write(v.as_bytes());
                let _ = s.write(b"\"");
            }
            if empty {
                let _ = s.write(b"\"");
            }
        }
    }
}

fn write_text<W>(aggrs: Iter, buffer: W) -> io::Result<W>
where
    W: Write,
{
    use flate2::Compression;
    use std::collections::HashSet;

    let mut seen: HashSet<String> = HashSet::new();
    let mut enc = GzEncoder::new(buffer, Compression::Fast);
    for value in aggrs {
        let sanitized_name: String = sanitize(&value.name);
        match value.kind() {
            AggregationMethod::Sum => if let Some(v) = value.sum() {
                if seen.insert(value.name) {
                    enc.write_all(b"# TYPE ")?;
                    enc.write_all(sanitized_name.as_bytes())?;
                    enc.write_all(b" counter\n")?;
                }
                enc.write_all(sanitized_name.as_bytes())?;
                enc.write_all(b"{")?;
                fmt_tags(&value.tags, &mut enc);
                enc.write_all(b"} ")?;
                enc.write_all(v.to_string().as_bytes())?;
                enc.write_all(b"\n")?;
            },
            AggregationMethod::Set => if let Some(v) = value.set() {
                if seen.insert(value.name) {
                    enc.write_all(b"# TYPE ")?;
                    enc.write_all(sanitized_name.as_bytes())?;
                    enc.write_all(b" gauge\n")?;
                }
                enc.write_all(sanitized_name.as_bytes())?;
                enc.write_all(b"{")?;
                fmt_tags(&value.tags, &mut enc);
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
                    for (k, v) in &(*value.tags) {
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
                fmt_tags(&value.tags, &mut enc);
                enc.write_all(b"} ")?;
                enc.write_all(
                    value.samples_sum().unwrap_or(0.0).to_string().as_bytes(),
                )?;
                enc.write_all(b"\n")?;
                enc.write_all(sanitized_name.as_bytes())?;
                enc.write_all(b"_count ")?;
                enc.write_all(b"{")?;
                fmt_tags(&value.tags, &mut enc);
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
                    for (k, v) in &(*value.tags) {
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
                fmt_tags(&value.tags, &mut enc);
                enc.write_all(b"} ")?;
                enc.write_all(
                    value.samples_sum().unwrap_or(0.0).to_string().as_bytes(),
                )?;
                enc.write_all(b"\n")?;
                enc.write_all(sanitized_name.as_bytes())?;
                enc.write_all(b"_count ")?;
                enc.write_all(b"{")?;
                fmt_tags(&value.tags, &mut enc);
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

impl Sink for Prometheus {
    fn flush_interval(&self) -> Option<u64> {
        Some(1)
    }

    fn flush(&mut self) {
        let mut lock = self.thrd_aggr.try_lock();
        if let Ok(ref mut aggr) = lock {
            **aggr = Some(self.aggrs.clone());
        }
    }

    fn deliver(&mut self, mut point: sync::Arc<Option<metric::Telemetry>>) -> () {
        let telem: metric::Telemetry = sync::Arc::make_mut(&mut point).take().unwrap();
        if let Some(age_threshold) = self.age_threshold {
            if (telem.timestamp - time::now()).abs() <= (age_threshold as i64) {
                self.aggrs.insert(telem);
            }
        } else {
            self.aggrs.insert(telem);
        }
    }

    fn deliver_line(&mut self, _: sync::Arc<Option<metric::LogLine>>) -> () {
        // nothing, intentionally
    }

    fn shutdown(mut self) -> () {
        self.flush();
        self.http_srv.shutdown();
    }

    fn valve_state(&self) -> Valve {
        Valve::Open
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
            use std::ops::IndexMut;

            let limit: usize = Arbitrary::arbitrary(g);
            let capacity_in_seconds: usize = match Arbitrary::arbitrary(g) {
                0 => 1,
                i => i,
            };
            let mut keys: Vec<u64> = Vec::new();
            let mut values: Vec<Accumulator> = Vec::new();
            for _ in 0..limit {
                let telem: metric::Telemetry = Arbitrary::arbitrary(g);
                match keys.binary_search_by(|probe| {
                    probe.partial_cmp(&telem.name_tag_hash()).unwrap()
                }) {
                    Ok(hsh_idx) => {
                        let prev = values.index_mut(hsh_idx);
                        if prev.kind() == telem.kind() {
                            prev.insert(telem);
                        } else {
                            continue;
                        }
                    }
                    Err(hsh_idx) => {
                        keys.insert(hsh_idx, telem.name_tag_hash());
                        let accum = match telem.kind() {
                            metric::AggregationMethod::Set
                            | metric::AggregationMethod::Sum
                            | metric::AggregationMethod::Histogram => {
                                Accumulator::Perpetual(telem)
                            }
                            metric::AggregationMethod::Summarize => {
                                let mut samples =
                                    Vec::with_capacity(capacity_in_seconds);
                                let val = telem.query(1.0).unwrap();
                                samples.push(telem);
                                Accumulator::Windowed {
                                    cap: capacity_in_seconds,
                                    samples: samples,
                                    count: 1,
                                    sum: val,
                                }
                            }
                        };
                        values.insert(hsh_idx, accum);
                    }
                }
            }
            PrometheusAggr {
                keys: keys,
                values: values,
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
