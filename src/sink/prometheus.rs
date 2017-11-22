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

use flate2::Compression;
use flate2::write::GzEncoder;
use hyper::header::{ContentEncoding, Encoding};
use hyper::server::{Handler, Listening, Request, Response, Server};
use metric;
use metric::{AggregationMethod, TagMap};
use protobuf::Message;
use protobuf::repeated::RepeatedField;
use protocols::prometheus::*;
use quantiles::histogram::Bound;
use sink::{Sink, Valve};
use std::collections::HashSet;
use std::f64;
use std::io;
use std::io::Write;
use std::str;
use std::sync;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

lazy_static! {
    /// Total reportable metrics
    pub static ref PROMETHEUS_AGGR_REPORTABLE: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    /// Total inside baseball perpetual metrics
    pub static ref PROMETHEUS_AGGR_PERPETUAL_LEN: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    /// Total inside baseball windowed metrics
    pub static ref PROMETHEUS_AGGR_WINDOWED_LEN: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    /// Total remaining metrics in aggr
    pub static ref PROMETHEUS_AGGR_REMAINING: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    /// Total writes to binary
    pub static ref PROMETHEUS_WRITE_BINARY: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    /// Total writes to text
    pub static ref PROMETHEUS_WRITE_TEXT: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    /// Total report errors
    pub static ref PROMETHEUS_REPORT_ERROR: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
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
    // `http_srv` is never used but we must keep it in this struct to avoid the
    // listening server being dropped
    http_srv: Listening,
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
}

impl Default for PrometheusConfig {
    fn default() -> Self {
        PrometheusConfig {
            host: "localhost".to_string(),
            port: 8086,
            config_path: None,
            capacity_in_seconds: 600, // ten minutes
        }
    }
}

struct SenderHandler {
    aggr: sync::Arc<Mutex<PrometheusAggr>>,
}

#[derive(Clone, Debug)]
enum Accumulator {
    Perpetual(metric::Telemetry),
    Windowed {
        cap: usize,
        samples: Vec<metric::Telemetry>,
    },
}

impl Accumulator {
    pub fn kind(&self) -> AggregationMethod {
        match *self {
            Accumulator::Perpetual(ref t) => t.kind(),
            Accumulator::Windowed { cap: _, samples: _ } => {
                AggregationMethod::Summarize
            }
        }
    }

    #[cfg(test)]
    pub fn total_samples(&self) -> usize {
        match *self {
            Accumulator::Perpetual(_) => 1,
            Accumulator::Windowed { cap: _, samples: ref s } => s.len(),
        }
    }

    pub fn insert(&mut self, telem: metric::Telemetry) {
        match *self {
            Accumulator::Perpetual(ref mut t) => *t += telem,
            Accumulator::Windowed {
                cap,
                ref mut samples,
            } => {
                assert_eq!(telem.kind(), AggregationMethod::Summarize);
                match samples
                    .binary_search_by(|probe| probe.timestamp.cmp(&telem.timestamp))
                {
                    Ok(idx) => {
                        samples[idx] += telem;
                    }
                    Err(idx) => {
                        samples.insert(idx, telem);
                        if samples.len() > cap {
                            samples.truncate(cap);
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

        match self.keys.binary_search_by(
            |probe| probe.partial_cmp(&telem.name_tag_hash()).unwrap(),
        ) {
            Ok(hsh_idx) => {
                let accum = self.values.index(hsh_idx).clone();
                match accum {
                    Accumulator::Perpetual(t) => Some(t),
                    Accumulator::Windowed { cap: _, samples } => {
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
    fn reportable(&mut self) -> Iter {
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
            match self.keys.binary_search_by(
                |probe| probe.partial_cmp(&telem.name_tag_hash()).unwrap(),
            ) {
                Ok(hsh_idx) => {
                    let mut prev = self.values.index_mut(hsh_idx);
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
                            samples.push(telem);
                            Accumulator::Windowed {
                                cap: self.capacity_in_seconds,
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
                    cap: _,
                    ref samples,
                } => {
                    self.idx += 1;
                    match samples.len() {
                        0 => unreachable!(),
                        1 => {
                            return Some(samples[0].clone());
                        }
                        _ => {
                            let mut start = samples[0].clone();
                            for t in &samples[1..] {
                                start += t.clone();
                            }
                            return Some(start);
                        }
                    }
                }
            }
        }
        None
    }
}

impl Handler for SenderHandler {
    fn handle(&self, req: Request, res: Response) {
        let mut aggr = self.aggr.lock().unwrap();
        // PROMETHEUS_AGGR_REPORTABLE is retained for backward compatability.
        PROMETHEUS_AGGR_REPORTABLE.store(aggr.count(), Ordering::Relaxed);
        PROMETHEUS_AGGR_REMAINING.store(aggr.count(), Ordering::Relaxed);
        // Typed hyper::mime is challenging to use. In particular, matching does
        // not seem to work like I expect and handling all other MIME cases in
        // the existing enum strikes me as a fool's errand, on account of there
        // may be an infinite number of MIMEs that'll come right on in. We'll
        // just be monsters and assume if you aren't asking for protobuf you're
        // asking for plaintext.
        let accept: Vec<&str> = req.headers
            .get_raw("accept")
            .unwrap_or(&[])
            .iter()
            .map(|x| str::from_utf8(x))
            .filter(|x| x.is_ok())
            .map(|x| x.unwrap())
            .collect();
        let mut accept_proto = false;
        for hdr in &accept {
            if hdr.contains("application/vnd.google.protobuf;") {
                accept_proto = true;
                break;
            }
        }
        let reportable = aggr.reportable();
        let res = if accept_proto {
            PROMETHEUS_WRITE_BINARY.fetch_add(1, Ordering::Relaxed);
            write_binary(reportable, res)
        } else {
            PROMETHEUS_WRITE_TEXT.fetch_add(1, Ordering::Relaxed);
            write_text(reportable, res)
        };
        if res.is_err() {
            PROMETHEUS_REPORT_ERROR.fetch_add(1, Ordering::Relaxed);
        }
    }
}

impl Prometheus {
    /// Create a new prometheus sink
    ///
    /// Please see documentation on `PrometheusConfig` for more details.
    pub fn new(config: PrometheusConfig) -> Prometheus {
        let aggrs = sync::Arc::new(sync::Mutex::new(
            PrometheusAggr::new(config.capacity_in_seconds),
        ));
        let srv_aggrs = sync::Arc::clone(&aggrs);
        let listener = Server::http((config.host.as_str(), config.port))
            .unwrap()
            .handle_threads(SenderHandler { aggr: srv_aggrs }, 1)
            .unwrap();

        Prometheus {
            aggrs: aggrs,
            http_srv: listener,
        }
    }
}

fn write_binary(aggrs: Iter, mut res: Response) -> io::Result<()> {
    res.headers_mut().set_raw(
        "content-type",
        vec![
            b"application/vnd.google.protobuf; \
                       proto=io.prometheus.client.MetricFamily; encoding=delimited"
                .to_vec(),
        ],
    );
    let mut res = res.start()?;
    for value in aggrs {
        let sanitized_name: String = sanitize(&value.name);
        match value.kind() {
            AggregationMethod::Sum => if let Some(v) = value.sum() {
                let mut metric_family = MetricFamily::new();
                metric_family.set_name(sanitized_name);
                let mut metric = Metric::new();
                let mut label_pairs = Vec::with_capacity(8);
                for &(ref k, ref v) in value.tags.iter() {
                    let mut lp = LabelPair::new();
                    lp.set_name(k.clone());
                    lp.set_value(v.clone());
                    label_pairs.push(lp);
                }
                metric.set_label(RepeatedField::from_vec(label_pairs));
                let mut counter = Counter::new();
                counter.set_value(v);
                metric.set_counter(counter);
                metric_family.set_field_type(MetricType::COUNTER);
                metric_family.set_metric(RepeatedField::from_vec(vec![metric]));
                metric_family.write_length_delimited_to_writer(res.by_ref())?
            },
            AggregationMethod::Set => if let Some(v) = value.set() {
                let mut metric_family = MetricFamily::new();
                metric_family.set_name(sanitized_name);
                let mut metric = Metric::new();
                let mut label_pairs = Vec::with_capacity(8);
                for &(ref k, ref v) in value.tags.iter() {
                    let mut lp = LabelPair::new();
                    lp.set_name(k.clone());
                    lp.set_value(v.clone());
                    label_pairs.push(lp);
                }
                metric.set_label(RepeatedField::from_vec(label_pairs));
                let mut gauge = Gauge::new();
                gauge.set_value(v);
                metric.set_gauge(gauge);
                metric_family.set_field_type(MetricType::GAUGE);
                metric_family.set_metric(RepeatedField::from_vec(vec![metric]));
                metric_family.write_length_delimited_to_writer(res.by_ref())?
            },
            AggregationMethod::Summarize => {
                let mut metric_family = MetricFamily::new();
                metric_family.set_name(sanitized_name);
                let mut metric = Metric::new();
                let mut label_pairs = Vec::with_capacity(8);
                for &(ref k, ref v) in value.tags.iter() {
                    let mut lp = LabelPair::new();
                    lp.set_name(k.clone());
                    lp.set_value(v.clone());
                    label_pairs.push(lp);
                }
                metric.set_label(RepeatedField::from_vec(label_pairs));
                let retained_count = value.count();
                let retained_sum = value.samples_sum().unwrap_or(0.0);
                let mut summary = Summary::new();
                summary.set_sample_count((retained_count + value.count()) as u64);
                if let Some(val) = value.samples_sum() {
                    summary.set_sample_sum(retained_sum + val);
                }
                let mut quantiles = Vec::with_capacity(9);
                for q in &[0.0, 1.0, 0.25, 0.5, 0.75, 0.90, 0.95, 0.99, 0.999] {
                    let mut quantile = Quantile::new();
                    quantile.set_quantile(*q);
                    quantile.set_value(value.query(*q).unwrap());
                    quantiles.push(quantile);
                }
                summary.set_quantile(RepeatedField::from_vec(quantiles));
                metric.set_summary(summary);
                metric_family.set_field_type(MetricType::SUMMARY);
                metric_family.set_metric(RepeatedField::from_vec(vec![metric]));
                metric_family.write_length_delimited_to_writer(res.by_ref())?
            }
            AggregationMethod::Histogram => {
                let mut metric_family = MetricFamily::new();
                metric_family.set_name(sanitized_name);
                let mut metric = Metric::new();
                let mut label_pairs = Vec::with_capacity(8);
                for &(ref k, ref v) in value.tags.iter() {
                    let mut lp = LabelPair::new();
                    lp.set_name(k.clone());
                    lp.set_value(v.clone());
                    label_pairs.push(lp);
                }
                metric.set_label(RepeatedField::from_vec(label_pairs));
                let mut histogram = Histogram::new();
                histogram.set_sample_count(value.count() as u64);
                if let Some(val) = value.samples_sum() {
                    histogram.set_sample_sum(val)
                }
                let mut buckets = Vec::with_capacity(16);
                if let Some(bin_iter) = value.bins() {
                    let mut cummulative: u64 = 0;
                    for &(bound, val) in bin_iter {
                        let bnd = match bound {
                            Bound::Finite(bnd) => bnd,
                            Bound::PosInf => f64::INFINITY,
                        };
                        cummulative += val as u64;
                        let mut bucket = Bucket::new();
                        bucket.set_cumulative_count(cummulative);
                        bucket.set_upper_bound(bnd);
                        buckets.push(bucket);
                    }
                }
                histogram.set_bucket(RepeatedField::from_vec(buckets));
                metric.set_histogram(histogram);
                metric_family.set_field_type(MetricType::HISTOGRAM);
                metric_family.set_metric(RepeatedField::from_vec(vec![metric]));
                metric_family.write_length_delimited_to_writer(res.by_ref())?
            }
        }
    }
    res.end()
}

#[allow(cyclomatic_complexity)]
#[inline]
fn fmt_tags(tags: &TagMap, s: &mut GzEncoder<Vec<u8>>) -> () {
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

fn write_text(aggrs: Iter, mut res: Response) -> io::Result<()> {
    {
        let headers = res.headers_mut();
        headers.set(ContentEncoding(vec![Encoding::Gzip]));
        headers.set_raw("content-type", vec![b"text/plain; version=0.0.4".to_vec()]);
    }
    let mut res = res.start()?;
    let mut seen: HashSet<String> = HashSet::new();
    let mut enc = GzEncoder::new(Vec::with_capacity(1024), Compression::Default);
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
    let encoded = enc.finish()?;
    res.write_all(&encoded)?;
    res.end()
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
        // intentionally blank
    }

    fn deliver(&mut self, mut point: sync::Arc<Option<metric::Telemetry>>) -> () {
        let mut aggrs = self.aggrs.lock().unwrap();
        let metric = sync::Arc::make_mut(&mut point).take().unwrap();
        aggrs.insert(metric);
    }

    fn deliver_line(&mut self, _: sync::Arc<Option<metric::LogLine>>) -> () {
        // nothing, intentionally
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
                match keys.binary_search_by(
                    |probe| probe.partial_cmp(&telem.name_tag_hash()).unwrap(),
                ) {
                    Ok(hsh_idx) => {
                        let mut prev = values.index_mut(hsh_idx);
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
                                samples.push(telem);
                                Accumulator::Windowed {
                                    cap: capacity_in_seconds,
                                    samples: samples,
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
            let mut windowed = Accumulator::Windowed { cap: cap, samples: Vec::new() };

            for t in telems.into_iter() {
                if t.kind() != AggregationMethod::Summarize {
                    continue;
                }
                windowed.insert(t);
            }

            assert!(windowed.total_samples() <= cap);

            TestResult::passed()
        }
        QuickCheck::new().quickcheck(inner as fn(usize, Vec<metric::Telemetry>) -> TestResult);
    }

    #[test]
    fn test_reportable() {
        fn inner(mut aggr: PrometheusAggr) -> TestResult {
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
