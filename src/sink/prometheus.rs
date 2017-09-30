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
//! All points are retained in a sliding window defined by
//! `retain_limit`. Points will persist indefinately unless new points come
//! behind them to push them out of the `retain_limit` window.

use hyper::server::{Handler, Listening, Request, Response, Server};
use metric;
use metric::AggregationMethod;
use protobuf::Message;
use protobuf::repeated::RepeatedField;
use protocols::prometheus::*;
use quantiles::histogram::Bound;
use seahash::SeaHasher;
use sink::{Sink, Valve};
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::hash_map::Entry;
use std::f64;
use std::hash::BuildHasherDefault;
use std::io;
use std::io::Write;
use std::mem;
use std::str;
use std::sync;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
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
    aggrs: sync::Arc<Mutex<PrometheusAggr>>,
    // `http_srv` is never used but we must keep it in this struct to avoid the
    // listening server being dropped
    http_srv: Listening,
}

/// The configuration for Prometheus sink
#[derive(Debug, Deserialize)]
pub struct PrometheusConfig {
    /// Configure how many seconds worth of Telemetry the Prometheus aggregation
    /// will retain. Values only expire as new values come in to push them out.
    pub retain_limit: usize,
    /// The host to listen for prometheus. This will be used to bind an HTTP
    /// server to the given host. Host may be an IP address or a DNS hostname.
    pub host: String,
    /// The port to bind the listening host to.
    pub port: u16,
    /// The unique name of the sink in the routing topology.
    pub config_path: Option<String>,
}

impl Default for PrometheusConfig {
    fn default() -> Self {
        PrometheusConfig {
            retain_limit: 10,
            host: "localhost".to_string(),
            port: 8086,
            config_path: None,
        }
    }
}

struct SenderHandler {
    aggr: sync::Arc<Mutex<PrometheusAggr>>,
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
    retain_limit: usize,
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
    perpetual: HashMap<u64, metric::Telemetry, BuildHasherDefault<SeaHasher>>,
    // Now, AggregationMethod::Summarize is different. We store up a moving
    // window of summaries and add them together on report. The window is
    // `retain_limit` seconds wide, with older entries being flushed during
    // flush intervals and insertion.
    oldest_timestamp: i64,
    retainers: HashMap<u64, WindowedRetainer, BuildHasherDefault<SeaHasher>>,
    windowed: HashMap<u64, Vec<metric::Telemetry>, BuildHasherDefault<SeaHasher>>,
}

/// When we store `AggregationMethod::Summarize` into windows we have to be
/// careful to kep the count and summation of past bins we've dropped
/// off. That's the purpose of `WindowedRetainer`.
#[derive(Clone, Debug)]
struct WindowedRetainer {
    historic_count: usize,
    historic_sum: f64,
}

impl Default for WindowedRetainer {
    fn default() -> WindowedRetainer {
        WindowedRetainer {
            historic_count: 0,
            historic_sum: 0.0,
        }
    }
}

impl PrometheusAggr {
    /// Return a reference to the stored Telemetry if it matches the passed
    /// Telemetry
    #[cfg(test)]
    fn find_match(&self, telem: &metric::Telemetry) -> Option<metric::Telemetry> {
        match telem.kind() {
            AggregationMethod::Set |
            AggregationMethod::Sum |
            AggregationMethod::Histogram => {
                self.perpetual.get(&telem.hash()).map(|x| x.clone())
            }
            AggregationMethod::Summarize => {
                if let Some(vs) = self.windowed.get(&telem.hash()) {
                    let mut iter = vs.iter();
                    if let Some(fst) = iter.next() {
                        let mut base = fst.clone();
                        for x in iter {
                            base += x.clone();
                        }
                        Some(base)
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
        }
    }

    /// Return all 'reportable' Telemetry
    ///
    /// This function returns all the stored Telemetry points that are available
    /// for shipping to Prometheus.
    fn reportable(&mut self) -> Vec<metric::Telemetry> {
        let mut ret = Vec::new();
        for v in self.perpetual.values() {
            ret.push(v.clone());
        }
        for v in self.windowed.values() {
            let mut iter = v.iter();
            if let Some(fst) = iter.next() {
                let mut base = fst.clone();
                for x in iter {
                    if x.timestamp > self.oldest_timestamp {
                        base += x.clone();
                    }
                }
                ret.push(base);
            }
        }
        ret
    }

    /// Insert a Telemetry into the aggregation
    ///
    /// This function inserts the given Telemetry into the inner aggregation of
    /// PrometheusAggr. Timestamps are _not_ respected. Distinctions between
    /// Telemetry of the same name are only made if their tagmaps are distinct.
    fn insert(&mut self, telem: metric::Telemetry) -> bool {
        match telem.kind() {
            AggregationMethod::Set |
            AggregationMethod::Sum |
            AggregationMethod::Histogram => {
                {
                    let entry = self.perpetual.entry(telem.hash());
                    match entry {
                        Entry::Occupied(mut o) => {
                            *o.get_mut() += telem;
                        }
                        Entry::Vacant(o) => {
                            o.insert(telem);
                        }
                    };
                }
                PROMETHEUS_AGGR_PERPETUAL_LEN
                    .store(self.perpetual.len(), Ordering::Relaxed);
            }
            AggregationMethod::Summarize => {
                {
                    let id = telem.hash();
                    let ts_vec = self.windowed.entry(id).or_insert_with(|| vec![]);
                    match (*ts_vec).binary_search_by(
                        |probe| probe.timestamp.cmp(&telem.timestamp),
                    ) {
                        Ok(idx) => ts_vec[idx] += telem,
                        Err(idx) => ts_vec.insert(idx, telem),
                    }
                    let mut overage = 0;
                    for val in ts_vec.iter() {
                        if val.timestamp < self.oldest_timestamp {
                            overage += 1;
                        } else {
                            break;
                        }
                    }
                    if overage > 0 {
                        for t in ts_vec.drain(0..overage) {
                            let retainer = self.retainers
                                .entry(id)
                                .or_insert_with(WindowedRetainer::default);
                            retainer.historic_sum += t.samples_sum().unwrap_or(0.0);
                            retainer.historic_count += t.count();
                        }
                    }
                }
                PROMETHEUS_AGGR_WINDOWED_LEN
                    .store(self.windowed.len(), Ordering::Relaxed);
            }
        }
        true
    }

    /// Recombine Telemetry into the aggregation
    ///
    /// In the event that Prometheus hangs up on us we have to recombine
    /// 'reportable' Telemetry back into the aggregation, else we lose it. This
    /// function _will_ reset the last_report for each time series.
    ///
    /// If a Telemetry is passed that did not previously exist or has not been
    /// reported the effect will be the same as if `insert` had been called.
    fn recombine(&mut self, telems: Vec<metric::Telemetry>) {
        for telem in telems {
            self.insert(telem);
        }
    }

    /// Return the total points stored by this aggregation
    fn count(&self) -> usize {
        self.windowed.len() + self.perpetual.len()
    }

    /// Create a new PrometheusAggr
    fn new(retain_limit: usize) -> PrometheusAggr {
        PrometheusAggr {
            retain_limit: retain_limit,
            perpetual: Default::default(),
            oldest_timestamp: time::now() - (retain_limit as i64),
            retainers: Default::default(),
            windowed: Default::default(),
        }
    }
}

impl Handler for SenderHandler {
    fn handle(&self, req: Request, res: Response) {
        let mut aggr = self.aggr.lock().unwrap();
        let reportable: Vec<metric::Telemetry> = aggr.reportable();
        PROMETHEUS_AGGR_REPORTABLE.store(reportable.len(), Ordering::Relaxed);
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
        let res = if accept_proto {
            PROMETHEUS_WRITE_BINARY.fetch_add(1, Ordering::Relaxed);
            write_binary(&reportable, &aggr.retainers, res)
        } else {
            PROMETHEUS_WRITE_TEXT.fetch_add(1, Ordering::Relaxed);
            write_text(&reportable, &aggr.retainers, res)
        };
        if res.is_err() {
            PROMETHEUS_REPORT_ERROR.fetch_add(1, Ordering::Relaxed);
            aggr.recombine(reportable);
        }
    }
}

impl Prometheus {
    /// Create a new prometheus sink
    ///
    /// Please see documentation on `PrometheusConfig` for more details.
    pub fn new(config: PrometheusConfig) -> Prometheus {
        let aggrs =
            sync::Arc::new(sync::Mutex::new(PrometheusAggr::new(config.retain_limit)));
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

fn write_binary(
    aggrs: &[metric::Telemetry],
    retainers: &HashMap<u64, WindowedRetainer, BuildHasherDefault<SeaHasher>>,
    mut res: Response,
) -> io::Result<()> {
    res.headers_mut().set_raw(
        "content-type",
        vec![
            b"application/vnd.google.protobuf; \
                       proto=io.prometheus.client.MetricFamily; encoding=delimited"
                .to_vec(),
        ],
    );
    let mut res = res.start().unwrap();
    for value in aggrs {
        match value.kind() {
            AggregationMethod::Sum => if let Some(v) = value.sum() {
                let mut metric_family = MetricFamily::new();
                metric_family.set_name(value.name.clone());
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
                metric_family
                    .write_length_delimited_to_writer(res.by_ref())
                    .expect("FAILED TO WRITE TO HTTP RESPONSE");
            },
            AggregationMethod::Set => if let Some(v) = value.set() {
                let mut metric_family = MetricFamily::new();
                metric_family.set_name(value.name.clone());
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
                metric_family
                    .write_length_delimited_to_writer(res.by_ref())
                    .expect("FAILED TO WRITE TO HTTP RESPONSE");
            },
            AggregationMethod::Summarize => {
                let mut metric_family = MetricFamily::new();
                metric_family.set_name(value.name.clone());
                let mut metric = Metric::new();
                let mut label_pairs = Vec::with_capacity(8);
                for &(ref k, ref v) in value.tags.iter() {
                    let mut lp = LabelPair::new();
                    lp.set_name(k.clone());
                    lp.set_value(v.clone());
                    label_pairs.push(lp);
                }
                metric.set_label(RepeatedField::from_vec(label_pairs));
                let (retained_count, retained_sum) =
                    if let Some(retainer) = retainers.get(&value.hash()) {
                        (retainer.historic_count, retainer.historic_sum)
                    } else {
                        (0, 0.0)
                    };
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
                metric_family
                    .write_length_delimited_to_writer(res.by_ref())
                    .expect("FAILED TO WRITE TO HTTP RESPONSE");
            }
            AggregationMethod::Histogram => {
                let mut metric_family = MetricFamily::new();
                metric_family.set_name(value.name.clone());
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
                metric_family
                    .write_length_delimited_to_writer(res.by_ref())
                    .expect("FAILED TO WRITE TO HTTP RESPONSE");
            }
        }
    }
    res.end()
}

#[allow(cyclomatic_complexity)]
fn write_text(
    aggrs: &[metric::Telemetry],
    retainers: &HashMap<u64, WindowedRetainer, BuildHasherDefault<SeaHasher>>,
    mut res: Response,
) -> io::Result<()> {
    res.headers_mut()
        .set_raw("content-type", vec![b"text/plain; version=0.0.4".to_vec()]);
    let mut buf = String::with_capacity(1024);
    let mut res = res.start().unwrap();
    let mut seen = HashSet::new();
    for value in aggrs {
        match value.kind() {
            AggregationMethod::Sum => if let Some(v) = value.sum() {
                if seen.insert(&value.name) {
                    buf.push_str("# TYPE ");
                    buf.push_str(&value.name);
                    buf.push_str(" counter\n");
                }
                buf.push_str(&value.name);
                if !value.tags.is_empty() {
                    buf.push_str("{");
                    for (k, v) in &(*value.tags) {
                        buf.push_str("\", ");
                        buf.push_str(k);
                        buf.push_str("=\"");
                        buf.push_str(v);
                    }
                    buf.push_str("\"} ");
                } else {
                    buf.push_str(" ");
                }
                buf.push_str(&v.to_string());
                buf.push_str("\n");
            },
            AggregationMethod::Set => if let Some(v) = value.set() {
                if seen.insert(&value.name) {
                    buf.push_str("# TYPE ");
                    buf.push_str(&value.name);
                    buf.push_str(" gauge\n");
                }
                buf.push_str(&value.name);
                if !value.tags.is_empty() {
                    buf.push_str("{");
                    for (k, v) in &(*value.tags) {
                        buf.push_str("\", ");
                        buf.push_str(k);
                        buf.push_str("=\"");
                        buf.push_str(v);
                    }
                    buf.push_str("\"} ");
                } else {
                    buf.push_str(" ");
                }
                buf.push_str(&v.to_string());
                buf.push_str("\n");
            },
            AggregationMethod::Histogram => if let Some(bin_iter) = value.bins() {
                if seen.insert(&value.name) {
                    buf.push_str("# TYPE ");
                    buf.push_str(&value.name);
                    buf.push_str(" histogram\n");
                }
                let mut running_sum = 0;
                for &(bound, val) in bin_iter {
                    buf.push_str(&value.name);
                    buf.push_str("{le=\"");
                    match bound {
                        Bound::Finite(bnd) => {
                            buf.push_str(&bnd.to_string());
                        }
                        Bound::PosInf => {
                            buf.push_str("+Inf");
                        }
                    }
                    for (k, v) in &(*value.tags) {
                        buf.push_str("\", ");
                        buf.push_str(k);
                        buf.push_str("=\"");
                        buf.push_str(v);
                    }
                    buf.push_str("\"} ");
                    buf.push_str(&(val + running_sum).to_string());
                    running_sum += val;
                    buf.push_str("\n");
                }
                buf.push_str(&value.name);
                buf.push_str("_sum ");
                if !value.tags.is_empty() {
                    buf.push_str("{");
                    for (k, v) in &(*value.tags) {
                        buf.push_str(k);
                        buf.push_str("=\"");
                        buf.push_str(v);
                        buf.push_str("\", ");
                    }
                    buf.push_str("} ");
                }
                buf.push_str(&value.samples_sum().unwrap_or(0.0).to_string());
                buf.push_str("\n");
                buf.push_str(&value.name);
                buf.push_str("_count ");
                if !value.tags.is_empty() {
                    buf.push_str("{");
                    for (k, v) in &(*value.tags) {
                        buf.push_str(k);
                        buf.push_str("=\"");
                        buf.push_str(v);
                        buf.push_str("\", ");
                    }
                    buf.push_str("} ");
                }
                buf.push_str(&value.count().to_string());
                buf.push_str("\n");
            },
            AggregationMethod::Summarize => {
                if seen.insert(&value.name) {
                    buf.push_str("# TYPE ");
                    buf.push_str(&value.name);
                    buf.push_str(" summary\n");
                }
                let sum_tags = sync::Arc::clone(&value.tags);
                let count_tags = sync::Arc::clone(&value.tags);
                for q in &[0.0, 1.0, 0.25, 0.5, 0.75, 0.90, 0.95, 0.99, 0.999] {
                    buf.push_str(&value.name);
                    buf.push_str("{quantile=\"");
                    buf.push_str(&q.to_string());
                    for (k, v) in &(*value.tags) {
                        buf.push_str("\", ");
                        buf.push_str(k);
                        buf.push_str("=\"");
                        buf.push_str(v);
                    }
                    buf.push_str("\"} ");
                    buf.push_str(&value.query(*q).unwrap().to_string());
                    buf.push_str("\n");
                }
                buf.push_str(&value.name);
                buf.push_str("_sum ");
                if !sum_tags.is_empty() {
                    buf.push_str("{");
                    for (k, v) in &(*sum_tags) {
                        buf.push_str(k);
                        buf.push_str("=\"");
                        buf.push_str(v);
                        buf.push_str("\", ");
                    }
                    buf.push_str("} ");
                }
                let (retained_count, retained_sum) =
                    if let Some(retainer) = retainers.get(&value.hash()) {
                        (retainer.historic_count, retainer.historic_sum)
                    } else {
                        (0, 0.0)
                    };
                buf.push_str(&(value.samples_sum().unwrap_or(0.0) + retained_sum)
                    .to_string());
                buf.push_str("\n");
                buf.push_str(&value.name);
                buf.push_str("_count ");
                if !count_tags.is_empty() {
                    buf.push_str("{");
                    for (k, v) in &(*count_tags) {
                        buf.push_str(k);
                        buf.push_str("=\"");
                        buf.push_str(v);
                        buf.push_str("\", ");
                    }
                    buf.push_str("} ");
                }
                buf.push_str(&(value.count() + retained_count).to_string());
                buf.push_str("\n");
            }
        }
        res.write_all(buf.as_bytes()).expect(
            "FAILED TO WRITE BUFFER INTO HTTP
    STREAMING RESPONSE",
        );
        buf.clear();
    }
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
///
/// In addition, we want to make sure nothing goofy happens to our metrics and
/// so set the kind to Summarize. The prometheus sink _does not_ respect source
/// metadata and stores everything as quantiles.
fn sanitize(mut metric: metric::Telemetry) -> metric::Telemetry {
    let name: String = mem::replace(&mut metric.name, Default::default());
    let mut new_name: Vec<u8> = Vec::with_capacity(128);
    for c in name.as_bytes() {
        match *c {
            b'a'...b'z' | b'A'...b'Z' | b'0'...b'9' | b':' | b'_' => new_name.push(*c),
            _ => new_name.push(b'_'),
        }
    }
    metric
        .thaw()
        .name(String::from_utf8(new_name).expect("wait, we bungled the conversion"))
        .kind(metric::AggregationMethod::Summarize)
        .harden()
        .unwrap()
}

impl Sink for Prometheus {
    fn flush_interval(&self) -> Option<u64> {
        Some(1)
    }

    fn flush(&mut self) {
        let mut aggrs = self.aggrs.lock().unwrap();
        aggrs.oldest_timestamp = time::now() - (aggrs.retain_limit as i64);
    }

    fn deliver(&mut self, mut point: sync::Arc<Option<metric::Telemetry>>) -> () {
        let mut aggrs = self.aggrs.lock().unwrap();
        let metric = sanitize(sync::Arc::make_mut(&mut point).take().unwrap());
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
            let retain_limit: usize = match Arbitrary::arbitrary(g) {
                0 => 1,
                n => n,
            };
            let limit: usize = Arbitrary::arbitrary(g);
            let mut windowed: HashMap<
                u64,
                Vec<metric::Telemetry>,
                BuildHasherDefault<SeaHasher>,
            > = Default::default();
            for _ in 0..limit {
                let telem: metric::Telemetry = Arbitrary::arbitrary(g);
                let ts_vec = windowed.entry(telem.hash()).or_insert(vec![]);
                match (*ts_vec)
                    .binary_search_by(|probe| probe.timestamp.cmp(&telem.timestamp))
                {
                    Ok(idx) => ts_vec[idx] += telem,
                    Err(idx) => ts_vec.insert(idx, telem),
                }
            }
            let mut perpetual: HashMap<
                u64,
                metric::Telemetry,
                BuildHasherDefault<SeaHasher>,
            > = Default::default();
            for _ in 0..limit {
                let telem: metric::Telemetry = Arbitrary::arbitrary(g);
                perpetual.insert(telem.hash(), telem);
            }
            PrometheusAggr {
                retain_limit: retain_limit,
                perpetual: perpetual,
                windowed: windowed,
                retainers: Default::default(),
                oldest_timestamp: 0,
            }
        }
    }

    // * recombining points should increase the size of aggr by the size of the
    //   report vec
    // * recombining points should adjust the last_report to the least of the
    //   combined points
    #[test]
    fn test_recombine() {
        fn inner(
            mut aggr: PrometheusAggr,
            recomb: Vec<metric::Telemetry>,
        ) -> TestResult {
            let cur_cnt = aggr.count();
            let recomb_len = recomb.len();

            aggr.recombine(recomb);

            let lower = cur_cnt;
            let upper = cur_cnt + recomb_len;

            assert!(lower <= aggr.count() || aggr.count() <= upper);
            TestResult::passed()
        }
        QuickCheck::new().tests(1000).max_tests(10000).quickcheck(
            inner
                as fn(PrometheusAggr, Vec<metric::Telemetry>)
                    -> TestResult,
        );
    }

    #[test]
    fn test_reportable() {
        fn inner(mut aggr: PrometheusAggr) -> TestResult {
            let cur_cnt = aggr.count();

            let reportable = aggr.reportable();

            assert_eq!(cur_cnt, reportable.len());

            TestResult::passed()
        }
        QuickCheck::new()
            .tests(1000)
            .max_tests(10000)
            .quickcheck(inner as fn(PrometheusAggr) -> TestResult);
    }

    // insertion must obey two properties, based on existence or not
    //
    //  IF EXISTS
    //    - insertion should NOT increase total count
    //    - insertion WILL modify existing telemetry in aggregation
    //
    //  IF NOT EXISTS
    //    - insertion WILL increase count by 1
    //    - insertion WILL make telemetry exist after the insertion
    #[test]
    fn test_insertion_exists_property() {
        fn inner(mut aggr: PrometheusAggr, telem: metric::Telemetry) -> TestResult {
            let cur_cnt = aggr.count();
            match aggr.find_match(&telem) {
                Some(other) => {
                    assert!(aggr.insert(telem.clone()));
                    assert_eq!(cur_cnt, aggr.count());
                    let new_t =
                        aggr.find_match(&telem).expect("could not find in test");
                    assert_eq!(other.name, new_t.name);
                    assert_eq!(new_t.kind(), telem.kind());
                }
                None => return TestResult::discard(),
            }
            TestResult::passed()
        }
        QuickCheck::new().tests(1000).max_tests(10000).quickcheck(
            inner as fn(PrometheusAggr, metric::Telemetry) -> TestResult,
        );
    }

    #[test]
    fn test_insertion_not_exists_property() {
        fn inner(mut aggr: PrometheusAggr, telem: metric::Telemetry) -> TestResult {
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
        QuickCheck::new().tests(1000).max_tests(10000).quickcheck(
            inner as fn(PrometheusAggr, metric::Telemetry) -> TestResult,
        );
    }

    #[test]
    fn test_sanitization() {
        fn inner(metric: metric::Telemetry) -> TestResult {
            let metric = sanitize(metric);
            for c in metric.name.chars() {
                match c {
                    'a'...'z' | 'A'...'Z' | '0'...'9' | ':' | '_' => continue,
                    _ => return TestResult::failed(),
                }
            }
            TestResult::passed()
        }
        QuickCheck::new()
            .tests(10000)
            .max_tests(100000)
            .quickcheck(inner as fn(metric::Telemetry) -> TestResult);
    }
}
