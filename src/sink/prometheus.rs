//! Prometheus is a pull-based aggregation server

use hyper::server::{Handler, Listening, Request, Response, Server};
use metric;
use protobuf::Message;
use protobuf::repeated::RepeatedField;
use protocols::prometheus::*;
use sink::{Sink, Valve};
use std::io;
use std::io::Write;
use std::mem;
use std::str;
use std::sync;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

lazy_static! {
    /// Total reportable metrics
    pub static ref PROMETHEUS_AGGR_REPORTABLE: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
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
    // The approach we take is unique in cernan: we drop all timestamps and _do
    // not bin_. This is driven by the following comment in Prometheus' doc /
    // our own experience trying to set explict timestamps:
    //
    //     Accordingly you should not set timestamps on the metric you expose,
    //     let Prometheus take care of that. If you think you need timestamps,
    //     then you probably need the pushgateway (without timestamps) instead.
    //
    // Prometheus has a certain view of the world -- and that's fine -- so we
    // need to meet it there.
    inner: Vec<metric::Telemetry>,
}

fn prometheus_cmp(
    l: &metric::Telemetry,
    r: &metric::Telemetry,
) -> Option<::std::cmp::Ordering> {
    match l.name.partial_cmp(&r.name) {
        Some(::std::cmp::Ordering::Equal) => ::metric::tagmap::cmp(&l.tags, &r.tags),
        other => other,
    }
}

impl PrometheusAggr {
    /// Return a reference to the stored Telemetry if it matches the passed
    /// Telemetry
    #[cfg(test)]
    fn find_match(&self, telem: &metric::Telemetry) -> Option<metric::Telemetry> {
        match self.inner.binary_search_by(|probe| {
            prometheus_cmp(probe, &telem).expect("could not compare")
        }) {
            Ok(idx) => Some(self.inner[idx].clone()),
            Err(_) => None,
        }
    }

    /// Return all 'reportable' Telemetry
    ///
    /// This function returns all the stored Telemetry points that are available
    /// for shipping to Prometheus. Shipping a point to Prometheus drops that
    /// point from memory, once it's gone over the wire.
    fn reportable(&mut self) -> Vec<metric::Telemetry> {
        mem::replace(&mut self.inner, Default::default())
    }

    /// Insert a Telemetry into the aggregation
    ///
    /// This function inserts the given Telemetry into the inner aggregation of
    /// PrometheusAggr. Timestamps are _not_ respected. Distinctions between
    /// Telemetry of the same name are only made if their tagmaps are distinct.
    fn insert(&mut self, telem: metric::Telemetry) -> bool {
        match self.inner.binary_search_by(|probe| {
            prometheus_cmp(probe, &telem).expect("could not compare")
        }) {
            Ok(idx) => self.inner[idx] += telem,
            Err(idx) => self.inner.insert(idx, telem),
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
        self.inner.len()
    }
}

impl Default for PrometheusAggr {
    fn default() -> PrometheusAggr {
        PrometheusAggr {
            inner: Default::default(),
        }
    }
}

impl Handler for SenderHandler {
    fn handle(&self, req: Request, res: Response) {
        let mut aggr = self.aggr.lock().unwrap();
        let reportable: Vec<metric::Telemetry> = aggr.reportable();
        PROMETHEUS_AGGR_REPORTABLE.fetch_add(reportable.len(), Ordering::Relaxed);
        PROMETHEUS_AGGR_REMAINING.fetch_add(aggr.count(), Ordering::Relaxed);
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
            write_binary(&reportable, res)
        } else {
            PROMETHEUS_WRITE_TEXT.fetch_add(1, Ordering::Relaxed);
            write_text(&reportable, res)
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
        let aggrs = sync::Arc::new(sync::Mutex::new(Default::default()));
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

fn write_binary(aggrs: &[metric::Telemetry], mut res: Response) -> io::Result<()> {
    res.headers_mut().set_raw(
        "content-type",
        vec![
            b"application/vnd.google.protobuf; \
                       proto=io.prometheus.client.MetricFamily; encoding=delimited"
                .to_vec(),
        ],
    );
    let mut res = res.start().unwrap();
    for m in aggrs {
        let mut metric_family = MetricFamily::new();
        metric_family.set_name(m.name.clone());
        let mut metric = Metric::new();
        let mut label_pairs = Vec::with_capacity(8);
        for &(ref k, ref v) in m.tags.iter() {
            let mut lp = LabelPair::new();
            lp.set_name(k.clone());
            lp.set_value(v.clone());
            label_pairs.push(lp);
        }
        metric.set_label(RepeatedField::from_vec(label_pairs));
        let mut summary = Summary::new();
        summary.set_sample_count(m.count() as u64);
        if let Some(smpl_sum) = m.samples_sum() {
            summary.set_sample_sum(smpl_sum);
        }
        let mut quantiles = Vec::with_capacity(9);
        for q in &[0.0, 1.0, 0.25, 0.5, 0.75, 0.90, 0.95, 0.99, 0.999] {
            let mut quantile = Quantile::new();
            quantile.set_quantile(*q);
            quantile.set_value(m.query(*q).unwrap());
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
    res.end()
}

fn write_text(aggrs: &[metric::Telemetry], mut res: Response) -> io::Result<()> {
    res.headers_mut()
        .set_raw("content-type", vec![b"text/plain; version=0.0.4".to_vec()]);
    let mut buf = String::with_capacity(1024);
    let mut res = res.start().unwrap();
    for m in aggrs {
        let sum_tags = sync::Arc::clone(&m.tags);
        let count_tags = sync::Arc::clone(&m.tags);
        for q in &[0.0, 1.0, 0.25, 0.5, 0.75, 0.90, 0.95, 0.99, 0.999] {
            buf.push_str(&m.name);
            buf.push_str("{quantile=\"");
            buf.push_str(&q.to_string());
            for (k, v) in &(*m.tags) {
                buf.push_str("\", ");
                buf.push_str(k);
                buf.push_str("=\"");
                buf.push_str(v);
            }
            buf.push_str("\"} ");
            buf.push_str(&m.query(*q).unwrap().to_string());
            buf.push_str("\n");
        }
        buf.push_str(&m.name);
        buf.push_str("_sum ");
        buf.push_str("{");
        for (k, v) in &(*sum_tags) {
            buf.push_str(k);
            buf.push_str("=\"");
            buf.push_str(v);
            buf.push_str("\", ");
        }
        buf.push_str("} ");
        buf.push_str(&m.samples_sum().unwrap_or(0.0).to_string());
        buf.push_str("\n");
        buf.push_str(&m.name);
        buf.push_str("_count ");
        buf.push_str("{");
        for (k, v) in &(*count_tags) {
            buf.push_str(k);
            buf.push_str("=\"");
            buf.push_str(v);
            buf.push_str("\", ");
        }
        buf.push_str("} ");
        buf.push_str(&m.count().to_string());
        buf.push_str("\n");
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
        None
    }

    fn flush(&mut self) {
        // There is no flush for the Prometheus sink. Prometheus prefers to
        // pull via HTTP / Protobuf. See PrometheusSrv.
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
        let aggrs = self.aggrs.lock().unwrap();
        if aggrs.count() > 10_000 {
            Valve::Closed
        } else {
            Valve::Open
        }
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
            let mut inner: Vec<metric::Telemetry> = Arbitrary::arbitrary(g);
            inner.sort_by(|a, b| prometheus_cmp(a, b).unwrap());
            PrometheusAggr { inner: inner }
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
            assert_eq!(0, aggr.count());

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
                    // TODO
                    //
                    // This will not longer function correctly. Previously we
                    // _only_ checked that the count got bumpbed but count is
                    // not a sensible thing when Telemetry is not all CKMS
                    // backed.
                    //
                    // That's okay. In the future we'll be obeying the
                    // aggregation of the input Telemetry and then this will
                    // work.
                    //
                    // assert_eq!(other.value(), new_t.value());
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
