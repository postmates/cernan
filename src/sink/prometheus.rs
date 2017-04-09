use hyper::server::{Handler, Listening, Request, Response, Server};
use metric;
use protobuf::Message;
use protobuf::repeated::RepeatedField;
use protocols::prometheus::*;
use sink::{Sink, Valve};
use source::report_telemetry;
use std::collections::BTreeMap;
use std::io;
use std::io::Write;
use std::mem;
use std::str;
use std::sync;
use std::sync::Mutex;
use time;

#[allow(dead_code)]
pub struct Prometheus {
    aggrs: sync::Arc<Mutex<PrometheusAggr>>,
    // `http_srv` is never used but we must keep it in this struct to avoid the
    // listening server being dropped
    http_srv: Listening,
}

#[derive(Debug)]
pub struct PrometheusConfig {
    pub bin_width: i64,
    pub host: String,
    pub port: u16,
    pub config_path: String,
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
/// PrometheusAggr. It's job is to encode these operations and allow us to
/// establish the above invariants.
#[derive(Clone, Debug)]
struct PrometheusAggr {
    /// last_time_report allows PrometheusAggr to keep track of when the last
    /// report from this structure was done, giving us the ability to reject
    /// points (per point three above) and compute what points to
    /// retain. Because these gates are _per time series_ we must keep a map of
    /// metric-name to report time.
    last_time_report: BTreeMap<String, i64>, // WARNING: UNBOUNDED
    inner: Vec<metric::Telemetry>,
}

impl PrometheusAggr {
    /// Return the last time 'name' was reported
    ///
    /// It's possible that this function will return None if the 'name' was
    /// never part of any report.
    #[cfg(test)]
    fn get_last_time_report(&self, name: &str) -> Option<i64> {
        self.last_time_report.get(name).map(|x| *x)
    }

    /// Return an iterator over the aggregation's stored Telemetry
    #[cfg(test)]
    fn iter(&self) -> ::std::slice::Iter<metric::Telemetry> {
        self.inner.iter()
    }

    /// Return an iterator over the aggregation's report times
    #[cfg(test)]
    fn iter_times(&self) -> ::std::collections::btree_map::Iter<String, i64> {
        self.last_time_report.iter()
    }

    /// Return all 'reportable' Telemetry
    ///
    /// This function returns all the stored Telemetry points that are available
    /// for shipping to Prometheus. Shipping a point to Prometheus drops that
    /// point from memory, once it's gone over the wire.
    ///
    /// A Telemetry will _not_ be reportable if it is within the current second
    /// bin. This function resets the last report time for each time series.
    fn reportable(&mut self, current_time: i64) -> Vec<metric::Telemetry> {
        // Prometheus does not allow metrics to appear from the same time
        // interval across reports. Consider what happens in the following
        // situation:
        //
        //     T0 - - - x - - - REPORT - - - x - - - T1 - - - - - - REPORT
        //
        // Where T0 and T1 are times, REPORT is a prometheus scrape and x is
        // some metric to the same time series. In both cases x will have the
        // same time, T0, but across two reports, giving prometheus
        // heartburn. What we do is partition the guarded set where values in
        // the current second are kept behind.
        let inner = mem::replace(&mut self.inner, Default::default());
        let (reportable, not_ripe): (Vec<metric::Telemetry>, Vec<metric::Telemetry>) =
            inner.into_iter().partition(|ref x| x.timestamp < current_time);
        for x in not_ripe.into_iter() {
            self.inner.push(x);
        }
        reportable
    }

    /// Insert a Telemetry into the aggregation
    ///
    /// This function inserts the given Telemetry into the inner aggregation of
    /// PrometheusAggr, being sure to respect any last report time gates. This
    /// function _does not_ change those time gates.
    ///
    /// If the Telemetry is inserted true is returned, else false.
    fn insert(&mut self, telem: metric::Telemetry) -> bool {
        if let Some(t) = self.last_time_report.get(&telem.name) {
            if telem.timestamp < *t {
                return false;
            }
        };
        // Why not use Metric::within to do the comparison? The documentation
        // here https://prometheus.io/docs/instrumenting/exposition_formats/
        // demands that "Each Metric within the same MetricFamily must have a
        // unique set of LabelPair fields.". It's very likely in cernan-world
        // that every metric which shares a name also shares its tags. It's not
        // for-sure but very, very likely.
        match self.inner.binary_search_by(|probe| probe.partial_cmp(&telem).unwrap()) {
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
        for telem in telems.into_iter() {
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
            last_time_report: Default::default(),
            inner: Default::default(),
        }
    }
}

impl Handler for SenderHandler {
    fn handle(&self, req: Request, res: Response) {
        let current_second = time::now(); // NOTE when #166 lands this will be broken
        let mut aggr = self.aggr.lock().unwrap();
        let reportable: Vec<metric::Telemetry> = aggr.reportable(current_second);
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
            report_telemetry("cernan.sinks.prometheus.write.binary", 1.0);
            write_binary(&reportable, res)
        } else {
            report_telemetry("cernan.sinks.prometheus.write.text", 1.0);
            write_text(&reportable, res)
        };
        if res.is_err() {
            aggr.recombine(reportable);
        }
    }
}

impl Prometheus {
    pub fn new(config: PrometheusConfig) -> Prometheus {
        let aggrs = sync::Arc::new(sync::Mutex::new(Default::default()));
        let srv_aggrs = aggrs.clone();
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
    res.headers_mut()
        .set_raw("content-type",
                 vec!["application/vnd.google.protobuf; \
                       proto=io.prometheus.client.MetricFamily; encoding=delimited"
                          .as_bytes()
                          .to_vec()]);
    let mut res = res.start().unwrap();
    for m in aggrs.into_iter() {
        let mut metric_family = MetricFamily::new();
        metric_family.set_name(m.name.clone());
        let mut metric = Metric::new();
        let mut label_pairs = Vec::with_capacity(8);
        for (k, v) in m.tags.into_iter() {
            let mut lp = LabelPair::new();
            lp.set_name(k.clone());
            lp.set_value(v.clone());
            label_pairs.push(lp);
        }
        metric.set_label(RepeatedField::from_vec(label_pairs));
        metric.set_timestamp_ms(m.timestamp * 1000); // FIXME #166
        let mut summary = Summary::new();
        summary.set_sample_count(m.count() as u64);
        summary.set_sample_sum(m.sum());
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
        metric_family.write_length_delimited_to_writer(res.by_ref())
            .expect("FAILED TO WRITE TO HTTP RESPONSE");
    }
    res.end()
}

fn write_text(aggrs: &[metric::Telemetry], mut res: Response) -> io::Result<()> {
    res.headers_mut().set_raw("content-type",
                              vec!["text/plain; version=0.0.4".as_bytes().to_vec()]);
    let mut buf = String::with_capacity(1024);
    let mut res = res.start().unwrap();
    for m in aggrs.into_iter() {
        let sum_tags = m.tags.clone();
        let count_tags = m.tags.clone();
        for q in &[0.0, 1.0, 0.25, 0.5, 0.75, 0.90, 0.95, 0.99, 0.999] {
            buf.push_str(&m.name);
            buf.push_str("{quantile=\"");
            buf.push_str(&q.to_string());
            for (k, v) in m.tags.into_iter() {
                buf.push_str("\", ");
                buf.push_str(k);
                buf.push_str("=\"");
                buf.push_str(v);
            }
            buf.push_str("\"} ");
            buf.push_str(&m.query(*q).unwrap().to_string());
            buf.push_str(" ");
            buf.push_str(&m.timestamp.to_string());
            buf.push_str("\n");
        }
        buf.push_str(&m.name);
        buf.push_str("_sum ");
        buf.push_str("{");
        for (k, v) in sum_tags.into_iter() {
            buf.push_str(k);
            buf.push_str("=\"");
            buf.push_str(v);
            buf.push_str("\", ");
        }
        buf.push_str("} ");
        buf.push_str(&m.sum().to_string());
        buf.push_str(" ");
        buf.push_str(&m.timestamp.to_string());
        buf.push_str("\n");
        buf.push_str(&m.name);
        buf.push_str("_count ");
        buf.push_str("{");
        for (k, v) in count_tags.into_iter() {
            buf.push_str(k);
            buf.push_str("=\"");
            buf.push_str(v);
            buf.push_str("\", ");
        }
        buf.push_str("} ");
        buf.push_str(&m.count().to_string());
        buf.push_str(" ");
        buf.push_str(&m.timestamp.to_string());
        buf.push_str("\n");
        res.write(buf.as_bytes()).expect("FAILED TO WRITE BUFFER INTO HTTP
    STREAMING RESPONSE");
        buf.clear();
    }
    res.end()
}

/// Sanitize cernan Telemetry into prometheus' notion
///
/// Prometheus is pretty strict about the names of its ingested metrics.
/// According to https://prometheus.io/docs/instrumenting/writing_exporters/
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
    for c in name.as_bytes().into_iter() {
        match *c {
            b'a'...b'z' | b'A'...b'Z' | b'0'...b'9' | b':' | b'_' => new_name.push(*c),
            _ => new_name.push(b'_'),
        }
    }
    metric.set_name(String::from_utf8(new_name).expect("wait, we bungled the conversion"))
        .aggr_summarize()
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
    extern crate quickcheck;
    extern crate rand;

    use self::quickcheck::{Arbitrary, Gen, QuickCheck, TestResult};
    use self::rand::{Rand, Rng};
    use super::*;
    use metric;

    impl Rand for PrometheusAggr {
        fn rand<R: Rng>(rng: &mut R) -> PrometheusAggr {
            let mut lt_reports: BTreeMap<String, i64> = Default::default();
            let total_lt_reports: u16 = rng.gen_range(0, 512);
            for _ in 0..total_lt_reports {
                let name: String = rng.gen_iter::<char>().take(30).collect();
                let report: i64 = rng.gen();
                lt_reports.insert(name, report);
            }
            let total_inner_sz: usize = rng.gen_range(0, 1024);
            PrometheusAggr {
                last_time_report: lt_reports,
                inner: rng.gen_iter::<metric::Telemetry>().take(total_inner_sz).collect(),
            }
        }
    }

    impl Arbitrary for PrometheusAggr {
        fn arbitrary<G: Gen>(g: &mut G) -> PrometheusAggr {
            g.gen()
        }
    }

    // * recombining points should increase the size of aggr by the size of the
    //   report vec
    // * recombining points should adjust the last_report to the least of the
    //   combined points
    #[test]
    fn test_recombine() {
        fn inner(mut aggr: PrometheusAggr, recomb: Vec<metric::Telemetry>) -> TestResult {
            let cur_cnt = aggr.count();
            let expected_cnt = recomb.len() + cur_cnt;
            let mut expected_last_reports: BTreeMap<String, i64> = Default::default();
            for telem in recomb.iter() {
                let elr_t = expected_last_reports.entry(telem.name.clone())
                    .or_insert(telem.timestamp);
                if *elr_t > telem.timestamp {
                    *elr_t = telem.timestamp;
                }
            }

            aggr.recombine(recomb);

            assert_eq!(aggr.count(), expected_cnt);
            for (nm, time) in aggr.iter_times() {
                if let Some(t) = expected_last_reports.get(nm) {
                    assert_eq!(time, t);
                }
            }
            TestResult::passed()
        }
        QuickCheck::new()
            .tests(1000)
            .max_tests(10000)
            .quickcheck(inner as fn(PrometheusAggr, Vec<metric::Telemetry>) -> TestResult);
    }

    #[test]
    fn test_reportable() {
        // points will be dropped on 'reportable' run iff
        //   * telem.timestamp < reportable current_time
        // additionally
        //   * count will be equal to the number of points where time >= reportable
        //     current_time
        //   * last_report_time will be set to the max timestamp such that timestamp
        //     < reportable current_time
        fn inner(mut aggr: PrometheusAggr, current_time: i64) -> TestResult {
            if current_time < 0 {
                return TestResult::discard();
            }
            let mut expected_remain = 0;
            let mut expected_reportable = 0;
            let mut expected_last_reports: BTreeMap<String, i64> = Default::default();
            for telem in aggr.iter() {
                if telem.timestamp >= current_time {
                    expected_remain += 1;
                } else {
                    expected_reportable += 1;
                    let mut lr = expected_last_reports.entry(telem.name.clone()).or_insert(0);
                    if *lr < telem.timestamp {
                        *lr = telem.timestamp;
                    }
                }
            }

            let reportable = aggr.reportable(current_time);

            assert_eq!(expected_reportable, reportable.len());
            assert_eq!(expected_remain, aggr.count());

            for (nm, time) in aggr.iter_times() {
                if let Some(t) = expected_last_reports.get(nm) {
                    assert_eq!(time, t);
                }
            }
            TestResult::passed()
        }
        QuickCheck::new()
            .tests(1000)
            .max_tests(10000)
            .quickcheck(inner as fn(PrometheusAggr, i64) -> TestResult);
    }

    #[test]
    fn test_insertion_property() {
        fn inner(mut aggr: PrometheusAggr, telem: metric::Telemetry) -> TestResult {
            let cur_cnt = aggr.count();
            match aggr.get_last_time_report(&telem.name) {
                Some(t) => {
                    if telem.timestamp < t {
                        // inserting point where time < last_report does not insert
                        assert!(!aggr.insert(telem.clone()));
                        //   * count remains the same
                        assert_eq!(cur_cnt, aggr.count());
                        //   * point timestamp is NOT changed
                        assert_eq!(t, aggr.get_last_time_report(&telem.name).unwrap());
                    } else {
                        // inserting point where time >= last_report does insert
                        assert!(aggr.insert(telem.clone()));
                        //   * count increases by one
                        assert_eq!(cur_cnt + 1, aggr.count());
                        //   * point timestamp is NOT changed
                        assert_eq!(t, aggr.get_last_time_report(&telem.name).unwrap());
                    }
                }
                None => {
                    // inserting new point:
                    //   * insertion returns true
                    assert!(aggr.insert(telem.clone()));
                    //   * count increases by one
                    assert_eq!(cur_cnt + 1, aggr.count());
                    //   * point timestamp is NOT changed
                    assert_eq!(None, aggr.get_last_time_report(&telem.name));
                }
            }
            TestResult::passed()
        }
        QuickCheck::new()
            .tests(1000)
            .max_tests(10000)
            .quickcheck(inner as fn(PrometheusAggr, metric::Telemetry) -> TestResult);
    }

    #[test]
    fn test_sanitization() {
        fn inner(metric: metric::Telemetry) -> TestResult {
            let metric = sanitize(metric);
            assert!(metric.is_summarize());
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
