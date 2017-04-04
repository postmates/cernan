use hyper::header::ContentType;
use hyper::mime::{Attr, Mime, SubLevel, TopLevel, Value};
use hyper::server::{Handler, Listening, Request, Response, Server};
use metric;
use protobuf::Message;
use protobuf::repeated::RepeatedField;
use protocols::prometheus::*;
use sink::{Sink, Valve};
use source::report_telemetry;
use std::io::Write;
use std::mem;
use std::str;
use std::sync;
use std::sync::Mutex;
use time;

pub type AggrMap = Vec<metric::Telemetry>;

#[allow(dead_code)]
pub struct Prometheus {
    aggrs: sync::Arc<Mutex<AggrMap>>,
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
    aggrs: sync::Arc<Mutex<AggrMap>>,
}

#[inline]
fn write_binary(aggrs: AggrMap, mut res: Response) {
    let mut params = Vec::with_capacity(2);
    params.push((Attr::Ext("proto".to_string()),
                 Value::Ext("io.prometheus.client.MetricFamily".to_string())));
    params.push((Attr::Ext("encoding".to_string()), Value::Ext("delimited".to_string())));
    res.headers_mut().set(ContentType(Mime(TopLevel::Application,

                                           SubLevel::Ext("application/vnd.google.protobuf"
                                               .to_string()),
                                           params)));
    let mut res = res.start().unwrap();
    for m in aggrs.into_iter() {
        let mut metric_family = MetricFamily::new();
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
    res.end().expect("FAILED TO CLOSE HTTP STREAMING RESPONSE");
}

#[inline]
fn write_text(aggrs: AggrMap, mut res: Response) {
    let mut params = Vec::with_capacity(1);
    params.push((Attr::Ext("version".to_string()), Value::Ext("0.0.4".to_string())));
    res.headers_mut().set(ContentType(Mime(TopLevel::Text, SubLevel::Plain, params)));
    let mut buf = String::with_capacity(1024);
    let mut res = res.start().unwrap();
    for m in aggrs.into_iter() {
        for q in &[0.0, 1.0, 0.25, 0.5, 0.75, 0.90, 0.95, 0.99, 0.999] {
            buf.push_str(&m.name);
            buf.push_str("{quantile=\"");
            buf.push_str(&q.to_string());
            buf.push_str("\"} ");
            buf.push_str(&m.query(*q).unwrap().to_string());
            buf.push_str(" ");
            buf.push_str(&m.timestamp.to_string());
            buf.push_str("\n");
        }
        buf.push_str(&m.name);
        buf.push_str("_sum ");
        buf.push_str(&m.sum().to_string());
        buf.push_str(" ");
        buf.push_str(&m.timestamp.to_string());
        buf.push_str("\n");
        buf.push_str(&m.name);
        buf.push_str("_count ");
        buf.push_str(&m.count().to_string());
        buf.push_str(" ");
        buf.push_str(&m.timestamp.to_string());
        buf.push_str("\n");
        res.write(buf.as_bytes()).expect("FAILED TO WRITE BUFFER INTO HTTP
    STREAMING RESPONSE");
        buf.clear();
    }
    res.end().expect("FAILED TO CLOSE HTTP STREAMING RESPONSE");
}

impl Handler for SenderHandler {
    fn handle(&self, req: Request, res: Response) {
        let mut guard = self.aggrs.lock().unwrap();
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
        let current_second = time::now(); // NOTE when #166 lands this will
        // _not_ be accurate and must be correctly adjusted.
        let aggrs: AggrMap = mem::replace(&mut guard, Default::default());
        let (reportable, not_fresh): (Vec<metric::Telemetry>, Vec<metric::Telemetry>) =
            aggrs.into_iter().partition(|ref x| x.timestamp == current_second);
        for x in not_fresh.into_iter() {
            guard.push(x);
        }
        // Typed hyper::mime is challenging to use. In particular, matching does
        // not seem to work like I expect and handling all other MIME cases in
        // the existing enum strikes me as a fool's errand, on account of there
        // may be an infinite number of MIMEs that'll come right on in. We'll
        // just be monsters and assume if you aren't asking for protobuf you're
        // asking for plaintext.
        let raw_headers: Vec<&str> = req.headers
            .get_raw("content-type")
            .unwrap_or(&[])
            .iter()
            .map(|x| str::from_utf8(x))
            .filter(|x| x.is_ok())
            .map(|x| x.unwrap())
            .collect();
        assert!(raw_headers.len() <= 1);
        if raw_headers.is_empty() {
            report_telemetry("cernan.sinks.prometheus.write.text", 1.0);
            report_telemetry("cernan.sinks.prometheus.empty_header", 1.0);
            write_text(reportable, res);
        } else {
            let header = raw_headers[0];
            if header.starts_with("application/vnd.google.protobuf;") {
                report_telemetry("cernan.sinks.prometheus.write.binary", 1.0);
                write_binary(reportable, res);
            } else if header.starts_with("text/plain;") {
                report_telemetry("cernan.sinks.prometheus.write.text", 1.0);
                write_text(reportable, res);
            }
        }
    }
}

impl Prometheus {
    pub fn new(config: PrometheusConfig) -> Prometheus {
        let aggrs = sync::Arc::new(sync::Mutex::new(Default::default()));
        let srv_aggrs = aggrs.clone();
        let listener = Server::http((config.host.as_str(), config.port))
            .unwrap()
            .handle_threads(SenderHandler { aggrs: srv_aggrs }, 1)
            .unwrap();

        Prometheus {
            aggrs: aggrs,
            http_srv: listener,
        }
    }
}

#[inline]
fn sanitize(metric: metric::Telemetry) -> metric::Telemetry {
    // According to https://prometheus.io/docs/instrumenting/writing_exporters/
    // "Only [a-zA-Z0-9:_] are valid in metric names, any other characters
    // should be sanitized to an underscore."
    //
    // Most metrics coming into cernan will have periods in them. This function
    // replaces all those periods with an underscore but does not attempt to do
    // a full sweep and convert every other invalid character.
    let name = metric.name.replace(".", "_");
    // In addition, we want to make sure nothing goofy happens to our metrics
    // and so set the kind to Summarize. The prometheus sink _does not_ respect
    // source metadata and stores everything as quantiles.
    metric.set_name(name).aggr_summarize()
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
        // Why not use Metric::within to do the comparison? The documentation
        // here https://prometheus.io/docs/instrumenting/exposition_formats/
        // demands that "Each Metric within the same MetricFamily must have a
        // unique set of LabelPair fields.". It's very likely in cernan-world
        // that every metric which shares a name also shares its tags. It's not
        // for-sure but very, very likely.
        match aggrs.binary_search_by(|probe| probe.partial_cmp(&metric).unwrap()) {
            Ok(idx) => aggrs[idx] += metric,
            Err(idx) => aggrs.insert(idx, metric),
        };
    }

    fn deliver_line(&mut self, _: sync::Arc<Option<metric::LogLine>>) -> () {
        // nothing, intentionally
    }

    fn valve_state(&self) -> Valve {
        let aggrs = self.aggrs.lock().unwrap();
        if aggrs.len() > 10_000 {
            Valve::Closed
        } else {
            Valve::Open
        }
    }
}

#[cfg(test)]
mod test {}
