use buckets::Buckets;
use metric::{AggregationMethod, LogLine, TagMap, Telemetry};
use sink::{Sink, Valve};
use source::report_telemetry;
use std::cmp;
use std::collections::HashSet;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::io::Write as IoWrite;
use std::mem;
use std::net::TcpStream;
use std::net::ToSocketAddrs;
use std::string;
use std::sync;
use time;

pub struct Wavefront {
    host: String,
    port: u16,
    bin_width: i64,
    aggrs: Buckets,
    seen_telemetry: HashSet<u64>, // hash of Telemetry.name + Telemetry.tags
    delivery_attempts: u32,
    percentiles: Vec<(String, f64)>,
    pub stats: String,
    flush_interval: u64,
}

fn calculate_hash(t: &Telemetry) -> u64 {
    let mut s = DefaultHasher::new();
    t.name.hash(&mut s);
    t.tags.hash(&mut s);
    s.finish()
}

#[derive(Debug, Deserialize)]
pub struct WavefrontConfig {
    pub bin_width: i64,
    pub host: String,
    pub port: u16,
    pub config_path: Option<String>,
    pub percentiles: Vec<(String, f64)>,
    pub tags: TagMap,
    pub flush_interval: u64,
}

impl Default for WavefrontConfig {
    fn default() -> WavefrontConfig {
        let percentiles = vec![
            ("min".to_string(), 0.0),
            ("max".to_string(), 1.0),
            ("2".to_string(), 0.02),
            ("9".to_string(), 0.09),
            ("25".to_string(), 0.25),
            ("50".to_string(), 0.5),
            ("75".to_string(), 0.75),
            ("90".to_string(), 0.90),
            ("91".to_string(), 0.91),
            ("95".to_string(), 0.95),
            ("98".to_string(), 0.98),
            ("99".to_string(), 0.99),
            ("999".to_string(), 0.999),
        ];
        WavefrontConfig {
            bin_width: 1,
            host: "localhost".to_string(),
            port: 2878,
            config_path: Some("sinks.wavefront".to_string()),
            percentiles: percentiles,
            tags: TagMap::default(),
            flush_interval: 60,
        }
    }
}

#[inline]
fn fmt_tags(tags: &TagMap, s: &mut String) -> () {
    let mut iter = tags.iter();
    if let Some(&(ref fk, ref fv)) = iter.next() {
        s.push_str(fk);
        s.push_str("=");
        s.push_str(fv);
        for &(ref k, ref v) in iter {
            s.push_str(" ");
            s.push_str(k);
            s.push_str("=");
            s.push_str(v);
        }
    }
}

pub fn padding<I: Iterator<Item = Telemetry>>(xs: I, span: i64) -> Padding<I> {
    Padding {
        span: span,
        orig: xs,
        emit_q: Vec::new(),
    }
}

pub struct Padding<I> {
    span: i64,
    orig: I,
    emit_q: Vec<Telemetry>,
}

impl<I> Iterator for Padding<I>
where
    I: Iterator<Item = Telemetry>,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        //println!("{:?}", self.emit_q);
        let next_x = if let Some(x) = self.emit_q.pop() {
            Some(x)
        } else {
            self.orig.next()
        };
        let next_y = if let Some(y) = self.emit_q.pop() {
            Some(y)
        } else {
            self.orig.next()
        };
        match (next_x, next_y) {
            (Some(x), Some(y)) => {
                //println!("x: {:?} || y: {:?}", x, y);
                ////println!("x_hash: {:?} || y_hash: {:?}", x.hash(), y.hash());
                if x.hash() == y.hash() {
                    //println!("span_comp: {:?}", (x.timestamp - y.timestamp).abs() / self.span);
                    match (x.timestamp - y.timestamp).abs() / self.span {
                        0 | 1 => {
                            self.emit_q.push(y);
                            return Some(x);
                        }
                        _ => {
                            if x.value() == Some(0.0) {
                                self.emit_q.push(y);
                            } else {
                                let sub_y = y.clone().timestamp(y.timestamp - 1).set_value(0.0);
                                self.emit_q.push(y);
                                self.emit_q.push(sub_y);
                                self.emit_q.push(
                                    x.clone().timestamp(x.timestamp + 1).set_value(0.0),
                                );
                            }
                            return Some(x);
                        }
                    }
                } else {
                    self.emit_q.push(y);
                    return Some(x);
                }
            }
            (Some(x), None) => {
                // end of sequence
                //println!("END OF SEQUENCE");
                return Some(x);
            }
            (None, _) => {
                //println!("FELL OFF THE WORLD");
                return None;
            }
        }
    }
}

#[inline]
fn get_from_cache<T>(cache: &mut Vec<(T, String)>, val: T) -> &str
where
    T: cmp::PartialOrd + string::ToString + Copy,
{
    match cache.binary_search_by(|probe| probe.0.partial_cmp(&val).unwrap()) {
        Ok(idx) => &cache[idx].1,
        Err(idx) => {
            let str_val = val.to_string();
            cache.insert(idx, (val, str_val));
            get_from_cache(cache, val)
        }
    }
}

impl Wavefront {
    pub fn new(config: WavefrontConfig) -> Result<Wavefront, String> {
        if config.host == "" {
            return Err("Host can not be empty".to_string());
        }
        Ok(Wavefront {
            host: config.host,
            port: config.port,
            bin_width: config.bin_width,
            aggrs: Buckets::new(config.bin_width),
            seen_telemetry: HashSet::new(),
            delivery_attempts: 0,
            percentiles: config.percentiles,
            stats: String::with_capacity(8_192),
            flush_interval: config.flush_interval,
        })
    }

    /// Convert the buckets into a String that
    /// can be sent to the the wavefront proxy
    pub fn format_stats(&mut self, _: i64) -> () {
        let mut time_cache: Vec<(i64, String)> = Vec::with_capacity(128);
        let mut count_cache: Vec<(usize, String)> = Vec::with_capacity(128);
        let mut value_cache: Vec<(f64, String)> = Vec::with_capacity(128);

        let mut tag_buf = String::with_capacity(1_024);
        let aggrs = mem::replace(&mut self.aggrs, Buckets::default());
        for value in padding(aggrs.into_iter(), self.bin_width) {
            match value.aggr_method {
                AggregationMethod::Sum => {
                    report_telemetry("cernan.sinks.wavefront.aggregation.sum", 1.0)
                }
                AggregationMethod::Set => {
                    report_telemetry("cernan.sinks.wavefront.aggregation.set", 1.0)
                }
                AggregationMethod::Summarize => {
                    report_telemetry(
                        "cernan.sinks.wavefront.aggregation.summarize",
                        1.0,
                    );
                    report_telemetry(
                        "cernan.sinks.wavefront.aggregation.\
                         summarize.total_percentiles",
                        self.percentiles.len() as f64,
                    );
                }
            };
            match value.aggr_method {
                AggregationMethod::Sum | AggregationMethod::Set => {
                    if let Some(v) = value.value() {
                        self.stats.push_str(&value.name);
                        self.stats.push_str(" ");
                        self.stats.push_str(get_from_cache(&mut value_cache, v));
                        self.stats.push_str(" ");
                        self.stats.push_str(
                                get_from_cache(&mut time_cache, value.timestamp),
                            );
                        self.stats.push_str(" ");
                        fmt_tags(&value.tags, &mut tag_buf);
                        self.stats.push_str(&tag_buf);
                        self.stats.push_str("\n");

                        tag_buf.clear();
                    }
                }
                AggregationMethod::Summarize => {
                    fmt_tags(&value.tags, &mut tag_buf);
                    for tup in &self.percentiles {
                        let stat: &String = &tup.0;
                        let quant: f64 = tup.1;
                        self.stats.push_str(&value.name);
                        self.stats.push_str(".");
                        self.stats.push_str(stat);
                        self.stats.push_str(" ");
                        self.stats.push_str(get_from_cache(
                            &mut value_cache,
                            value.query(quant).unwrap(),
                        ));
                        self.stats.push_str(" ");
                        self.stats.push_str(
                                get_from_cache(&mut time_cache, value.timestamp),
                            );
                        self.stats.push_str(" ");
                        self.stats.push_str(&tag_buf);
                        self.stats.push_str("\n");
                    }
                    let count = value.count();
                    self.stats.push_str(&value.name);
                    self.stats.push_str(".count");
                    self.stats.push_str(" ");
                    self.stats.push_str(get_from_cache(&mut count_cache, count));
                    self.stats.push_str(" ");
                    self.stats
                        .push_str(get_from_cache(&mut time_cache, value.timestamp));
                    self.stats.push_str(" ");
                    self.stats.push_str(&tag_buf);
                    self.stats.push_str("\n");

                    let mean = value.mean();
                    self.stats.push_str(&value.name);
                    self.stats.push_str(".mean");
                    self.stats.push_str(" ");
                    self.stats.push_str(get_from_cache(&mut value_cache, mean));
                    self.stats.push_str(" ");
                    self.stats
                        .push_str(get_from_cache(&mut time_cache, value.timestamp));
                    self.stats.push_str(" ");
                    self.stats.push_str(&tag_buf);
                    self.stats.push_str("\n");

                    tag_buf.clear();
                }
            }
        }
    }
}

impl Sink for Wavefront {
    fn flush_interval(&self) -> Option<u64> {
        Some(self.flush_interval)
    }

    fn flush(&mut self) {
        self.format_stats(time::now());
        loop {
            report_telemetry(
                "cernan.sinks.wavefront.delivery_attempts",
                self.delivery_attempts as f64,
            );
            if self.delivery_attempts > 0 {
                debug!("delivery attempts: {}", self.delivery_attempts);
            }
            let addrs = (self.host.as_str(), self.port).to_socket_addrs();
            match addrs {
                Ok(srv) => {
                    let ips: Vec<_> = srv.collect();
                    for ip in ips {
                        match TcpStream::connect(ip) {
                            Ok(mut stream) => {
                                let res = stream.write(self.stats.as_bytes());
                                if res.is_ok() {
                                    self.stats.clear();
                                    self.delivery_attempts = 0;
                                    return;
                                } else {
                                    self.delivery_attempts =
                                        self.delivery_attempts.saturating_add(1);
                                }
                            }
                            Err(e) => {
                                info!(
                                    "Unable to connect to proxy at {} using addr {} with error \
                                     {}",
                                    self.host,
                                    ip,
                                    e
                                )
                            }
                        }
                        time::delay(self.delivery_attempts);
                    }
                }
                Err(e) => {
                    info!(
                        "Unable to perform DNS lookup on host {} with error {}",
                        self.host,
                        e
                    );
                }
            }
        }
    }

    fn deliver(&mut self, mut point: sync::Arc<Option<Telemetry>>) -> () {
        let telem: Telemetry = sync::Arc::make_mut(&mut point).take().unwrap();
        self.seen_telemetry.insert(calculate_hash(&telem));
        self.aggrs.add(telem);
    }

    fn deliver_line(&mut self, _: sync::Arc<Option<LogLine>>) -> () {
        // nothing, intentionally
    }

    fn valve_state(&self) -> Valve {
        if self.aggrs.len() > 10_000 {
            Valve::Closed
        } else {
            Valve::Open
        }
    }
}

#[cfg(test)]
mod test {
    use quickcheck::{QuickCheck, TestResult};
    use super::*;
    use chrono::{TimeZone, Utc};
    use metric::{TagMap, Telemetry};
    use sink::Sink;
    use std::sync::Arc;

    #[test]
    fn manual_test_no_unpadded_gaps() {
        let bin_width = 1;
        let mut bucket = Buckets::new(bin_width);

        bucket.add(Telemetry::new("", 3.0).timestamp(3).aggr_sum());
        bucket.add(Telemetry::new("", 19.0).timestamp(19).aggr_sum());
        bucket.add(Telemetry::new("", 28.0).timestamp(28).aggr_sum());
        bucket.add(Telemetry::new("", 57.0).timestamp(57).aggr_sum());

        let mut padding = padding(bucket.into_iter(), bin_width);

        {
            let t = padding.next().unwrap();
            assert_eq!(t.name, "");
            assert_eq!(t.value(), Some(3.0));
            assert_eq!(t.timestamp, 3);
        }
        {
            let t = padding.next().unwrap();
            assert_eq!(t.name, "");
            assert_eq!(t.value(), Some(0.0));
            assert_eq!(t.timestamp, 4);
        }
        {
            let t = padding.next().unwrap();
            assert_eq!(t.name, "");
            assert_eq!(t.value(), Some(0.0));
            assert_eq!(t.timestamp, 18);
        }
        {
            let t = padding.next().unwrap();
            assert_eq!(t.name, "");
            assert_eq!(t.value(), Some(19.0));
            assert_eq!(t.timestamp, 19);
        }
        {
            let t = padding.next().unwrap();
            assert_eq!(t.name, "");
            assert_eq!(t.value(), Some(0.0));
            assert_eq!(t.timestamp, 20);
        }
        {
            let t = padding.next().unwrap();
            assert_eq!(t.name, "");
            assert_eq!(t.value(), Some(0.0));
            assert_eq!(t.timestamp, 27);
        }
        {
            let t = padding.next().unwrap();
            assert_eq!(t.name, "");
            assert_eq!(t.value(), Some(28.0));
            assert_eq!(t.timestamp, 28);
        }
        {
            let t = padding.next().unwrap();
            assert_eq!(t.name, "");
            assert_eq!(t.value(), Some(0.0));
            assert_eq!(t.timestamp, 29);
        }
        {
            let t = padding.next().unwrap();
            assert_eq!(t.name, "");
            assert_eq!(t.value(), Some(0.0));
            assert_eq!(t.timestamp, 56);
        }
        {
            let t = padding.next().unwrap();
            assert_eq!(t.name, "");
            assert_eq!(t.value(), Some(57.0));
            assert_eq!(t.timestamp, 57);
        }
    }

    #[test]
    fn test_no_unpadded_gaps() {
        fn inner(bin_width: u8, ms: Vec<Telemetry>) -> TestResult {
            if bin_width == 0 {
                return TestResult::discard();
            }
            // TODO
            //
            // The fact that I have to set a bucket here suggests that 'padding'
            // might be sensible for inclusion as an interator on the
            // Bucket. But! It doesn't have to be that way, if the
            // Vec<Telemetry> is pre-prepped.
            let mut bucket = Buckets::new(bin_width as i64);
            for m in ms.clone() {
                bucket.add(m);
            }
            let mut padding = padding(bucket.into_iter(), bin_width as i64).peekable();

            while let Some(t) = padding.next() {
                if let Some(next_t) = padding.peek() {
                    // When we examine the next point in a series there are
                    // three possibilities:
                    //
                    //  1. the points don't hash the same, so we move on
                    //  2. the points do hash the same:
                    //     a. if their timestamps are greater than one span
                    //        apart then they are both zero
                    //     b. if both points are non-zero they must not be
                    //        more than one span apart
                    if t.hash() == next_t.hash() {
                        let span = (t.timestamp - next_t.timestamp).abs() / (bin_width as i64);
                        // //println!("{:?}\n{:?}", t, next_t);
                        if span > 1 {
                            assert_eq!(t.value(), Some(0.0));
                            assert_eq!(next_t.value(), Some(0.0));
                        }
                        if (t.value() != Some(0.0)) && (next_t.value() != Some(0.0)) {
                            assert!(span <= 1);
                        }
                    } else {
                        continue
                    }
                } else {
                    break;
                }
            }

            TestResult::passed()
        }
        QuickCheck::new().quickcheck(inner as fn(u8, Vec<Telemetry>) -> TestResult);
    }

    #[test]
    fn test_format_wavefront() {
        let mut tags = TagMap::default();
        tags.insert("source".into(), "test-src".into());
        let percentiles = vec![
            ("min".to_string(), 0.0),
            ("max".to_string(), 1.0),
            ("2".to_string(), 0.02),
            ("9".to_string(), 0.09),
            ("25".to_string(), 0.25),
            ("50".to_string(), 0.5),
            ("75".to_string(), 0.75),
            ("90".to_string(), 0.90),
            ("91".to_string(), 0.91),
            ("95".to_string(), 0.95),
            ("98".to_string(), 0.98),
            ("99".to_string(), 0.99),
            ("999".to_string(), 0.999),
        ];
        let config = WavefrontConfig {
            bin_width: 1,
            host: "127.0.0.1".to_string(),
            port: 1987,
            config_path: Some("sinks.wavefront".to_string()),
            tags: tags.clone(),
            percentiles: percentiles,
            flush_interval: 60,
        };
        let mut wavefront = Wavefront::new(config).unwrap();
        let dt_0 = Utc.ymd(1990, 6, 12).and_hms_milli(9, 10, 11, 00).timestamp();
        let dt_1 = Utc.ymd(1990, 6, 12).and_hms_milli(9, 10, 12, 00).timestamp();
        let dt_2 = Utc.ymd(1990, 6, 12).and_hms_milli(9, 10, 13, 00).timestamp();
        wavefront.deliver(Arc::new(Some(
            Telemetry::new("test.counter", -1.0)
                .timestamp(dt_0)
                .aggr_sum()
                .overlay_tags_from_map(&tags),
        )));
        wavefront.deliver(Arc::new(Some(
            Telemetry::new("test.counter", 2.0)
                .timestamp(dt_0)
                .aggr_sum()
                .overlay_tags_from_map(&tags),
        )));
        wavefront.deliver(Arc::new(Some(
            Telemetry::new("test.counter", 3.0)
                .timestamp(dt_1)
                .aggr_sum()
                .overlay_tags_from_map(&tags),
        )));
        wavefront.deliver(Arc::new(Some(
            Telemetry::new("test.gauge", 3.211)
                .timestamp(dt_0)
                .aggr_set()
                .overlay_tags_from_map(&tags),
        )));
        wavefront.deliver(Arc::new(Some(
            Telemetry::new("test.gauge", 4.322)
                .timestamp(dt_1)
                .aggr_set()
                .overlay_tags_from_map(&tags),
        )));
        wavefront.deliver(Arc::new(Some(
            Telemetry::new("test.gauge", 5.433)
                .timestamp(dt_2)
                .aggr_set()
                .overlay_tags_from_map(&tags),
        )));
        wavefront.deliver(Arc::new(Some(
            Telemetry::new("test.timer", 12.101)
                .timestamp(dt_0)
                .aggr_summarize()
                .overlay_tags_from_map(&tags),
        )));
        wavefront.deliver(Arc::new(Some(
            Telemetry::new("test.timer", 1.101)
                .timestamp(dt_0)
                .aggr_summarize()
                .overlay_tags_from_map(&tags),
        )));
        wavefront.deliver(Arc::new(Some(
            Telemetry::new("test.timer", 3.101)
                .timestamp(dt_0)
                .aggr_summarize()
                .overlay_tags_from_map(&tags),
        )));
        wavefront.deliver(Arc::new(Some(
            Telemetry::new("test.raw", 1.0)
                .timestamp(dt_0)
                .aggr_set()
                .overlay_tags_from_map(&tags),
        )));
        wavefront.deliver(Arc::new(Some(
            Telemetry::new("test.raw", 2.0)
                .timestamp(dt_1)
                .aggr_set()
                .overlay_tags_from_map(&tags),
        )));
        wavefront.format_stats(dt_2);
        let lines: Vec<&str> = wavefront.stats.lines().collect();

        //println!("{:?}", lines);
        assert!(lines.contains(&"test.counter 1 645181811 source=test-src"));
        assert!(lines.contains(&"test.counter 3 645181812 source=test-src"));
        assert!(lines.contains(&"test.gauge 3.211 645181811 source=test-src"));
        assert!(lines.contains(&"test.gauge 4.322 645181812 source=test-src"));
        assert!(lines.contains(&"test.gauge 5.433 645181813 source=test-src"));
        assert!(lines.contains(&"test.timer.min 1.101 645181811 source=test-src"));
        assert!(lines.contains(&"test.timer.max 12.101 645181811 source=test-src"));
        assert!(lines.contains(&"test.timer.2 1.101 645181811 source=test-src"));
        assert!(lines.contains(&"test.timer.9 1.101 645181811 source=test-src"));
        assert!(lines.contains(&"test.timer.25 1.101 645181811 source=test-src"));
        assert!(lines.contains(&"test.timer.50 3.101 645181811 source=test-src"));
        assert!(lines.contains(&"test.timer.75 3.101 645181811 source=test-src"));
        assert!(lines.contains(&"test.timer.90 12.101 645181811 source=test-src"));
        assert!(lines.contains(&"test.timer.91 12.101 645181811 source=test-src"));
        assert!(lines.contains(&"test.timer.95 12.101 645181811 source=test-src"));
        assert!(lines.contains(&"test.timer.98 12.101 645181811 source=test-src"));
        assert!(lines.contains(&"test.timer.99 12.101 645181811 source=test-src"));
        assert!(lines.contains(&"test.timer.999 12.101 645181811 source=test-src"));
        assert!(lines.contains(&"test.timer.count 3 645181811 source=test-src"));
        assert!(
            lines.contains(
                &"test.timer.mean 5.434333333333334 645181811 source=test-src"
            )
        );
        assert!(lines.contains(&"test.raw 1 645181811 source=test-src"));
    }
}
