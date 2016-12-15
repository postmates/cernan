use std::net::TcpStream;
use std::fmt::Write;
use std::io::Write as IoWrite;
use metric::{Metric, LogLine, TagMap, MetricKind};
use buckets::Buckets;
use sink::{Sink, Valve};
use std::net::ToSocketAddrs;
use time;

pub struct Wavefront {
    host: String,
    port: u16,
    aggrs: Buckets,
    delivery_attempts: u32,
    sink_name: String,
    global_tags: TagMap,
}

#[derive(Debug)]
pub struct WavefrontConfig {
    pub bin_width: i64,
    pub host: String,
    pub port: u16,
    pub config_path: String,
    pub tags: TagMap,
}

#[inline]
fn fmt_tags(tags: &TagMap) -> String {
    let mut s = String::new();
    let mut iter = tags.iter();
    if let Some(&(ref fk, ref fv)) = iter.next() {
        write!(s, "{}={}", fk, fv).unwrap();
        for &(ref k, ref v) in iter {
            write!(s, " {}={}", k, v).unwrap();
        }
    }
    s
}

impl Wavefront {
    pub fn new(config: WavefrontConfig) -> Wavefront {
        Wavefront {
            host: config.host,
            port: config.port,
            aggrs: Buckets::new(config.bin_width),
            delivery_attempts: 0,
            sink_name: config.config_path,
            global_tags: config.tags,
        }
    }

    /// Convert the buckets into a String that
    /// can be sent to the the wavefront proxy
    pub fn format_stats(&mut self, now: i64) -> String {
        let mut stats = String::new();
        let mut payload_size: u32 = 0;
        let mut gauge_payload_size: u32 = 0;
        let mut delta_gauge_payload_size: u32 = 0;
        let mut raw_payload_size: u32 = 0;
        let mut counter_payload_size: u32 = 0;
        let mut timer_payload_size: u32 = 0;
        let mut histogram_payload_size: u32 = 0;
        let mut time_spreads = Metric::new(format!("cernan.{}.time_spreads", self.sink_name), 0.0)
            .histogram();
        let mut counter_time_spreads = Metric::new(format!("cernan.{}.counter.time_spreads",
                                                           self.sink_name),
                                                   0.0)
            .histogram();
        let mut gauge_time_spreads =
            Metric::new(format!("cernan.{}.gauge.time_spreads", self.sink_name), 0.0).histogram();
        let mut delta_gauge_time_spreads =
            Metric::new(format!("cernan.{}.delta_gauge.time_spreads", self.sink_name), 0.0).histogram();
        let mut raw_time_spreads =
            Metric::new(format!("cernan.{}.raw.time_spreads", self.sink_name), 0.0).histogram();
        let mut timer_time_spreads =
            Metric::new(format!("cernan.{}.timer.time_spreads", self.sink_name), 0.0).histogram();
        let mut histogram_time_spreads = Metric::new(format!("cernan.{}.histogram.time_spreads",
                                                             self.sink_name),
                                                     0.0)
            .histogram();

        for kind in &[MetricKind::Counter,
                      MetricKind::Gauge,
                      MetricKind::DeltaGauge,
                      MetricKind::Timer,
                      MetricKind::Histogram,
                      MetricKind::Raw] {
            match kind {
                &MetricKind::Counter => {
                    for (key, vals) in self.aggrs.counters().iter() {
                        for m in vals {
                            if let Some(v) = m.value() {
                                payload_size = payload_size.saturating_add(1);
                                counter_payload_size = counter_payload_size.saturating_add(1);
                                time_spreads =
                                    time_spreads.insert_value((m.created_time - now).abs() as f64);
                                counter_time_spreads = counter_time_spreads.insert_value((m.created_time - now).abs() as f64);
                                write!(stats, "{} {} {} {}\n", key, v, m.time, fmt_tags(&m.tags))
                                    .unwrap();
                            }
                        }
                    }
                }
                &MetricKind::Gauge => {
                    for (key, vals) in self.aggrs.gauges().iter() {
                        for m in vals {
                            if let Some(v) = m.value() {
                                payload_size = payload_size.saturating_add(1);
                                gauge_payload_size = gauge_payload_size.saturating_add(1);
                                time_spreads =
                                    time_spreads.insert_value((m.created_time - now).abs() as f64);
                                gauge_time_spreads = gauge_time_spreads.insert_value((m.created_time - now).abs() as f64);
                                write!(stats, "{} {} {} {}\n", key, v, m.time, fmt_tags(&m.tags))
                                    .unwrap();
                            }
                        }
                    }
                }
                &MetricKind::DeltaGauge => {
                    for (key, vals) in self.aggrs.delta_gauges().iter() {
                        for m in vals {
                            if let Some(v) = m.value() {
                                payload_size = payload_size.saturating_add(1);
                                delta_gauge_payload_size = delta_gauge_payload_size.saturating_add(1);
                                time_spreads =
                                    time_spreads.insert_value((m.created_time - now).abs() as f64);
                                delta_gauge_time_spreads = delta_gauge_time_spreads.insert_value((m.created_time - now).abs() as f64);
                                write!(stats, "{} {} {} {}\n", key, v, m.time, fmt_tags(&m.tags))
                                    .unwrap();
                            }
                        }
                    }
                }
                &MetricKind::Raw => {
                    for (key, vals) in self.aggrs.raws().iter() {
                        for m in vals {
                            if let Some(v) = m.value() {
                                payload_size = payload_size.saturating_add(1);
                                raw_payload_size = raw_payload_size.saturating_add(1);
                                time_spreads =
                                    time_spreads.insert_value((m.created_time - now).abs() as f64);
                                raw_time_spreads = raw_time_spreads.insert_value((m.created_time - now).abs() as f64);
                                write!(stats, "{} {} {} {}\n", key, v, m.time, fmt_tags(&m.tags))
                                    .unwrap();
                            }
                        }
                    }
                }
                &MetricKind::Timer => {
                    for (key, hists) in self.aggrs.timers().iter() {
                        for hist in hists {
                            for tup in &[("min", 0.0),
                                         ("max", 1.0),
                                         ("2", 0.02),
                                         ("9", 0.09),
                                         ("25", 0.25),
                                         ("50", 0.5),
                                         ("75", 0.75),
                                         ("90", 0.90),
                                         ("91", 0.91),
                                         ("95", 0.95),
                                         ("98", 0.98),
                                         ("99", 0.99),
                                         ("999", 0.999)] {
                                let stat: &str = tup.0;
                                let quant: f64 = tup.1;
                                write!(stats,
                                       "{}.{} {} {} {}\n",
                                       key,
                                       stat,
                                       hist.query(quant).unwrap(),
                                       hist.time,
                                       fmt_tags(&hist.tags))
                                    .unwrap()
                            }
                            payload_size = payload_size.saturating_add(1);
                            timer_payload_size = timer_payload_size.saturating_add(1);
                            time_spreads =
                                time_spreads.insert_value((hist.created_time - now).abs() as f64);
                            timer_time_spreads = timer_time_spreads.insert_value((hist.created_time - now).abs() as f64);
                            let count = hist.count();
                            write!(stats,
                                   "{}.count {} {} {}\n",
                                   key,
                                   count,
                                   hist.time,
                                   fmt_tags(&hist.tags))
                                .unwrap();
                        }
                    }
                }
                &MetricKind::Histogram => {
                    for (key, hists) in self.aggrs.histograms().iter() {
                        for hist in hists {
                            for tup in &[("min", 0.0),
                                         ("max", 1.0),
                                         ("2", 0.02),
                                         ("9", 0.09),
                                         ("25", 0.25),
                                         ("50", 0.5),
                                         ("75", 0.75),
                                         ("90", 0.90),
                                         ("91", 0.91),
                                         ("95", 0.95),
                                         ("98", 0.98),
                                         ("99", 0.99),
                                         ("999", 0.999)] {
                                let stat: &str = tup.0;
                                let quant: f64 = tup.1;
                                write!(stats,
                                       "{}.{} {} {} {}\n",
                                       key,
                                       stat,
                                       hist.query(quant).unwrap(),
                                       hist.time,
                                       fmt_tags(&hist.tags))
                                    .unwrap()
                            }
                            payload_size = payload_size.saturating_add(1);
                            histogram_payload_size = histogram_payload_size.saturating_add(1);
                            time_spreads =
                                time_spreads.insert_value((hist.created_time - now).abs() as f64);
                            histogram_time_spreads = histogram_time_spreads.insert_value((hist.created_time - now).abs() as f64);
                            let count = hist.count();
                            write!(stats,
                                   "{}.count {} {} {}\n",
                                   key,
                                   count,
                                   hist.time,
                                   fmt_tags(&hist.tags))
                                .unwrap();
                        }
                    }
                }
            }
        }

        for ref spread in &[&time_spreads,
                            &gauge_time_spreads,
                            &delta_gauge_time_spreads,
                            &raw_time_spreads,
                            &counter_time_spreads,
                            &timer_time_spreads,
                            &histogram_time_spreads] {
            for tup in &[("min", 0.0),
                         ("max", 1.0),
                         ("50", 0.5),
                         ("75", 0.75),
                         ("95", 0.95),
                         ("99", 0.99),
                         ("999", 0.999)] {
                let stat: &str = tup.0;
                let quant: f64 = tup.1;
                write!(stats,
                       "{}.{} {} {} {}\n",
                       spread.name,
                       stat,
                       spread.query(quant).unwrap(),
                       now,
                       fmt_tags(&self.global_tags))
                    .unwrap()
            }
            write!(stats,
                   "{}.count {} {} {}\n",
                   spread.name,
                   spread.count(),
                   now, 
                   fmt_tags(&self.global_tags))
                .unwrap();
        }

        write!(stats,
               "cernan.{}.delivery_attempts {} {} {}\n",
               self.sink_name,
               self.delivery_attempts,
               now,
               fmt_tags(&self.global_tags))
            .unwrap();

        for &(sz, nm) in &[(payload_size, "payload_size"),
                           (gauge_payload_size, "gauge_payload_size"),
                           (delta_gauge_payload_size, "delta_gauge_payload_size"),
                           (raw_payload_size, "raw_payload_size"),
                           (counter_payload_size, "counter_payload_size"),
                           (timer_payload_size, "timer_payload_size"),
                           (histogram_payload_size, "histogram_payload_size")] {
            write!(stats,
                   "cernan.{}.{} {} {} {}\n",
                   self.sink_name,
                   nm,
                   sz,
                   now,
                   fmt_tags(&self.global_tags))
                .unwrap();
        }

        stats
    }
}

impl Sink for Wavefront {
    fn flush(&mut self) {
        loop {
            if self.delivery_attempts > 0 {
                debug!("delivery attempts: {}", self.delivery_attempts);
            }
            let addrs = (self.host.as_str(), self.port).to_socket_addrs();
            match addrs {
                Ok(srv) => {
                    let ips: Vec<_> = srv.collect();
                    for ip in ips {
                        time::delay(self.delivery_attempts);
                        match TcpStream::connect(ip) {
                            Ok(mut stream) => {
                                let res = stream.write(self.format_stats(time::now()).as_bytes());
                                if res.is_ok() {
                                    trace!("flushed to wavefront!");
                                    self.aggrs.reset();
                                    self.delivery_attempts = 0;
                                    return;
                                } else {
                                    self.delivery_attempts = self.delivery_attempts
                                        .saturating_add(1);
                                }
                            }
                            Err(e) => {
                                info!("Unable to connect to proxy at {} using addr {} with error \
                                       {}",
                                      self.host,
                                      ip,
                                      e)
                            }
                        }
                    }
                }
                Err(e) => {
                    info!("Unable to perform DNS lookup on host {} with error {}",
                          self.host,
                          e);
                }
            }
        }
    }

    fn deliver(&mut self, point: Metric) -> Valve<Metric> {
        self.aggrs.add(point);
        Valve::Open
    }

    fn deliver_line(&mut self, _: LogLine) -> Valve<LogLine> {
        // nothing, intentionally
        Valve::Open
    }
}

#[cfg(test)]
mod test {
    extern crate quickcheck;

    use metric::{Metric, TagMap};
    use sink::Sink;
    use chrono::{UTC, TimeZone};
    use super::*;

    #[test]
    fn test_format_wavefront() {
        let mut tags = TagMap::default();
        tags.insert("source".into(), "test-src".into());
        let config = WavefrontConfig {
            bin_width: 1,
            host: "127.0.0.1".to_string(),
            port: 1987,
            config_path: "sinks.wavefront".to_string(),
            tags: tags.clone(),
        };
        let mut wavefront = Wavefront::new(config);
        let dt_0 = UTC.ymd(1990, 6, 12).and_hms_milli(9, 10, 11, 00).timestamp();
        let dt_1 = UTC.ymd(1990, 6, 12).and_hms_milli(9, 10, 12, 00).timestamp();
        let dt_2 = UTC.ymd(1990, 6, 12).and_hms_milli(9, 10, 13, 00).timestamp();
        wavefront.deliver(Metric::new("test.counter", -1.0)
            .time(dt_0)
            .counter()
            .overlay_tags_from_map(&tags));
        wavefront.deliver(Metric::new("test.counter", 2.0)
            .time(dt_0)
            .counter()
            .overlay_tags_from_map(&tags));
        wavefront.deliver(Metric::new("test.counter", 3.0)
            .time(dt_1)
            .counter()
            .overlay_tags_from_map(&tags));
        wavefront.deliver(Metric::new("test.gauge", 3.211)
            .time(dt_0)
            .gauge()
            .overlay_tags_from_map(&tags));
        wavefront.deliver(Metric::new("test.gauge", 4.322)
            .time(dt_1)
            .gauge()
            .overlay_tags_from_map(&tags));
        wavefront.deliver(Metric::new("test.gauge", 5.433)
            .time(dt_2)
            .gauge()
            .overlay_tags_from_map(&tags));
        wavefront.deliver(Metric::new("test.timer", 12.101)
            .time(dt_0)
            .timer()
            .overlay_tags_from_map(&tags));
        wavefront.deliver(Metric::new("test.timer", 1.101)
            .time(dt_0)
            .timer()
            .overlay_tags_from_map(&tags));
        wavefront.deliver(Metric::new("test.timer", 3.101)
            .time(dt_0)
            .timer()
            .overlay_tags_from_map(&tags));
        wavefront.deliver(Metric::new("test.raw", 1.0)
            .time(dt_0)
            .overlay_tags_from_map(&tags)
            .created_time(dt_0));
        wavefront.deliver(Metric::new("test.raw", 2.0)
            .time(dt_1)
            .overlay_tags_from_map(&tags)
            .created_time(dt_0));
        let result = wavefront.format_stats(dt_2);
        let lines: Vec<&str> = result.lines().collect();

        println!("{:?}", lines);
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
        assert!(lines.contains(&"test.raw 1 645181811 source=test-src"));
    }
}
