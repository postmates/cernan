use backend::Backend;
use buckets::Buckets;
use std::str::FromStr;
use rustc_serialize::json;
use hyper::client::Client;
use hyper::header::{ContentType, Authorization, Basic, Connection};
use url;
use chrono;
use mime::Mime;
use metric::Metric;
use std::rc::Rc;

pub struct Librato {
    username: String,
    auth_token: String,
    source: String,
    host: String,
    aggrs: Buckets,
}

#[derive(Debug, RustcEncodable)]
struct LCounter {
    name: String,
    value: f64,
}

#[derive(Debug, RustcEncodable)]
struct LGauge {
    name: String,
    value: f64,
}

#[derive(Debug, RustcEncodable)]
struct LPayload {
    gauges: Vec<LGauge>,
    counters: Vec<LCounter>,
    source: String,
    measure_time: i64,
}

impl Librato {
    /// Create a Librato formatter
    ///
    /// # Examples
    ///
    /// ```
    /// let wave = Librato::new(host, port, source);
    /// ```
    pub fn new(username: &str, auth_token: &str, source: &str, host: &str) -> Librato {
        Librato {
            username: String::from_str(username).unwrap(),
            auth_token: String::from_str(auth_token).unwrap(),
            source: String::from_str(source).unwrap(),
            host: String::from_str(host).unwrap(),
            aggrs: Buckets::new(),
        }
    }

    /// Convert the buckets into a String pair vector for later conversion into
    /// a POST body
    pub fn format_stats(&self, curtime: Option<i64>) -> String {
        let start = match curtime {
            Some(x) => x,
            None => chrono::UTC::now().timestamp(),
        };

        let mut gauges = vec![];
        let mut counters = vec![];

        for (key, value) in self.aggrs.counters().iter() {
            counters.push(LCounter {
                name: key.as_ref().to_string(),
                value: *value,
            });
        }
        for (key, value) in self.aggrs.gauges().iter() {
            gauges.push(LGauge {
                name: key.as_ref().to_string(),
                value: *value,
            });
        }

        for (key, value) in self.aggrs.histograms().iter() {
            for tup in &[("min", 0.0),
                         ("max", 1.0),
                         ("50", 0.5),
                         ("90", 0.90),
                         ("99", 0.99),
                         ("999", 0.999)] {
                let stat: &str = tup.0;
                let quant: f64 = tup.1;
                gauges.push(LGauge {
                    name: format!("{}.{}", key, stat),
                    value: value.query(quant).unwrap().1,
                });
            }
        }

        for (key, value) in self.aggrs.timers().iter() {
            for tup in &[("min", 0.0),
                         ("max", 1.0),
                         ("50", 0.5),
                         ("90", 0.90),
                         ("99", 0.99),
                         ("999", 0.999)] {
                let stat: &str = tup.0;
                let quant: f64 = tup.1;
                gauges.push(LGauge {
                    name: format!("{}.{}", key, stat),
                    value: value.query(quant).unwrap().1,
                });
            }
        }

        let obj = LPayload {
            gauges: gauges,
            counters: counters,
            source: self.source.clone(),
            measure_time: start,
        };
        json::encode(&obj).unwrap()
    }
}

impl Backend for Librato {
    fn deliver(&mut self, point: Rc<Metric>) {
        self.aggrs.add(&point);
    }

    fn flush(&mut self) {
        let client = Client::new();
        let payload = self.format_stats(None);
        let mime: Mime = "application/json".parse().unwrap();
        let uri = url::Url::parse(&(self.host)).expect("malformed url");
        client.post(uri)
            .body(&payload)
            .header(ContentType(mime))
            .header(Authorization(Basic {
                username: self.username.clone(),
                password: Some(self.auth_token.clone()),
            }))
            .header(Connection::keep_alive())
            .send()
            .unwrap();
    }
}

#[cfg(test)]
mod test {
    use metric::{Metric, MetricKind};
    use backend::Backend;
    use std::rc::Rc;
    use string_cache::Atom;
    use super::*;

    #[test]
    fn test_format_librato_buckets_no_timers() {
        let mut librato = Librato::new("user", "token", "test-src", "http://librato.example.com");
        librato.deliver(Rc::new(Metric::new(Atom::from("test.counter"),
                                            1.0,
                                            MetricKind::Counter(1.0))));
        librato.deliver(Rc::new(Metric::new(Atom::from("test.gauge"), 3.211, MetricKind::Gauge)));
        librato.deliver(Rc::new(Metric::new(Atom::from("src-test.gauge.2"),
                                            3.211,
                                            MetricKind::Gauge)));
        librato.deliver(Rc::new(Metric::new(Atom::from("test.timer"), 12.101, MetricKind::Timer)));
        librato.deliver(Rc::new(Metric::new(Atom::from("test.timer"), 1.101, MetricKind::Timer)));
        librato.deliver(Rc::new(Metric::new(Atom::from("test.timer"), 3.101, MetricKind::Timer)));
        let result = librato.format_stats(Some(10101));

        println!("{:?}", result);
        assert_eq!("{\"gauges\":[{\"name\":\"test.gauge\",\"value\":3.211},{\"name\":\"src-test.\
                    gauge.2\",\"value\":3.211},{\"name\":\"test.timer.min\",\"value\":1.101},\
                    {\"name\":\"test.timer.max\",\"value\":12.101},{\"name\":\"test.timer.50\",\
                    \"value\":3.101},{\"name\":\"test.timer.90\",\"value\":12.101},{\"name\":\
                    \"test.timer.99\",\"value\":12.101},{\"name\":\"test.timer.999\",\"value\":\
                    12.101}],\"counters\":[{\"name\":\"test.counter\",\"value\":1.0}],\"source\":\
                    \"test-src\",\"measure_time\":10101}",
                   result);
    }
}
