use super::super::backend::Backend;
use super::super::buckets::Buckets;
use std::str::FromStr;
use time;
use rustc_serialize::json;
use hyper::client::Client;
use hyper::header::{ContentType, Authorization, Basic, Connection};
use hyper;
use url;
use mime::Mime;

#[derive(Debug)]
pub struct Librato {
    username: String,
    auth_token: String,
    source: String,
    host: String,
}

#[derive(RustcDecodable, RustcEncodable, Debug)]
pub struct LCounter {
    name: String,
    value: f64,
}

#[derive(RustcDecodable, RustcEncodable, Debug)]
pub struct LGuage {
    name: String,
    value: f64,
}

#[derive(RustcDecodable, RustcEncodable, Debug)]
pub struct LPayload {
    guages: Vec<LGuage>,
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
        }
    }

    /// Convert the buckets into a String pair vector for later conversion into
    /// a POST body
    pub fn format_stats(&self, buckets: &Buckets, curtime: Option<i64>) -> String {
        let start = match curtime {
            Some(x) => x,
            None => time::get_time().sec,
        };

        let mut guages = vec![];
        let mut counters = vec![];

        counters.push(LCounter {
            name: "cernan.bad_messages".to_string(),
            value: buckets.bad_messages() as f64,
        });
        counters.push(LCounter {
            name: "cernan.total_messages".to_string(),
            value: buckets.total_messages() as f64,
        });

        for (key, value) in buckets.counters().iter() {
            counters.push(LCounter {
                name: key.to_string(),
                value: *value,
            });
        }
        for (key, value) in buckets.gauges().iter() {
            guages.push(LGuage {
                name: key.to_string(),
                value: *value,
            });
        }
        let obj = LPayload {
            guages: guages,
            counters: counters,
            source: self.source.clone(),
            measure_time: start,
        };
        json::encode(&obj).unwrap()
    }
}

impl Backend for Librato {
    fn flush(&mut self, buckets: &Buckets) {

        let client = Client::new();
        let payload = self.format_stats(&buckets, Some(time::get_time().sec));
        let mime: Mime = "application/json".parse().unwrap();
        let uri = url::Url::parse(&(self.host)).ok().expect("malformed url");
        let res = client.post(uri)
            .body(&payload)
            .header(ContentType(mime))
            .header(Authorization(Basic {
                username: self.username.clone(),
                password: Some(self.auth_token.clone()),
            }))
            .header(Connection::keep_alive())
            .send()
            .unwrap();
        assert_eq!(res.status, hyper::Ok);
    }
}

#[cfg(test)]
mod test {
    use super::super::super::metric::{Metric, MetricKind};
    use super::super::super::buckets::Buckets;
    use super::*;

    fn make_buckets() -> Buckets {
        let mut buckets = Buckets::new();
        let m1 = Metric::new("test.counter", 1.0, MetricKind::Counter(1.0));
        let m2 = Metric::new("test.gauge", 3.211, MetricKind::Gauge);

        let m3 = Metric::new("test.timer", 12.101, MetricKind::Timer);
        let m4 = Metric::new("test.timer", 1.101, MetricKind::Timer);
        let m5 = Metric::new("test.timer", 3.101, MetricKind::Timer);
        buckets.add(&m1);
        buckets.add(&m2);
        buckets.add(&m3);
        buckets.add(&m4);
        buckets.add(&m5);
        buckets
    }

    #[test]
    fn test_format_librato_buckets_no_timers() {
        let buckets = make_buckets();
        let librato = Librato::new("user", "token", "test-src", "http://librato.example.com");
        let result = librato.format_stats(&buckets, Some(10101));

        assert!(result ==
                "{\"guages\":[{\"name\":\"test.gauge\",\"value\":3.211}],\"counters\":[{\"name\":\
                 \"cernan.bad_messages\",\"value\":0.0},{\"name\":\"cernan.total_messages\",\
                 \"value\":5.0},{\"name\":\"test.counter\",\"value\":1.0}],\"source\":\
                 \"test-src\",\"measure_time\":10101}");
    }
}
