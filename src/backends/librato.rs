use super::super::backend::Backend;
use super::super::buckets::Buckets;
use std::str::FromStr;
use std::collections::BTreeMap;
use time;
use rustc_serialize::json::{Json, ToJson};
use hyper::client::Client;
use hyper::header::{ContentType, Authorization, Basic, Connection};
use url;
use mime::Mime;
use regex::Regex;

#[derive(Debug)]
pub struct Librato {
    username: String,
    auth_token: String,
    source: String,
    host: String,
}

#[derive(Debug)]
pub struct LCounter {
    name: String,
    value: f64,
    source: Option<String>,
}

impl ToJson for LCounter {
    fn to_json(&self) -> Json {
        let mut d = BTreeMap::new();
        d.insert("name".to_string(), self.name.to_json());
        d.insert("value".to_string(), self.value.to_json());
        match self.source {
            Some(ref src) => {
                d.insert("source".to_string(), src.to_json());
                ()
            }
            None => (),
        };
        Json::Object(d)
    }
}

#[derive(Debug)]
pub struct LGuage {
    name: String,
    value: f64,
    source: Option<String>,
}

impl ToJson for LGuage {
    fn to_json(&self) -> Json {
        let mut d = BTreeMap::new();
        d.insert("name".to_string(), self.name.to_json());
        d.insert("value".to_string(), self.value.to_json());
        match self.source {
            Some(ref src) => {
                d.insert("source".to_string(), src.to_json());
                ()
            }
            None => (),
        };
        Json::Object(d)
    }
}

#[derive(Debug)]
pub struct LPayload {
    guages: Vec<LGuage>,
    counters: Vec<LCounter>,
    source: String,
    measure_time: i64,
}

impl ToJson for LPayload {
    fn to_json(&self) -> Json {
        let mut d = BTreeMap::new();
        d.insert("guages".to_string(), self.guages.to_json());
        d.insert("counters".to_string(), self.counters.to_json());
        d.insert("source".to_string(), self.source.to_json());
        d.insert("measure_time".to_string(), self.measure_time.to_json());
        Json::Object(d)
    }
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

        let re = Regex::new(r"(?x)
((?P<source>.*)-)?  # the source
(?P<metric>.*) # the metric
")
            .unwrap();

        let mut guages = vec![];
        let mut counters = vec![];

        counters.push(LCounter {
            name: "cernan.bad_messages".to_string(),
            value: buckets.bad_messages() as f64,
            source: None,
        });
        counters.push(LCounter {
            name: "cernan.total_messages".to_string(),
            value: buckets.total_messages() as f64,
            source: None,
        });

        for (key, value) in buckets.counters().iter() {
            let caps = re.captures(&key).unwrap();
            counters.push(LCounter {
                name: caps.name("metric").unwrap().to_string(),
                value: *value,
                source: caps.name("source").map(|x| x.to_string()),
            });
        }
        for (key, value) in buckets.gauges().iter() {
            let caps = re.captures(&key).unwrap();
            guages.push(LGuage {
                name: caps.name("metric").unwrap().to_string(),
                value: *value,
                source: caps.name("source").map(|x| x.to_string()),
            });
        }

        for (key, value) in buckets.histograms().iter() {
            let caps = re.captures(&key).unwrap();
            guages.push(LGuage {
                name: format!("{}.min", caps.name("metric").unwrap().to_string()),
                value: value.min().unwrap(),
                source: caps.name("source").map(|x| x.to_string()),
            });
            guages.push(LGuage {
                name: format!("{}.max", caps.name("metric").unwrap().to_string()),
                value: value.max().unwrap(),
                source: caps.name("source").map(|x| x.to_string()),
            });
            guages.push(LGuage {
                name: format!("{}.mean", caps.name("metric").unwrap().to_string()),
                value: value.mean().unwrap(),
                source: caps.name("source").map(|x| x.to_string()),
            });
            guages.push(LGuage {
                name: format!("{}.50", caps.name("metric").unwrap().to_string()),
                value: value.percentile(50.0).unwrap(),
                source: caps.name("source").map(|x| x.to_string()),
            });
            guages.push(LGuage {
                name: format!("{}.90", caps.name("metric").unwrap().to_string()),
                value: value.percentile(90.0).unwrap(),
                source: caps.name("source").map(|x| x.to_string()),
            });
            guages.push(LGuage {
                name: format!("{}.99", caps.name("metric").unwrap().to_string()),
                value: value.percentile(99.0).unwrap(),
                source: caps.name("source").map(|x| x.to_string()),
            });
            guages.push(LGuage {
                name: format!("{}.999", caps.name("metric").unwrap().to_string()),
                value: value.percentile(99.9).unwrap(),
                source: caps.name("source").map(|x| x.to_string()),
            });
        }

        for (key, value) in buckets.timers().iter() {
            let caps = re.captures(&key).unwrap();
            guages.push(LGuage {
                name: format!("{}.min", caps.name("metric").unwrap().to_string()),
                value: value.min().unwrap(),
                source: caps.name("source").map(|x| x.to_string()),
            });
            guages.push(LGuage {
                name: format!("{}.max", caps.name("metric").unwrap().to_string()),
                value: value.max().unwrap(),
                source: caps.name("source").map(|x| x.to_string()),
            });
            guages.push(LGuage {
                name: format!("{}.mean", caps.name("metric").unwrap().to_string()),
                value: value.mean().unwrap(),
                source: caps.name("source").map(|x| x.to_string()),
            });
            guages.push(LGuage {
                name: format!("{}.50", caps.name("metric").unwrap().to_string()),
                value: value.percentile(50.0).unwrap(),
                source: caps.name("source").map(|x| x.to_string()),
            });
            guages.push(LGuage {
                name: format!("{}.90", caps.name("metric").unwrap().to_string()),
                value: value.percentile(90.0).unwrap(),
                source: caps.name("source").map(|x| x.to_string()),
            });
            guages.push(LGuage {
                name: format!("{}.99", caps.name("metric").unwrap().to_string()),
                value: value.percentile(99.0).unwrap(),
                source: caps.name("source").map(|x| x.to_string()),
            });
            guages.push(LGuage {
                name: format!("{}.999", caps.name("metric").unwrap().to_string()),
                value: value.percentile(99.9).unwrap(),
                source: caps.name("source").map(|x| x.to_string()),
            });
        }

        let obj = LPayload {
            guages: guages,
            counters: counters,
            source: self.source.clone(),
            measure_time: start,
        };
        obj.to_json().to_string()
    }
}

impl Backend for Librato {
    fn flush(&mut self, buckets: &Buckets) {

        let client = Client::new();
        let payload = self.format_stats(&buckets, Some(time::get_time().sec));
        let mime: Mime = "application/json".parse().unwrap();
        let uri = url::Url::parse(&(self.host)).ok().expect("malformed url");
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
        // assert_eq!(res.status, hyper::Ok);
    }
}

#[cfg(test)]
mod test {
    use super::super::super::metric::{Metric, MetricKind};
    use super::super::super::buckets::Buckets;
    use regex::Regex;
    use super::*;

    fn make_buckets() -> Buckets {
        let mut buckets = Buckets::new();
        let m1 = Metric::new("test.counter", 1.0, MetricKind::Counter(1.0));
        let m2 = Metric::new("test.gauge", 3.211, MetricKind::Gauge);
        let m6 = Metric::new("src-test.gauge.2", 3.211, MetricKind::Gauge);

        let m3 = Metric::new("test.timer", 12.101, MetricKind::Timer);
        let m4 = Metric::new("test.timer", 1.101, MetricKind::Timer);
        let m5 = Metric::new("test.timer", 3.101, MetricKind::Timer);
        buckets.add(&m1);
        buckets.add(&m2);
        buckets.add(&m3);
        buckets.add(&m4);
        buckets.add(&m5);
        buckets.add(&m6);
        buckets
    }

    #[test]
    fn test_our_regex_with_source() {
        let re = Regex::new(r"(?x)
((?P<source>.*)-)?  # the source
(?P<metric>.*) # the metric
")
            .unwrap();
        let caps = re.captures("source-foo.bar.baz").unwrap();
        assert_eq!(Some("source"), caps.name("source"));
        assert_eq!(Some("foo.bar.baz"), caps.name("metric"));
    }

    #[test]
    fn test_our_regex_no_source() {
        let re = Regex::new(r"(?x)
((?P<source>.*)-)?  # the source
(?P<metric>.*) # the metric
")
            .unwrap();
        let caps = re.captures("foo.bar.baz").unwrap();
        assert_eq!(None, caps.name("source"));
        assert_eq!(Some("foo.bar.baz"), caps.name("metric"));
    }

    #[test]
    fn test_format_librato_buckets_no_timers() {
        let buckets = make_buckets();
        let librato = Librato::new("user", "token", "test-src", "http://librato.example.com");
        let result = librato.format_stats(&buckets, Some(10101));

        assert_eq!("{\"counters\":[{\"name\":\"cernan.bad_messages\",\"value\":0.0},{\"name\":\
                    \"cernan.total_messages\",\"value\":6.0},{\"name\":\"test.counter\",\
                    \"value\":1.0}],\"guages\":[{\"name\":\"test.gauge\",\"value\":3.211},\
                    {\"name\":\"test.gauge.2\",\"source\":\"src\",\"value\":3.211},{\"name\":\
                    \"test.timer.min\",\"value\":1.1},{\"name\":\"test.timer.max\",\"value\":12.\
                    1},{\"name\":\"test.timer.mean\",\"value\":5.43},{\"name\":\"test.timer.50\",\
                    \"value\":12.1},{\"name\":\"test.timer.90\",\"value\":12.1},{\"name\":\"test.\
                    timer.99\",\"value\":12.1},{\"name\":\"test.timer.999\",\"value\":12.1}],\
                    \"measure_time\":10101,\"source\":\"test-src\"}",
                   result);
    }
}
