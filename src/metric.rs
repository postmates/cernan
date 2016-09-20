use metrics::*;
use chrono::UTC;
use std::sync::Arc;
use string_cache::Atom;

#[derive(PartialEq, Clone, Debug)]
pub struct MetricQOS {
    pub counter: u64,
    pub gauge: u64,
    pub timer: u64,
    pub histogram: u64,
    pub raw: u64,
}

impl MetricQOS {
    pub fn default() -> MetricQOS {
        MetricQOS {
            counter: 1,
            gauge: 1,
            timer: 1,
            histogram: 1,
            raw: 1,
        }
    }
}

#[derive(PartialEq, Debug, Clone)]
pub enum MetricKind {
    Counter(f64),
    Gauge,
    DeltaGauge,
    Timer,
    Histogram,
    Raw,
}

#[derive(PartialEq, Debug)]
pub enum MetricSign {
    Positive,
    Negative,
}

#[derive(PartialEq, Debug, Clone)]
pub struct Metric {
    pub kind: MetricKind,
    pub name: Atom,
    pub value: f64,
    pub time: i64,
}

impl Metric {
    pub fn counter(name: &str) -> Metric {
        Metric {
            name: Atom::from(name),
            value: 1.0,
            kind: MetricKind::Counter(1.0),
            time: UTC::now().timestamp(),
        }
    }

    /// Create a new metric
    ///
    /// Uses the Into trait to allow both str and String types.
    pub fn new(name: Atom, raw_value: f64, raw_kind: MetricKind, sign: Option<MetricSign>) -> Metric {
        let kind = match raw_kind {
            MetricKind::Gauge => {
                match sign {
                    Some(MetricSign::Positive) => MetricKind::DeltaGauge,
                    Some(MetricSign::Negative) => MetricKind::DeltaGauge,
                    None => raw_kind,
                }
            },
            _ => raw_kind,
        };

        let value = match sign {
            None => raw_value,
            Some(MetricSign::Positive) => raw_value,
            Some(MetricSign::Negative) => -1.0 * raw_value,
        };

        Metric {
            name: name,
            value: value,
            kind: kind,
            time: UTC::now().timestamp(),
        }
    }

    pub fn new_with_time(name: Atom, value: f64, time: Option<i64>, kind: MetricKind) -> Metric {
        Metric {
            name: name,
            value: value,
            kind: kind,
            time: time.unwrap_or(UTC::now().timestamp()),
        }
    }


    /// Valid message formats are:
    ///
    /// - `<str:metric_name>:<f64:value>|<str:type>`
    /// - `<str:metric_name>:<f64:value>|c|@<f64:sample_rate>`
    ///
    /// Multiple metrics can be sent in a single UDP packet
    /// separated by newlines.
    pub fn parse_statsd(source: &str) -> Option<Vec<Arc<Metric>>> {
        statsd::parse_MetricPayload(source).ok()
    }

    pub fn parse_graphite(source: &str) -> Option<Vec<Arc<Metric>>> {
        graphite::parse_MetricPayload(source).ok()
    }
}

#[cfg(test)]
mod tests {
    use metric::{Metric, MetricKind};
    use string_cache::Atom;
    use chrono::{UTC, TimeZone};
    //    use test::Bencher; // see bench_prs

    #[test]
    fn test_parse_graphite() {
        let pyld = "fst 1 101\nsnd -2.0 202\nthr 3 303\nfth@fth 4 404\nfv%fv 5 505\ns-th 6 606\n";
        let prs = Metric::parse_graphite(pyld);

        assert!(prs.is_some());
        let prs_pyld = prs.unwrap();

        assert_eq!(prs_pyld[0].kind, MetricKind::Raw);
        assert_eq!(prs_pyld[0].name, Atom::from("fst"));
        assert_eq!(prs_pyld[0].value, 1.0);
        assert_eq!(prs_pyld[0].time, UTC.timestamp(101, 0).timestamp());

        assert_eq!(prs_pyld[1].kind, MetricKind::Raw);
        assert_eq!(prs_pyld[1].name, Atom::from("snd"));
        assert_eq!(prs_pyld[1].value, -2.0);
        assert_eq!(prs_pyld[1].time, UTC.timestamp(202, 0).timestamp());

        assert_eq!(prs_pyld[2].kind, MetricKind::Raw);
        assert_eq!(prs_pyld[2].name, Atom::from("thr"));
        assert_eq!(prs_pyld[2].value, 3.0);
        assert_eq!(prs_pyld[2].time, UTC.timestamp(303, 0).timestamp());

        assert_eq!(prs_pyld[3].kind, MetricKind::Raw);
        assert_eq!(prs_pyld[3].name, Atom::from("fth@fth"));
        assert_eq!(prs_pyld[3].value, 4.0);
        assert_eq!(prs_pyld[3].time, UTC.timestamp(404, 0).timestamp());

        assert_eq!(prs_pyld[4].kind, MetricKind::Raw);
        assert_eq!(prs_pyld[4].name, Atom::from("fv%fv"));
        assert_eq!(prs_pyld[4].value, 5.0);
        assert_eq!(prs_pyld[4].time, UTC.timestamp(505, 0).timestamp());

        assert_eq!(prs_pyld[5].kind, MetricKind::Raw);
        assert_eq!(prs_pyld[5].name, Atom::from("s-th"));
        assert_eq!(prs_pyld[5].value, 6.0);
        assert_eq!(prs_pyld[5].time, UTC.timestamp(606, 0).timestamp());
    }

    #[test]
    fn test_parse_metric_via_api() {
        let pyld = "zrth:0|g\nfst:-1.1|ms\nsnd:+2.2|g\nthd:3.3|h\nfth:4|c\nfvth:5.5|c@2\nsxth:-6.6|g\nsvth:+7.77|g";
        let prs = Metric::parse_statsd(pyld);

        assert!(prs.is_some());
        let prs_pyld = prs.unwrap();

        assert_eq!(prs_pyld[0].kind, MetricKind::Gauge);
        assert_eq!(prs_pyld[0].name, Atom::from("zrth"));
        assert_eq!(prs_pyld[0].value, 0.0);

        assert_eq!(prs_pyld[1].kind, MetricKind::Timer);
        assert_eq!(prs_pyld[1].name, Atom::from("fst"));
        assert_eq!(prs_pyld[1].value, -1.1);

        assert_eq!(prs_pyld[2].kind, MetricKind::DeltaGauge);
        assert_eq!(prs_pyld[2].name, Atom::from("snd"));
        assert_eq!(prs_pyld[2].value, 2.2);

        assert_eq!(prs_pyld[3].kind, MetricKind::Histogram);
        assert_eq!(prs_pyld[3].name, Atom::from("thd"));
        assert_eq!(prs_pyld[3].value, 3.3);

        assert_eq!(prs_pyld[4].kind, MetricKind::Counter(1.0));
        assert_eq!(prs_pyld[4].name, Atom::from("fth"));
        assert_eq!(prs_pyld[4].value, 4.0);

        assert_eq!(prs_pyld[5].kind, MetricKind::Counter(2.0));
        assert_eq!(prs_pyld[5].name, Atom::from("fvth"));
        assert_eq!(prs_pyld[5].value, 5.5);

        assert_eq!(prs_pyld[6].kind, MetricKind::DeltaGauge);
        assert_eq!(prs_pyld[6].name, Atom::from("sxth"));
        assert_eq!(prs_pyld[6].value, -6.6);

        assert_eq!(prs_pyld[7].kind, MetricKind::DeltaGauge);
        assert_eq!(prs_pyld[7].name, Atom::from("svth"));
        assert_eq!(prs_pyld[7].value, 7.77);
    }

    #[test]
    fn test_metric_equal_in_name() {
        let res = Metric::parse_statsd("A=:1|ms\n").unwrap();

        assert_eq!(Atom::from("A="), res[0].name);
        assert_eq!(1.0, res[0].value);
        assert_eq!(MetricKind::Timer, res[0].kind);
    }

    #[test]
    fn test_metric_slash_in_name() {
        let res = Metric::parse_statsd("A/:1|ms\n").unwrap();

        assert_eq!(Atom::from("A/"), res[0].name);
        assert_eq!(1.0, res[0].value);
        assert_eq!(MetricKind::Timer, res[0].kind);
    }

    #[test]
    fn test_metric_sample_gauge() {
        let res = Metric::parse_statsd("foo:1|g@0.22\nbar:101|g@2\n").unwrap();

        assert_eq!(Atom::from("foo"), res[0].name);
        assert_eq!(1.0, res[0].value);
        assert_eq!(MetricKind::Gauge, res[0].kind);

        assert_eq!(Atom::from("bar"), res[1].name);
        assert_eq!(101.0, res[1].value);
        assert_eq!(MetricKind::Gauge, res[1].kind);
    }

    #[test]
    fn test_metric_parse_invalid_no_name() {
        assert_eq!(None, Metric::parse_statsd(""));
    }

    #[test]
    fn test_metric_parse_invalid_no_value() {
        assert_eq!(None, Metric::parse_statsd("foo:"));
    }

    #[test]
    fn test_metric_multiple() {
        let res = Metric::parse_statsd("a.b:12.1|g\nb_c:13.2|c").unwrap();
        assert_eq!(2, res.len());

        assert_eq!(Atom::from("a.b"), res[0].name);
        assert_eq!(12.1, res[0].value);

        assert_eq!(Atom::from("b_c"), res[1].name);
        assert_eq!(13.2, res[1].value);
    }

    #[test]
    fn test_metric_invalid() {
        let invalid = vec!["", "metric", "metric|11:", "metric|12", "metric:13|", ":|@", ":1.0|c"];
        for input in invalid.iter() {
            let result = Metric::parse_statsd(*input);
            assert!(result.is_none());
        }
    }
}
