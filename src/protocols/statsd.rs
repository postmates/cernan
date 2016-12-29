use metric;
use std::str::FromStr;
use std::sync;
use time;

/// Valid message formats are:
///
/// - `<str:metric_name>:<f64:value>|<str:type>`
/// - `<str:metric_name>:<f64:value>|c|@<f64:sample_rate>`
/// p
/// Multiple metrics can be sent in a single UDP packet
/// separated by newlines.
pub fn parse_statsd(source: &str,
                    res: &mut Vec<metric::Metric>,
                    metric: sync::Arc<Option<metric::Metric>>)
                    -> bool {
    for src in source.lines() {
        let mut offset = 0;
        let len = src.len();
        match (&src[offset..]).find(':') {
            Some(colon_idx) => {
                let name = &src[offset..(offset + colon_idx)];
                if name.is_empty() {
                    return false;
                };
                offset += colon_idx + 1;
                if offset >= len {
                    return false;
                };
                match (&src[offset..]).find('|') {
                    Some(pipe_idx) => {
                        let val = match f64::from_str(&src[offset..(offset + pipe_idx)]) {
                            Ok(f) => f,
                            Err(_) => return false,
                        };
                        let mut metric = sync::Arc::make_mut(&mut metric.clone()).take().unwrap();
                        metric = metric.set_name(name);
                        metric = metric.set_value(val);
                        metric = metric.time(time::now());
                        metric = match &src[offset..(offset + 1)] {
                            "+" | "-" => metric.delta_gauge(),
                            _ => metric,
                        };
                        offset += pipe_idx + 1;
                        if offset >= len {
                            return false;
                        };
                        metric = match (&src[offset..]).find('@') {
                            Some(sample_idx) => {
                                match &src[offset..(offset + sample_idx)] {
                                    "g" => metric.gauge(),
                                    "ms" => metric.timer(),
                                    "h" => metric.histogram(),
                                    "c" => {
                                        let sample = match f64::from_str(&src[(offset + sample_idx +
                                                                               1)..]) {
                                            Ok(f) => f,
                                            Err(_) => return false,
                                        };
                                        metric = metric.counter();
                                        metric.set_value(val * (1.0 / sample))
                                    }
                                    _ => return false,
                                }
                            }
                            None => {
                                match &src[offset..] {
                                    "g" | "g\n" => metric.gauge(),
                                    "ms" | "ms\n" => metric.timer(),
                                    "h" | "h\n" => metric.histogram(),
                                    "c" | "c\n" => metric.counter(),
                                    _ => return false,
                                }
                            }
                        };

                        res.push(metric);
                    }
                    None => return false,
                }
            }
            None => return false,
        }
    }
    !res.is_empty()
}

#[cfg(test)]
mod tests {
    use metric::{Metric, MetricKind};
    use std::sync;
    use super::*;

    #[test]
    fn test_parse_negative_timer() {
        let metric = sync::Arc::new(Some(Metric::default()));
        let mut res = Vec::new();
        assert!(parse_statsd("fst:-1.1|ms\n", &mut res, metric));

        assert_eq!(res[0].kind, MetricKind::Timer);
        assert_eq!(res[0].name, "fst");
        assert_eq!(res[0].query(1.0), Some(-1.1));
    }

    #[test]
    fn test_metric_equal_in_name() {
        let metric = sync::Arc::new(Some(Metric::default()));
        let mut res = Vec::new();
        assert!(parse_statsd("A=:1|ms\n", &mut res, metric));

        assert_eq!("A=", res[0].name);
        assert_eq!(Some(1.0), res[0].query(1.0));
        assert_eq!(MetricKind::Timer, res[0].kind);
    }

    #[test]
    fn test_metric_slash_in_name() {
        let metric = sync::Arc::new(Some(Metric::default()));
        let mut res = Vec::new();
        assert!(parse_statsd("A/:1|ms\n", &mut res, metric));

        assert_eq!("A/", res[0].name);
        assert_eq!(Some(1.0), res[0].query(1.0));
        assert_eq!(MetricKind::Timer, res[0].kind);
    }

    #[test]
    fn test_metric_sample_gauge() {
        let metric = sync::Arc::new(Some(Metric::default()));
        let mut res = Vec::new();
        assert!(parse_statsd("foo:1|g@0.22\nbar:101|g@2\n", &mut res, metric));
        //                              0         A     F
        assert_eq!("foo", res[0].name);
        assert_eq!(Some(1.0), res[0].query(1.0));
        assert_eq!(MetricKind::Gauge, res[0].kind);

        assert_eq!("bar", res[1].name);
        assert_eq!(Some(101.0), res[1].query(1.0));
        assert_eq!(MetricKind::Gauge, res[1].kind);
    }

    #[test]
    fn test_metric_parse_invalid_no_name() {
        let metric = sync::Arc::new(Some(Metric::default()));
        let mut res = Vec::new();
        assert!(!parse_statsd("", &mut res, metric));
    }


    #[test]
    fn test_metric_parse_invalid_no_value() {
        let metric = sync::Arc::new(Some(Metric::default()));
        let mut res = Vec::new();
        assert!(!parse_statsd("foo:", &mut res, metric));
    }

    #[test]
    fn test_metric_multiple() {
        let metric = sync::Arc::new(Some(Metric::default()));
        let mut res = Vec::new();
        assert!(parse_statsd("a.b:12.1|g\nb_c:13.2|c\n", &mut res, metric));
        assert_eq!(2, res.len());

        assert_eq!("a.b", res[0].name);
        assert_eq!(Some(12.1), res[0].value());

        assert_eq!("b_c", res[1].name);
        assert_eq!(Some(13.2), res[1].value());
    }

    #[test]
    fn test_metric_optional_final_newline() {
        let metric = sync::Arc::new(Some(Metric::default()));
        let mut res = Vec::new();
        assert!(parse_statsd("a.b:12.1|g\nb_c:13.2|c", &mut res, metric));
        assert_eq!(2, res.len());

        assert_eq!("a.b", res[0].name);
        assert_eq!(Some(12.1), res[0].value());

        assert_eq!("b_c", res[1].name);
        assert_eq!(Some(13.2), res[1].value());
    }

    #[test]
    fn test_metric_invalid() {
        let invalid = vec!["", "metric", "metric|11:", "metric|12", "metric:13|", ":|@", ":1.0|c"];
        let metric = sync::Arc::new(Some(Metric::default()));
        for input in invalid.iter() {
            assert!(!parse_statsd(*input, &mut Vec::new(), metric.clone()));
        }
    }

    #[test]
    fn test_parse_metric_via_api() {
        let pyld = "zrth:0|g\nfst:-1.1|ms\nsnd:+2.2|g\nthd:3.3|h\nfth:4|c\nfvth:5.5|c@0.1\nsxth:\
                    -6.6|g\nsvth:+7.77|g\n";
        let metric = sync::Arc::new(Some(Metric::default()));
        let mut res = Vec::new();
        assert!(parse_statsd(pyld, &mut res, metric));

        assert_eq!(res[0].kind, MetricKind::Gauge);
        assert_eq!(res[0].name, "zrth");
        assert_eq!(res[0].value(), Some(0.0));

        assert_eq!(res[1].kind, MetricKind::Timer);
        assert_eq!(res[1].name, "fst");
        assert_eq!(res[1].query(1.0), Some(-1.1));

        assert_eq!(res[2].kind, MetricKind::DeltaGauge);
        assert_eq!(res[2].name, "snd");
        assert_eq!(res[2].value(), Some(2.2));

        assert_eq!(res[3].kind, MetricKind::Histogram);
        assert_eq!(res[3].name, "thd");
        assert_eq!(res[3].query(1.0), Some(3.3));

        assert_eq!(res[4].kind, MetricKind::Counter);
        assert_eq!(res[4].name, "fth");
        assert_eq!(res[4].value(), Some(4.0));

        assert_eq!(res[5].kind, MetricKind::Counter);
        assert_eq!(res[5].name, "fvth");
        assert_eq!(res[5].value(), Some(55.0));

        assert_eq!(res[6].kind, MetricKind::DeltaGauge);
        assert_eq!(res[6].name, "sxth");
        assert_eq!(res[6].value(), Some(-6.6));

        assert_eq!(res[7].kind, MetricKind::DeltaGauge);
        assert_eq!(res[7].name, "svth");
        assert_eq!(res[7].value(), Some(7.77));
    }
}
