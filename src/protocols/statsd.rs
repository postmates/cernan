use metric;
use metric::AggregationMethod;
use std::str::FromStr;
use std::sync;
use time;

/// Valid message formats are:
///
/// - `<str:metric_name>:<f64:value>|<str:type>`
/// - `<str:metric_name>:<f64:value>|c|@<f64:sample_rate>`
/// - `<str:metric_name>:<f64:value>|c@<f64:sample_rate>`
/// - `<str:metric_name>:<f64:value>|g|@<f64:sample_rate>`
/// - `<str:metric_name>:<f64:value>|g@<f64:sample_rate>`
/// p
/// Multiple metrics can be sent in a single UDP packet
/// separated by newlines.
pub fn parse_statsd(
    source: &str,
    res: &mut Vec<metric::Telemetry>,
    metric: sync::Arc<Option<metric::Telemetry>>,
) -> bool {
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
                        let val =
                            match f64::from_str(&src[offset..(offset + pipe_idx)]) {
                                Ok(f) => f,
                                Err(_) => return false,
                            };
                        let mut metric = sync::Arc::make_mut(&mut metric.clone())
                            .take()
                            .unwrap()
                            .thaw();
                        metric = metric.name(name);
                        metric = metric.value(val);
                        metric = metric.timestamp(time::now());
                        let signed = match &src[offset..(offset + 1)] {
                            "+" | "-" => true,
                            _ => false,
                        };
                        offset += pipe_idx + 1;
                        if offset >= len {
                            return false;
                        };
                        metric = match (&src[offset..]).find('@') {
                            Some(sample_idx) => {
                                match &src[offset..(offset + sample_idx)] {
                                    "g|" | "g" => {
                                        let sample = match f64::from_str(
                                            &src[(offset + sample_idx + 1)..],
                                        ) {
                                            Ok(f) => f,
                                            Err(_) => return false,
                                        };
                                        metric = metric.persist(true);
                                        metric = if signed {
                                            metric.kind(AggregationMethod::Sum)
                                        } else {
                                            metric.kind(AggregationMethod::Set)
                                        };
                                        metric.value(val * (1.0 / sample))
                                    }
                                    "c|" | "c" => {
                                        let sample = match f64::from_str(
                                            &src[(offset + sample_idx + 1)..],
                                        ) {
                                            Ok(f) => f,
                                            Err(_) => return false,
                                        };
                                        metric = metric
                                            .kind(AggregationMethod::Sum)
                                            .persist(false);
                                        metric.value(val * (1.0 / sample))
                                    }
                                    "ms" | "ms|" | "h" | "h|" => {
                                        let sample = match f64::from_str(
                                            &src[(offset + sample_idx + 1)..],
                                        ) {
                                            Ok(f) => f,
                                            Err(_) => return false,
                                        };
                                        metric = metric
                                            .kind(AggregationMethod::Summarize)
                                            .persist(false);
                                        metric.value(val * (1.0 / sample))
                                    }
                                    _ => return false,
                                }
                            }
                            None => match &src[offset..] {
                                "g" => {
                                    metric = metric.persist(true);
                                    if signed {
                                        metric.kind(AggregationMethod::Sum)
                                    } else {
                                        metric.kind(AggregationMethod::Set)
                                    }
                                }
                                "ms" | "h" => metric
                                    .kind(AggregationMethod::Summarize)
                                    .persist(false),
                                "c" => {
                                    metric.kind(AggregationMethod::Sum).persist(false)
                                }
                                _ => return false,
                            },
                        };
                        res.push(metric.harden().unwrap());
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
    use super::*;
    use metric::{AggregationMethod, Telemetry};
    use quickcheck::{Arbitrary, Gen, QuickCheck, TestResult};
    use std::sync;

    #[derive(Clone, Debug)]
    enum StatsdAggregation {
        Gauge,
        Counter,
        Timer,
        Histogram,
    }

    impl Arbitrary for StatsdAggregation {
        fn arbitrary<G>(g: &mut G) -> Self
        where
            G: Gen,
        {
            let i: usize = g.gen_range(0, 4);
            match i {
                0 => StatsdAggregation::Gauge,
                1 => StatsdAggregation::Counter,
                2 => StatsdAggregation::Timer,
                _ => StatsdAggregation::Histogram,
            }
        }
    }

    #[derive(Clone, Debug)]
    struct StatsdLine {
        name: String,
        value: f64,
        sampled: bool,
        sample_bar: bool,
        sample_rate: f64,
        newline_terminated: bool,
        aggregation: StatsdAggregation,
    }

    impl Arbitrary for StatsdLine {
        fn arbitrary<G>(g: &mut G) -> Self
        where
            G: Gen,
        {
            let name_len = g.gen_range(1, 256);
            let val: f64 = g.gen();
            let sampled: bool = g.gen();
            let sample_bar: bool = g.gen();
            let sample_rate: f64 = g.gen();
            let newline_terminated: bool = g.gen();
            let aggregation = StatsdAggregation::arbitrary(g);

            let tmp: String = g.gen_ascii_chars().take(name_len).collect();
            StatsdLine {
                name: tmp,
                value: val,
                sampled: sampled,
                sample_bar: sample_bar,
                sample_rate: sample_rate,
                newline_terminated: newline_terminated,
                aggregation: aggregation,
            }
        }
    }

    #[derive(Clone, Debug)]
    struct StatsdPayload {
        lines: Vec<StatsdLine>,
    }

    impl Arbitrary for StatsdPayload {
        fn arbitrary<G>(g: &mut G) -> Self
        where
            G: Gen,
        {
            StatsdPayload {
                lines: Arbitrary::arbitrary(g),
            }
        }
    }

    fn payload_to_str(pyld: &StatsdPayload) -> String {
        let mut pyld_buf = String::with_capacity(1_024);

        let max = pyld.lines.len();
        if max == 0 {
            return "".to_string();
        }
        assert!(max != 0);
        for line in &pyld.lines[0..max - 1] {
            pyld_buf.push_str(&line.name.replace(":", "."));
            pyld_buf.push_str(":");
            pyld_buf.push_str(&line.value.to_string());
            pyld_buf.push_str("|");
            match line.aggregation {
                StatsdAggregation::Gauge => pyld_buf.push_str("g"),
                StatsdAggregation::Counter => pyld_buf.push_str("c"),
                StatsdAggregation::Timer => pyld_buf.push_str("ms"),
                StatsdAggregation::Histogram => pyld_buf.push_str("h"),
            };
            if line.sampled {
                if line.sample_bar {
                    pyld_buf.push_str("|@");
                } else {
                    pyld_buf.push_str("@");
                }
                pyld_buf.push_str(&line.sample_rate.to_string());
            }
            pyld_buf.push_str("\n");
        }
        let line = &pyld.lines[max - 1];
        pyld_buf.push_str(&line.name.replace(":", "."));
        pyld_buf.push_str(":");
        pyld_buf.push_str(&line.value.to_string());
        pyld_buf.push_str("|");
        match line.aggregation {
            StatsdAggregation::Gauge => pyld_buf.push_str("g"),
            StatsdAggregation::Counter => pyld_buf.push_str("c"),
            StatsdAggregation::Timer => pyld_buf.push_str("ms"),
            StatsdAggregation::Histogram => pyld_buf.push_str("h"),
        };
        if line.sampled {
            if line.sample_bar {
                pyld_buf.push_str("|@");
            } else {
                pyld_buf.push_str("@");
            }
            pyld_buf.push_str(&line.sample_rate.to_string());
        }
        if line.newline_terminated {
            pyld_buf.push_str("\n");
        }
        pyld_buf
    }

    #[test]
    fn test_parse_qc() {
        fn inner(pyld: StatsdPayload) -> TestResult {
            if pyld.lines.is_empty() {
                return TestResult::discard();
            }
            let lines = payload_to_str(&pyld);
            let metric = sync::Arc::new(Some(Telemetry::default()));
            let mut res = Vec::new();

            if parse_statsd(&lines, &mut res, metric) {
                assert_eq!(res.len(), pyld.lines.len());
                for (sline, telem) in pyld.lines.iter().zip(res.iter()) {
                    assert_eq!(sline.name, telem.name);
                    if sline.sampled {
                        assert!(
                            (sline.value * (1.0 / sline.sample_rate) -
                                telem.value().unwrap())
                                .abs() < 0.0001
                        );
                    } else {
                        assert!((sline.value - telem.value().unwrap()).abs() < 0.0001);
                    }
                    match sline.aggregation {
                        StatsdAggregation::Counter => {
                            assert_eq!(telem.kind(), AggregationMethod::Sum);
                            assert_eq!(telem.persist, false);
                        }
                        StatsdAggregation::Gauge => {
                            assert_eq!(telem.kind(), AggregationMethod::Set);
                            assert_eq!(telem.persist, true);
                        }
                        StatsdAggregation::Timer => {
                            assert_eq!(telem.kind(), AggregationMethod::Summarize);
                            assert_eq!(telem.persist, false);
                        }
                        StatsdAggregation::Histogram => {
                            assert_eq!(telem.kind(), AggregationMethod::Summarize);
                            assert_eq!(telem.persist, false);
                        }
                    }
                }
                TestResult::passed()
            } else {
                TestResult::failed()
            }
        }
        QuickCheck::new()
            .tests(1000)
            .max_tests(10000)
            .quickcheck(inner as fn(StatsdPayload) -> TestResult);
    }

    #[test]
    fn test_counter() {
        let metric = sync::Arc::new(Some(Telemetry::default()));
        let mut res = Vec::new();
        assert!(parse_statsd(
            "a.b:3.1|c\na-b:4|c|@0.1\na-b:5.2|c@0.2\n",
            &mut res,
            metric
        ));
        assert_eq!(res[0].kind(), AggregationMethod::Sum);
        assert_eq!(res[0].name, "a.b");
        assert_eq!(res[0].persist, false);
        assert_eq!(Some(3.1), res[0].value());
        assert_eq!(res[1].kind(), AggregationMethod::Sum);
        assert_eq!(res[1].name, "a-b");
        assert_eq!(res[1].persist, false);
        assert_eq!(Some(40.0), res[1].value());
        assert_eq!(res[2].kind(), AggregationMethod::Sum);
        assert_eq!(res[2].name, "a-b");
        assert_eq!(res[2].persist, false);
        assert_eq!(Some(26.0), res[2].value());
    }

    #[test]
    fn test_parse_negative_timer() {
        let metric = sync::Arc::new(Some(Telemetry::default()));
        let mut res = Vec::new();
        assert!(parse_statsd("fst:-1.1|ms\n", &mut res, metric));

        assert_eq!(res[0].kind(), AggregationMethod::Summarize);
        assert_eq!(res[0].name, "fst");
        assert_eq!(res[0].persist, false);
        assert_eq!(res[0].query(1.0), Some(-1.1));
    }

    #[test]
    fn test_metric_equal_in_name() {
        let metric = sync::Arc::new(Some(Telemetry::default()));
        let mut res = Vec::new();
        assert!(parse_statsd("A=:1|ms\n", &mut res, metric));

        assert_eq!(res[0].kind(), AggregationMethod::Summarize);
        assert_eq!(res[0].name, "A=");
        assert_eq!(res[0].persist, false);
        assert_eq!(Some(1.0), res[0].query(1.0));
    }

    #[test]
    fn test_metric_slash_in_name() {
        let metric = sync::Arc::new(Some(Telemetry::default()));
        let mut res = Vec::new();
        assert!(parse_statsd("A/:1|ms\n", &mut res, metric));

        assert_eq!(res[0].kind(), AggregationMethod::Summarize);
        assert_eq!(res[0].name, "A/");
        assert_eq!(res[0].persist, false);
        assert_eq!(Some(1.0), res[0].query(1.0));
    }

    #[test]
    fn test_metric_sample_gauge() {
        let metric = sync::Arc::new(Some(Telemetry::default()));
        let mut res = Vec::new();
        assert!(parse_statsd(
            "foo:1|g|@+0.22\nbar:101|g|@2\nbaz:2|g@0.2\nqux:4|g@0.1",
            &mut res,
            metric
        ));
        //                              0         A     F
        assert_eq!(res[0].kind(), AggregationMethod::Set);
        assert_eq!(res[0].name, "foo");
        assert_eq!(res[0].persist, true);
        assert_eq!(Some(1.0 * (1.0 / 0.22)), res[0].query(1.0));

        assert_eq!(res[1].kind(), AggregationMethod::Set);
        assert_eq!(res[1].name, "bar");
        assert_eq!(res[1].persist, true);
        assert_eq!(Some(101.0 * (1.0 / 2.0)), res[1].query(1.0));

        assert_eq!(res[2].kind(), AggregationMethod::Set);
        assert_eq!(res[2].name, "baz");
        assert_eq!(res[2].persist, true);
        assert_eq!(Some(2.0 * (1.0 / 0.2)), res[2].query(1.0));

        assert_eq!(res[3].kind(), AggregationMethod::Set);
        assert_eq!(res[3].name, "qux");
        assert_eq!(res[3].persist, true);
        assert_eq!(Some(4.0 * (1.0 / 0.1)), res[3].query(1.0));
    }

    #[test]
    fn test_metric_parse_invalid_no_name() {
        let metric = sync::Arc::new(Some(Telemetry::default()));
        let mut res = Vec::new();
        assert!(!parse_statsd("", &mut res, metric));
    }


    #[test]
    fn test_metric_parse_invalid_no_value() {
        let metric = sync::Arc::new(Some(Telemetry::default()));
        let mut res = Vec::new();
        assert!(!parse_statsd("foo:", &mut res, metric));
    }

    #[test]
    fn test_metric_multiple() {
        let metric = sync::Arc::new(Some(Telemetry::default()));
        let mut res = Vec::new();
        assert!(parse_statsd("a.b:12.1|g\nb_c:13.2|c\n", &mut res, metric));
        assert_eq!(2, res.len());

        assert_eq!(res[0].kind(), AggregationMethod::Set);
        assert_eq!(res[0].name, "a.b");
        assert_eq!(res[0].persist, true);
        assert_eq!(Some(12.1), res[0].value());

        assert_eq!(res[1].kind(), AggregationMethod::Sum);
        assert_eq!(res[1].name, "b_c");
        assert_eq!(res[1].persist, false);
        assert_eq!(Some(13.2), res[1].value());
    }

    #[test]
    fn test_metric_optional_final_newline() {
        let metric = sync::Arc::new(Some(Telemetry::default()));
        let mut res = Vec::new();
        assert!(parse_statsd("a.b:12.1|g\nb_c:13.2|c", &mut res, metric));
        assert_eq!(2, res.len());

        assert_eq!(res[0].kind(), AggregationMethod::Set);
        assert_eq!(res[0].name, "a.b");
        assert_eq!(res[0].persist, true);
        assert_eq!(Some(12.1), res[0].value());

        assert_eq!(res[1].kind(), AggregationMethod::Sum);
        assert_eq!(res[1].name, "b_c");
        assert_eq!(res[1].persist, false);
        assert_eq!(Some(13.2), res[1].value());
    }

    #[test]
    fn test_solo_negative_gauge_as_ephemeral_set() {
        let pyld = "zrth:-1|g\n";
        let metric = sync::Arc::new(Some(Telemetry::default()));
        let mut res = Vec::new();
        assert!(parse_statsd(pyld, &mut res, metric));

        assert_eq!(res[0].kind(), AggregationMethod::Sum);
        assert_eq!(res[0].name, "zrth");
        assert_eq!(res[0].persist, true);
        assert_eq!(res[0].value(), Some(-1.0));
    }

    #[test]
    fn test_multi_gauge_as_persist_sum() {
        let pyld = "zrth:0|g\nzrth:-1|g\n";
        let metric = sync::Arc::new(Some(Telemetry::default()));
        let mut res = Vec::new();
        assert!(parse_statsd(pyld, &mut res, metric));

        assert_eq!(res[0].kind(), AggregationMethod::Set);
        assert_eq!(res[0].name, "zrth");
        assert_eq!(res[0].persist, true);
        assert_eq!(res[0].value(), Some(0.0));

        assert_eq!(res[1].kind(), AggregationMethod::Sum);
        assert_eq!(res[1].name, "zrth");
        assert_eq!(res[1].persist, true);
        assert_eq!(res[1].value(), Some(-1.0));
    }

    #[test]
    fn test_metric_invalid() {
        let invalid = vec![
            "",
            "metric",
            "metric|11:",
            "metric|12",
            "metric:13|",
            ":|@",
            ":1.0|c",
        ];
        let metric = sync::Arc::new(Some(Telemetry::default()));
        for input in invalid.iter() {
            assert!(!parse_statsd(*input, &mut Vec::new(), metric.clone()));
        }
    }

    #[test]
    fn test_parse_metric_via_api() {
        let pyld = "zrth:0|g\nfst:-1.1|ms\nsnd:+2.2|g\nthd:3.3|h\nfth:4|c\nfvth:5.5|c|@0.1\nsxth:\
                    -6.6|g\nsvth:+7.77|g\n";
        let metric = sync::Arc::new(Some(Telemetry::default()));
        let mut res = Vec::new();
        assert!(parse_statsd(pyld, &mut res, metric));

        assert_eq!(res[0].kind(), AggregationMethod::Set);
        assert_eq!(res[0].name, "zrth");
        assert_eq!(res[0].persist, true);
        assert_eq!(res[0].value(), Some(0.0));

        assert_eq!(res[1].kind(), AggregationMethod::Summarize);
        assert_eq!(res[1].name, "fst");
        assert_eq!(res[1].persist, false);
        assert_eq!(res[1].query(1.0), Some(-1.1));

        assert_eq!(res[2].kind(), AggregationMethod::Sum);
        assert_eq!(res[2].name, "snd");
        assert_eq!(res[2].persist, true);
        assert_eq!(res[2].value(), Some(2.2));

        assert_eq!(res[3].kind(), AggregationMethod::Summarize);
        assert_eq!(res[3].name, "thd");
        assert_eq!(res[3].persist, false);
        assert_eq!(res[3].query(1.0), Some(3.3));

        assert_eq!(res[4].kind(), AggregationMethod::Sum);
        assert_eq!(res[4].name, "fth");
        assert_eq!(res[4].persist, false);
        assert_eq!(res[4].value(), Some(4.0));

        assert_eq!(res[5].kind(), AggregationMethod::Sum);
        assert_eq!(res[5].name, "fvth");
        assert_eq!(res[5].persist, false);
        assert_eq!(res[5].value(), Some(55.0));

        assert_eq!(res[6].kind(), AggregationMethod::Sum);
        assert_eq!(res[6].name, "sxth");
        assert_eq!(res[6].persist, true);
        assert_eq!(res[6].value(), Some(-6.6));

        assert_eq!(res[7].kind(), AggregationMethod::Sum);
        assert_eq!(res[7].name, "svth");
        assert_eq!(res[7].persist, true);
        assert_eq!(res[7].value(), Some(7.77));
    }
}
