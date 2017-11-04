//! The prometheus text protocol parser. Defined here:
//! https://prometheus.
//! io/docs/instrumenting/exposition_formats/#text-format-details

use metric;
use std::sync;

/// Parse a prometheus text blob
pub fn parse(
    _source: &str,
    _res: &mut Vec<metric::Telemetry>,
    _template: &sync::Arc<Option<metric::Telemetry>>,
) -> bool {
    unimplemented!()
}

#[cfg(test)]
mod tests {
    use super::*;
    use metric::{AggregationMethod, Telemetry};
    use quickcheck::{Arbitrary, Gen, QuickCheck, TestResult};
    use std::collections::HashMap;
    use std::f64;

    #[derive(Clone, Debug)]
    enum GoFloat {
        NaN,
        PosInf,
        NegInf,
        Num(f64),
    }

    impl Arbitrary for GoFloat {
        fn arbitrary<G>(g: &mut G) -> Self
        where
            G: Gen,
        {
            let i: usize = g.gen_range(0, 4);
            match i {
                0 => GoFloat::NaN,
                1 => GoFloat::PosInf,
                2 => GoFloat::NegInf,
                _ => GoFloat::Num(Arbitrary::arbitrary(g)),
            }
        }
    }

    #[derive(Clone, Debug)]
    struct SummaryPair {
        quantile: GoFloat,
        value: GoFloat,
    }

    impl Arbitrary for SummaryPair {
        fn arbitrary<G>(g: &mut G) -> Self
        where
            G: Gen,
        {
            SummaryPair {
                quantile: Arbitrary::arbitrary(g),
                value: Arbitrary::arbitrary(g),
            }
        }
    }

    #[derive(Clone, Debug)]
    struct HistogramPair {
        le_bound: GoFloat,
        value: GoFloat,
    }

    impl Arbitrary for HistogramPair {
        fn arbitrary<G>(g: &mut G) -> Self
        where
            G: Gen,
        {
            HistogramPair {
                le_bound: Arbitrary::arbitrary(g),
                value: Arbitrary::arbitrary(g),
            }
        }
    }

    #[derive(Clone, Debug)]
    enum PrometheusAggregation {
        Counter(GoFloat),
        Gauge(GoFloat),
        // Histogram { pairs: Vec<HistogramPair>, sum: GoFloat, count: GoFloat },
        // Summary { pairs: Vec<SummaryPair>, sum: GoFloat, count: GoFloat },
        Untyped(GoFloat),
    }

    impl Arbitrary for PrometheusAggregation {
        fn arbitrary<G>(g: &mut G) -> Self
        where
            G: Gen,
        {
            let i: usize = g.gen_range(0, 5);
            match i {
                0 => PrometheusAggregation::Counter(Arbitrary::arbitrary(g)),
                1 => PrometheusAggregation::Gauge(Arbitrary::arbitrary(g)),
                // 2 => PrometheusAggregation::Histogram { pairs:
                // Arbitrary::arbitrary(g),
                // sum:
                // Arbitrary::arbitrary(g),
                // count:
                // Arbitrary::arbitrary(g),
                // },
                // 3 => PrometheusAggregation::Summary { pairs:
                // Arbitrary::arbitrary(g),
                //                                       sum: Arbitrary::arbitrary(g),
                // count:
                // Arbitrary::arbitrary(g),
                // },
                _ => PrometheusAggregation::Untyped(Arbitrary::arbitrary(g)),
            }
        }
    }

    #[derive(Clone, Debug)]
    struct PrometheusBlock {
        name: String,
        description: Option<String>,
        labels: Option<HashMap<String, String>>,
        label_trailing_comma: bool,
        aggregation: PrometheusAggregation,
        timestamp: Option<u32>,
    }

    impl Arbitrary for PrometheusBlock {
        fn arbitrary<G>(g: &mut G) -> Self
        where
            G: Gen,
        {
            let name_len = g.gen_range(1, 256);
            let descr_len = g.gen_range(1, 256);

            let desc = if g.gen() {
                Some(g.gen_ascii_chars().take(descr_len).collect())
            } else {
                None
            };

            PrometheusBlock {
                name: g.gen_ascii_chars().take(name_len).collect(),
                description: desc,
                labels: Arbitrary::arbitrary(g),
                label_trailing_comma: Arbitrary::arbitrary(g),
                aggregation: Arbitrary::arbitrary(g),
                timestamp: Arbitrary::arbitrary(g),
            }
        }
    }

    #[derive(Clone, Debug)]
    struct PrometheusPayload {
        blocks: Vec<PrometheusBlock>,
    }

    impl Arbitrary for PrometheusPayload {
        fn arbitrary<G>(g: &mut G) -> Self
        where
            G: Gen,
        {
            PrometheusPayload {
                blocks: Arbitrary::arbitrary(g),
            }
        }
    }

    fn labels_to_buf(
        labels: &Option<HashMap<String, String>>,
        label_trailing_comma: bool,
        buf: &mut String,
    ) {
        if let &Some(ref labels) = labels {
            let mut iter = labels.iter();
            if let Some(label) = iter.next() {
                buf.push_str("{");
                label_to_buf(label, buf);
            }
            for label in labels {
                label_to_buf(label, buf);
            }
            if label_trailing_comma {
                buf.push_str(",");
            }
            buf.push_str("}");
        }
    }

    fn timestamp_to_buf(ts: Option<u32>, buf: &mut String) -> () {
        if let Some(t) = ts {
            buf.push_str(&t.to_string());
        }
    }

    fn go_float_to_buf(flt: &GoFloat, buf: &mut String) -> () {
        match *flt {
            GoFloat::NaN => buf.push_str("Nan"),
            GoFloat::PosInf => buf.push_str("+Inf"),
            GoFloat::NegInf => buf.push_str("-Inf"),
            GoFloat::Num(f) => buf.push_str(&f.to_string()),
        }
    }

    fn label_to_buf(label: (&String, &String), buf: &mut String) -> () {
        buf.push_str(label.0);
        buf.push_str("=");
        buf.push_str("\"");
        buf.push_str(label.1);
        buf.push_str("\"");
    }

    fn flat_block_to_buf(
        flt: &GoFloat,
        block: &PrometheusBlock,
        mut buf: &mut String,
    ) -> () {
        if let Some(ref descr) = block.description {
            buf.push_str("# HELP ");
            buf.push_str(descr);
            buf.push_str("\n");
        }
        buf.push_str(&block.name);
        buf.push_str(" ");
        labels_to_buf(&block.labels, block.label_trailing_comma, &mut buf);
        buf.push_str(" ");
        go_float_to_buf(&flt, &mut buf);
        buf.push_str(" ");
        timestamp_to_buf(block.timestamp, &mut buf);
        buf.push_str("\n");
    }

    fn payload_to_str(payload: &PrometheusPayload) -> String {
        let mut buf = String::with_capacity(1_024);

        for block in &payload.blocks {
            block_to_str(&block, &mut buf);
            buf.push_str("\n");
        }

        buf
    }

    fn block_to_str(block: &PrometheusBlock, mut buf: &mut String) -> () {
        match block.aggregation {
            PrometheusAggregation::Counter(ref flt) => {
                buf.push_str("# TYPE counter\n");
                flat_block_to_buf(flt, &block, &mut buf);
            }
            PrometheusAggregation::Gauge(ref flt) => {
                buf.push_str("# TYPE gauge\n");
                flat_block_to_buf(flt, &block, &mut buf);
            }
            PrometheusAggregation::Untyped(ref flt) => {
                flat_block_to_buf(flt, &block, &mut buf);
            }
            // PrometheusAggregation::Histogram { ref pairs, ref sum, ref count } => {
            //     buf.push_str("# TYPE histogram\n");
            //     if let Some(ref descr) = block.description {
            //         buf.push_str("# HELP ");
            //         buf.push_str(descr);
            //         buf.push_str("\n");
            //     }
            //     buf.push_str(&format!("{}_sum", &block.name));
            //     buf.push_str(" ");
            //     go_float_to_buf(&sum, &mut buf);
            //     buf.push_str(&format!("{}_count", &block.name));
            //     buf.push_str(" ");
            //     go_float_to_buf(&count, &mut buf);
            //     for pair in pairs {
            //         let mut new_block = block.clone();
            //         let mut bound = String::new();
            //         go_float_to_buf(&pair.le_bound, &mut bound);
            //         if let Some(ref mut labels) = new_block.labels {
            //             labels.insert("le".to_string(), bound);
            //         } else {
            //             let mut labels = HashMap::new();
            //             labels.insert("le".to_string(), bound);
            //             new_block.labels = Some(labels);
            //         }
            //         flat_block_to_buf(&pair.value, &new_block, &mut buf);
            //     }
            // }
            // PrometheusAggregation::Summary { ref pairs, ref sum, ref count } => {
            //     buf.push_str("# TYPE histogram\n");
            //     if let Some(ref descr) = block.description {
            //         buf.push_str("# HELP ");
            //         buf.push_str(descr);
            //         buf.push_str("\n");
            //     }
            //     buf.push_str(&format!("{}_sum", &block.name));
            //     buf.push_str(" ");
            //     go_float_to_buf(&sum, &mut buf);
            //     buf.push_str(&format!("{}_count", &block.name));
            //     buf.push_str(" ");
            //     go_float_to_buf(&count, &mut buf);
            //     for pair in pairs {
            //         let mut new_block = block.clone();
            //         let mut bound = String::new();
            //         go_float_to_buf(&pair.quantile, &mut bound);
            //         if let Some(ref mut labels) = new_block.labels {
            //             labels.insert("quantile".to_string(), bound);
            //         } else {
            //             let mut labels = HashMap::new();
            //             labels.insert("quantile".to_string(), bound);
            //             new_block.labels = Some(labels);
            //         }
            //         flat_block_to_buf(&pair.value, &new_block, &mut buf);
            //     }
            // }
        }
    }

    fn go_float_eq(l: f64, r: &GoFloat) -> bool {
        match *r {
            GoFloat::NaN => l == f64::NAN,
            GoFloat::NegInf => l == f64::NEG_INFINITY,
            GoFloat::PosInf => l == f64::INFINITY,
            GoFloat::Num(f) => (l - f).abs() < 0.00001,
        }
    }

    #[test]
    fn test_parse_qc() {
        fn inner(pyld: PrometheusPayload) -> TestResult {
            if pyld.blocks.is_empty() {
                return TestResult::discard();
            }
            let lines = payload_to_str(&pyld);
            let metric = sync::Arc::new(Some(Telemetry::default()));
            let mut res = Vec::new();

            if parse(&lines, &mut res, &metric) {
                assert_eq!(res.len(), pyld.blocks.len());
                for (sline, telem) in pyld.blocks.iter().zip(res.iter()) {
                    assert_eq!(sline.name, telem.name);
                    match sline.aggregation {
                        PrometheusAggregation::Counter(ref flt) => {
                            assert_eq!(telem.kind(), AggregationMethod::Sum);
                            assert!(go_float_eq(telem.sum().unwrap(), flt));
                            assert_eq!(telem.persist, true);
                        }
                        PrometheusAggregation::Gauge(ref flt) => {
                            assert_eq!(telem.kind(), AggregationMethod::Set);
                            assert!(go_float_eq(telem.set().unwrap(), flt));
                            assert_eq!(telem.persist, true);
                        }
                        // PrometheusAggregation::Histogram { ..  } => {
                        //     unimplemented!();
                        // }
                        // PrometheusAggregation::Summary { ..  } => {
                        //     unimplemented!();
                        // }
                        PrometheusAggregation::Untyped(ref flt) => {
                            assert_eq!(telem.kind(), AggregationMethod::Set);
                            assert!(go_float_eq(telem.set().unwrap(), flt));
                            assert_eq!(telem.persist, true);
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
            .quickcheck(inner as fn(PrometheusPayload) -> TestResult);
    }

}
