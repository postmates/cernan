use metric::Metric;
use std::str::FromStr;
use std::sync;

pub fn parse_graphite(source: &str,
                      res: &mut Vec<Metric>,
                      metric: sync::Arc<Option<Metric>>)
                      -> bool {
    let mut iter = source.split_whitespace();
    while let Some(name) = iter.next() {
        match iter.next() {
            Some(val) => {
                match iter.next() {
                    Some(time) => {
                        let parsed_val = match f64::from_str(val) {
                            Ok(f) => f,
                            Err(_) => return false,
                        };
                        let parsed_time = match i64::from_str(time) {
                            Ok(t) => t,
                            Err(_) => return false,
                        };
                        let metric = sync::Arc::make_mut(&mut metric.clone()).take().unwrap();
                        res.push(metric.set_name(name).set_value(parsed_val).time(parsed_time));
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
    use chrono::{TimeZone, UTC};
    use metric::MetricKind;
    use super::*;

    #[test]
    fn test_parse_graphite() {
        let pyld = "fst 1 101\nsnd -2.0 202\nthr 3 303\nfth@fth 4 404\nfv%fv 5 505\ns-th 6 606\n";
        let mut res = Vec::new();
        let metric = sync::Arc::new(Some(Metric::default()));
        assert!(parse_graphite(pyld, &mut res, metric));

        assert_eq!(res[0].kind, MetricKind::Raw);
        assert_eq!(res[0].name, "fst");
        assert_eq!(res[0].value(), Some(1.0));
        assert_eq!(res[0].time, UTC.timestamp(101, 0).timestamp());

        assert_eq!(res[1].kind, MetricKind::Raw);
        assert_eq!(res[1].name, "snd");
        assert_eq!(res[1].value(), Some(-2.0));
        assert_eq!(res[1].time, UTC.timestamp(202, 0).timestamp());

        assert_eq!(res[2].kind, MetricKind::Raw);
        assert_eq!(res[2].name, "thr");
        assert_eq!(res[2].value(), Some(3.0));
        assert_eq!(res[2].time, UTC.timestamp(303, 0).timestamp());

        assert_eq!(res[3].kind, MetricKind::Raw);
        assert_eq!(res[3].name, "fth@fth");
        assert_eq!(res[3].value(), Some(4.0));
        assert_eq!(res[3].time, UTC.timestamp(404, 0).timestamp());

        assert_eq!(res[4].kind, MetricKind::Raw);
        assert_eq!(res[4].name, "fv%fv");
        assert_eq!(res[4].value(), Some(5.0));
        assert_eq!(res[4].time, UTC.timestamp(505, 0).timestamp());

        assert_eq!(res[5].kind, MetricKind::Raw);
        assert_eq!(res[5].name, "s-th");
        assert_eq!(res[5].value(), Some(6.0));
        assert_eq!(res[5].time, UTC.timestamp(606, 0).timestamp());
    }
}
