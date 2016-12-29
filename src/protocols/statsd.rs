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
