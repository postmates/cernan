/// Internal metric representation
///
use std::fmt;


/// Enum of metric types
pub enum MetricKind {
    Counter(f64), // sample rate
    Gauge,
    Timer,
    Histogram,
}

impl fmt::Debug for MetricKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            MetricKind::Gauge => write!(f, "Gauge"),
            MetricKind::Histogram => write!(f, "Histogram"),
            MetricKind::Timer => write!(f, "Timer"),
            MetricKind::Counter(s) => write!(f, "Counter(s={})", s),
        }
    }
}


/// Error types for parsing Metrics from strings.
///
#[derive(Debug)]
pub enum ParseError {
    // Error message, column
    SyntaxError(&'static str, usize),
}


/// Metric value objects.
///
#[derive(Debug)]
pub struct Metric {
    pub kind: MetricKind,
    pub name: String,
    pub value: f64,
}

impl Metric {
    /// Create a new metric
    ///
    /// Uses the Into trait to allow both str and String types.
    pub fn new<S: Into<String>>(name: S, value: f64, kind: MetricKind) -> Metric {
        Metric {
            name: name.into(),
            value: value,
            kind: kind,
        }
    }

    /// Valid message formats are:
    ///
    /// - `<str:metric_name>:<f64:value>|<str:type>`
    /// - `<str:metric_name>:<f64:value>|c|@<f64:sample_rate>`
    ///
    /// Multiple metrics can be sent in a single UDP packet
    /// separated by newlines.
    pub fn parse(source: &str) -> Result<Vec<Metric>, ParseError> {
        let mut results: Vec<Metric> = Vec::new();

        for line in source.lines() {
            match Metric::parse_line(line) {
                Ok(metric) => results.push(metric),
                Err(e) => return Err(e),
            }
        }
        if results.len() == 0 {
            return Err(ParseError::SyntaxError("No metrics found", 0));
        }
        Ok(results)
    }

    /// Parses a metric from each line in a packet.
    fn parse_line(line: &str) -> Result<Metric, ParseError> {
        // track position in string
        let mut idx = 0;

        // Get the metric name
        let name = match line.find(':') {
            Some(pos) => {
                idx = pos + 1;
                &line[0..pos]
            }
            _ => "",
        };
        if name.is_empty() {
            return Err(ParseError::SyntaxError("Metrics require a name.", idx));
        }

        // Get the float val
        let value = match line[idx..].find('|') {
            Some(pos) => {
                let start = idx;
                idx += pos + 1;
                line[start..idx - 1].parse::<f64>().ok().unwrap()
            }
            _ => return Err(ParseError::SyntaxError("Metrics require a value.", idx)),
        };
        let kind_name = match line[idx..].find('|') {
            Some(pos) => {
                let start = idx;
                idx += pos;
                line[start..idx].to_string()
            }
            _ => line[idx..].to_string(),
        };

        // Get kind parts, use deref/ref tricks
        // to get types to match
        let kind = match &*kind_name {
            "ms" => MetricKind::Timer,
            "g" => MetricKind::Gauge,
            "h" => MetricKind::Histogram,
            "c" => {
                let rate: f64 = match line[idx..].find('@') {
                    Some(pos) => {
                        idx += pos + 1;
                        line[idx..].parse::<f64>().ok().unwrap()
                    }
                    _ => 1.0,
                };
                MetricKind::Counter(rate)
            }
            _ => return Err(ParseError::SyntaxError("Unknown metric type.", idx)),
        };
        Ok(Metric::new(name, value, kind))
    }
}



// Tests
//
#[cfg(test)]
mod tests {
    use metric::{Metric, MetricKind};
    use std::collections::HashMap;

    #[test]
    fn test_metric_kind_debug_fmt() {
        assert_eq!("Gauge", format!("{:?}", MetricKind::Gauge));
        assert_eq!("Histogram", format!("{:?}", MetricKind::Histogram));
        assert_eq!("Timer", format!("{:?}", MetricKind::Timer));
        assert_eq!("Counter(s=6)", format!("{:?}", MetricKind::Counter(6.0)));
    }

    #[test]
    fn test_metric_parse_invalid_no_name() {
        let res = Metric::parse("");
        assert!(res.is_err(), "Should have an error");
        assert!(!res.is_ok(), "Should have an error");
    }

    #[test]
    fn test_metric_parse_invalid_no_value() {
        let res = Metric::parse("foo:");
        assert!(res.is_err(), "Should have an error");
        assert!(!res.is_ok(), "Should have an error");
    }

    #[test]
    fn test_metric_multiple() {
        let res = Metric::parse("a.b:12.1|g\nb.c:13.2|c").unwrap();
        assert_eq!(2, res.len());

        assert_eq!("a.b", res[0].name);
        assert_eq!(12.1, res[0].value);

        assert_eq!("b.c", res[1].name);
        assert_eq!(13.2, res[1].value);
    }

    #[test]
    fn test_metric_valid() {
        let mut valid = HashMap::new();
        valid.insert("foo.test:12.3|ms\n",
                     Metric::new("foo.test", 12.3, MetricKind::Timer));
        valid.insert("foo.test:12.3|ms",
                     Metric::new("foo.test", 12.3, MetricKind::Timer));
        valid.insert("test:18.123|g",
                     Metric::new("test", 18.123, MetricKind::Gauge));
        valid.insert("test:18.123|g",
                     Metric::new("test", 18.123, MetricKind::Gauge));
        valid.insert("hist_test:18.123|h",
                     Metric::new("hist_test", 18.123, MetricKind::Histogram));
        valid.insert("hist_test:18.123|h",
                     Metric::new("hist_test", 18.123, MetricKind::Histogram));
        valid.insert("thing.total:12|c",
                     Metric::new("thing.total", 12.0, MetricKind::Counter(1.0)));
        valid.insert("thing.total:5.6|c|@123",
                     Metric::new("thing.total", 5.6, MetricKind::Counter(123.0)));

        for (input, expected) in valid.iter() {
            let result = Metric::parse(*input);
            assert!(result.is_ok());

            let actual = result.ok().unwrap();
            assert_eq!(expected.name, actual[0].name);
            assert_eq!(expected.value, actual[0].value);

            // TODO this is a silly way to test
            assert_eq!(format!("{:?}", expected.kind),
                       format!("{:?}", actual[0].kind));
        }
    }

    #[test]
    fn test_metric_invalid() {
        let invalid = vec!["",
                           "metric",
                           "metric|11:",
                           "metric|12",
                           "metric:13|",
                           "metric:14|c@1",
                           ":|@",
                           ":1.0|c"];
        for input in invalid.iter() {
            println!("{:?}", input);
            let result = Metric::parse(*input);
            assert!(result.is_err());
        }
    }
}
