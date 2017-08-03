use buckets::Buckets;
use chrono::DateTime;
use chrono::naive::NaiveDateTime;
use chrono::offset::Utc;
use metric::{AggregationMethod, LogLine, Telemetry};
use sink::{Sink, Valve};
use std::sync;

/// The 'console' sink exists for development convenience. The sink will
/// aggregate according to [buckets](../buckets/struct.Buckets.html) method and
/// print each `flush-interval` to stdout.
pub struct Console {
    aggrs: Buckets,
    buffer: Vec<LogLine>,
    flush_interval: u64,
}

impl Console {
    /// Create a new Console
    ///
    /// # Examples
    ///
    /// ```
    /// use cernan::sink::{Console, ConsoleConfig};
    /// let config = ConsoleConfig { config_path:
    /// Some("sinks.console".to_string()),
    /// bin_width: 2, flush_interval: 60 };
    /// let c = Console::new(config);
    /// ```
    pub fn new(config: ConsoleConfig) -> Console {
        Console {
            aggrs: Buckets::new(config.bin_width),
            buffer: Vec::new(),
            flush_interval: config.flush_interval,
        }
    }
}

/// The configuration struct for Console. There's not a whole lot to configure
/// here, independent of other sinks, but Console does do aggregations and that
/// requires knowing what the user wants for `bin_width`.
#[derive(Debug, Deserialize)]
pub struct ConsoleConfig {
    /// The sink's unique name in the routing topology.
    pub config_path: Option<String>,
    /// Sets the bin width for Console's underlying
    /// [bucket](../buckets/struct.Bucket.html).
    pub bin_width: i64,
    pub flush_interval: u64,
}

impl Default for ConsoleConfig {
    fn default() -> ConsoleConfig {
        ConsoleConfig {
            bin_width: 1,
            flush_interval: 60,
            config_path: None,
        }
    }
}

impl ConsoleConfig {
    /// Convenience method to create a ConsoleConfig with `bin_width` equal to
    /// 1.
    ///
    /// # Examples
    ///
    /// ```
    /// use cernan::sink::ConsoleConfig;
    /// let config = ConsoleConfig::new("sinks.console".to_string(), 60);
    /// assert_eq!(1, config.bin_width);
    /// ```
    pub fn new(config_path: String, flush_interval: u64) -> ConsoleConfig {
        ConsoleConfig {
            config_path: Some(config_path),
            bin_width: 1,
            flush_interval: flush_interval,
        }
    }
}

impl Sink for Console {
    fn valve_state(&self) -> Valve {
        Valve::Open
    }

    fn deliver(&mut self, mut point: sync::Arc<Option<Telemetry>>) -> () {
        self.aggrs.add(sync::Arc::make_mut(&mut point).take().unwrap());
    }

    fn deliver_line(&mut self, mut lines: sync::Arc<Option<LogLine>>) -> () {
        let line: LogLine = sync::Arc::make_mut(&mut lines).take().unwrap();
        self.buffer.append(&mut vec![line]);
    }

    fn flush_interval(&self) -> Option<u64> {
        Some(self.flush_interval)
    }

    fn flush(&mut self) {
        println!("Flushing lines: {}", Utc::now().to_rfc3339());
        for line in &self.buffer {
            println!("{} {}: {}", format_time(line.time), line.path, line.value);
        }
        self.buffer.clear();

        println!("Flushing metrics: {}", Utc::now().to_rfc3339());

        let mut sums = String::new();
        let mut sets = String::new();
        let mut summaries = String::new();

        for telem in self.aggrs.iter() {
            match telem.kind() {
                AggregationMethod::Histogram => {
                    unimplemented!();
                }
                AggregationMethod::Sum => {
                    let mut tgt = &mut sums;
                    if let Some(f) = telem.sum() {
                        tgt.push_str("    ");
                        tgt.push_str(&telem.name);
                        tgt.push_str("(");
                        tgt.push_str(&telem.timestamp.to_string());
                        tgt.push_str("): ");
                        tgt.push_str(&f.to_string());
                        tgt.push_str("\n");
                    }
                }
                AggregationMethod::Set => {
                    let mut tgt = &mut sets;
                    if let Some(f) = telem.set() {
                        tgt.push_str("    ");
                        tgt.push_str(&telem.name);
                        tgt.push_str("(");
                        tgt.push_str(&telem.timestamp.to_string());
                        tgt.push_str("): ");
                        tgt.push_str(&f.to_string());
                        tgt.push_str("\n");
                    }
                }
                AggregationMethod::Summarize => {
                    let mut tgt = &mut summaries;
                    for tup in &[
                        ("min", 0.0),
                        ("max", 1.0),
                        ("50", 0.5),
                        ("90", 0.90),
                        ("99", 0.99),
                        ("999", 0.999),
                    ] {
                        let stat: &str = tup.0;
                        let quant: f64 = tup.1;
                        if let Some(f) = telem.query(quant) {
                            tgt.push_str("    ");
                            tgt.push_str(&telem.name);
                            tgt.push_str(": ");
                            tgt.push_str(stat);
                            tgt.push_str(" ");
                            tgt.push_str(&f.to_string());
                            tgt.push_str("\n");
                        }
                    }
                }
            }
        }
        println!("  sums:");
        print!("{}", sums);
        println!("  sets:");
        print!("{}", sets);
        println!("  summaries:");
        print!("{}", summaries);

        self.aggrs.reset();
    }
}

#[inline]
fn format_time(time: i64) -> String {
    let naive_time = NaiveDateTime::from_timestamp(time, 0);
    let utc_time: DateTime<Utc> = DateTime::from_utc(naive_time, Utc);
    format!("{}", utc_time.format("%Y-%m-%dT%H:%M:%S%.3fZ"))
}
