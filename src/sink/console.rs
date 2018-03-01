//! Console Event logger.

use buckets::Buckets;
use chrono::DateTime;
use chrono::naive::NaiveDateTime;
use chrono::offset::Utc;
use metric::{AggregationMethod, LogLine, TagMap, Telemetry};
use sink::{Sink, Valve};

/// The 'console' sink exists for development convenience. The sink will
/// aggregate according to [buckets](../buckets/struct.Buckets.html) method and
/// print each `flush-interval` to stdout.
pub struct Console {
    aggrs: Buckets,
    buffer: Vec<LogLine>,
    flush_interval: u64,
    tags: TagMap,
}

/// The configuration struct for Console. There's not a whole lot to configure
/// here, independent of other sinks, but Console does do aggregations and that
/// requires knowing what the user wants for `bin_width`.
#[derive(Clone, Debug, Deserialize)]
pub struct ConsoleConfig {
    /// The sink's unique name in the routing topology.
    pub config_path: Option<String>,
    /// Sets the bin width for Console's underlying
    /// [bucket](../buckets/struct.Bucket.html).
    pub bin_width: i64,
    /// The tags to be applied to all `metric::Event`s streaming through this
    /// sink. These tags will overwrite any tags carried by the `metric::Event`
    /// itself.
    pub tags: TagMap,
    /// The sink specific `flush_interval`.
    pub flush_interval: u64,
}

impl Default for ConsoleConfig {
    fn default() -> ConsoleConfig {
        ConsoleConfig {
            bin_width: 1,
            flush_interval: 60,
            config_path: None,
            tags: TagMap::default(),
        }
    }
}

impl ConsoleConfig {
    /// Convenience method to create a ConsoleConfig with `bin_width` equal to
    /// 1.
    pub fn new(
        config_path: String,
        flush_interval: u64,
        tags: TagMap,
    ) -> ConsoleConfig {
        ConsoleConfig {
            config_path: Some(config_path),
            bin_width: 1,
            flush_interval: flush_interval,
            tags: tags,
        }
    }
}

impl Sink<ConsoleConfig> for Console {
    fn init(config: ConsoleConfig) -> Self {
        Console {
            aggrs: Buckets::new(config.bin_width),
            buffer: Vec::new(),
            flush_interval: config.flush_interval,
            tags: config.tags,
        }
    }

    fn valve_state(&self) -> Valve {
        Valve::Open
    }

    fn deliver(&mut self, point: Telemetry) -> () {
        self.aggrs.add(point);
    }

    fn deliver_line(&mut self, line: LogLine) -> () {
        self.buffer.append(&mut vec![line]);
    }

    fn flush_interval(&self) -> Option<u64> {
        Some(self.flush_interval)
    }

    fn flush(&mut self) {
        println!("Flushing lines: {}", Utc::now().to_rfc3339());
        for line in &self.buffer {
            print!("{} {}: {}", format_time(line.time), line.path, line.value);
            for (k, v) in line.tags(&self.tags) {
                print!(" {}={}", k, v);
            }
            println!("");
        }
        self.buffer.clear();

        println!("Flushing metrics: {}", Utc::now().to_rfc3339());

        let mut sums = String::new();
        let mut sets = String::new();
        let mut summaries = String::new();
        let mut histograms = String::new();

        for telem in self.aggrs.iter() {
            match telem.kind() {
                AggregationMethod::Histogram => {
                    use quantiles::histogram::Bound;
                    let tgt = &mut histograms;
                    if let Some(bin_iter) = telem.bins() {
                        for &(bound, val) in bin_iter {
                            tgt.push_str("    ");
                            tgt.push_str(&telem.name);
                            tgt.push_str("_");
                            match bound {
                                Bound::Finite(bnd) => {
                                    tgt.push_str(&bnd.to_string());
                                }
                                Bound::PosInf => {
                                    tgt.push_str("pos_inf");
                                }
                            };
                            tgt.push_str("(");
                            tgt.push_str(&telem.timestamp.to_string());
                            tgt.push_str("): ");
                            tgt.push_str(&val.to_string());
                            tgt.push_str("\n");
                        }
                    }
                }
                AggregationMethod::Sum => {
                    let tgt = &mut sums;
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
                    let tgt = &mut sets;
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
                    let tgt = &mut summaries;
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
        println!("  histograms:");
        print!("{}", histograms);

        self.aggrs.reset();
    }

    fn shutdown(mut self) -> () {
        self.flush();
    }
}

#[inline]
fn format_time(time: i64) -> String {
    let naive_time = NaiveDateTime::from_timestamp(time, 0);
    let utc_time: DateTime<Utc> = DateTime::from_utc(naive_time, Utc);
    format!("{}", utc_time.format("%Y-%m-%dT%H:%M:%S%.3fZ"))
}
