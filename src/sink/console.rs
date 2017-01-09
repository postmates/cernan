use buckets::Buckets;
use chrono;
use metric::{AggregationMethod, LogLine, Telemetry};
use sink::{Sink, Valve};
use std::sync;

pub struct Console {
    aggrs: Buckets,
}

impl Console {
    pub fn new(config: ConsoleConfig) -> Console {
        Console { aggrs: Buckets::new(config.bin_width) }
    }
}

#[derive(Debug)]
pub struct ConsoleConfig {
    pub config_path: String,
    pub bin_width: i64,
}

impl ConsoleConfig {
    pub fn new(config_path: String) -> ConsoleConfig {
        ConsoleConfig {
            config_path: config_path,
            bin_width: 1,
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

    fn deliver_line(&mut self, _: sync::Arc<Option<LogLine>>) -> () {
        // drop the line, intentionally
    }

    fn flush(&mut self) {
        println!("Flushing metrics: {}", chrono::UTC::now().to_rfc3339());

        let mut sums = String::new();
        let mut sets = String::new();
        let mut summaries = String::new();

        for (key, values) in self.aggrs.aggrs() {
            for value in values {
                match value.aggr_method {
                    AggregationMethod::Sum => {
                        let mut tgt = &mut sums;
                        if let Some(f) = value.value() {
                            tgt.push_str("    ");
                            tgt.push_str(&key);
                            tgt.push_str("(");
                            tgt.push_str(&value.timestamp.to_string());
                            tgt.push_str("): ");
                            tgt.push_str(&f.to_string());
                            tgt.push_str("\n");
                        }
                    } 
                    AggregationMethod::Set => {
                        let mut tgt = &mut sets;
                        if let Some(f) = value.value() {
                            tgt.push_str("    ");
                            tgt.push_str(&key);
                            tgt.push_str("(");
                            tgt.push_str(&value.timestamp.to_string());
                            tgt.push_str("): ");
                            tgt.push_str(&f.to_string());
                            tgt.push_str("\n");
                        }
                    }
                    AggregationMethod::Summarize => {
                        let mut tgt = &mut summaries;
                        for tup in &[("min", 0.0),
                                     ("max", 1.0),
                                     ("50", 0.5),
                                     ("90", 0.90),
                                     ("99", 0.99),
                                     ("999", 0.999)] {
                            let stat: &str = tup.0;
                            let quant: f64 = tup.1;
                            if let Some(f) = value.query(quant) {
                                tgt.push_str("    ");
                                tgt.push_str(&key);
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
