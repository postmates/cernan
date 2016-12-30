use buckets::Buckets;
use chrono;
use metric::{LogLine, Metric};
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

/// Print a single stats line.
fn fmt_line(key: &str, time: i64, value: f64) {
    println!("    {}({}): {}", key, time, value)
}

impl Sink for Console {
    fn valve_state(&self) -> Valve {
        Valve::Open
    }

    fn deliver(&mut self, mut point: sync::Arc<Option<Metric>>) -> () {
        self.aggrs.add(sync::Arc::make_mut(&mut point).take().unwrap());
    }

    fn deliver_line(&mut self, _: sync::Arc<Option<LogLine>>) -> () {
        // drop the line, intentionally
    }

    fn flush(&mut self) {
        println!("Flushing metrics: {}", chrono::UTC::now().to_rfc3339());

        println!("  counters:");
        for (key, value) in self.aggrs.counters() {
            for m in value {
                if let Some(f) = m.value() {
                    fmt_line(key, m.time, f)
                }
            }
        }

        println!("  gauges:");
        for (key, value) in self.aggrs.gauges() {
            for m in value {
                if let Some(f) = m.value() {
                    fmt_line(key, m.time, f)
                }
            }
        }

        println!("  delta-gauges:");
        for (key, value) in self.aggrs.delta_gauges() {
            for m in value {
                if let Some(f) = m.value() {
                    fmt_line(key, m.time, f)
                }
            }
        }

        println!("  raws:");
        for (key, value) in self.aggrs.raws() {
            for m in value {
                if let Some(f) = m.value() {
                    fmt_line(key, m.time, f)
                }
            }
        }

        println!("  histograms:");
        for (key, hists) in self.aggrs.histograms() {
            for h in hists {
                for tup in &[("min", 0.0),
                             ("max", 1.0),
                             ("50", 0.5),
                             ("90", 0.90),
                             ("99", 0.99),
                             ("999", 0.999)] {
                    let stat: &str = tup.0;
                    let quant: f64 = tup.1;
                    println!("    {}: {} {}", key, stat, h.query(quant).unwrap());
                }
            }
        }

        println!("  timers:");
        for (key, tmrs) in self.aggrs.timers() {
            for tmr in tmrs {
                for tup in &[("min", 0.0),
                             ("max", 1.0),
                             ("50", 0.5),
                             ("90", 0.90),
                             ("99", 0.99),
                             ("999", 0.999)] {
                    let stat: &str = tup.0;
                    let quant: f64 = tup.1;
                    println!("    {}: {} {}", key, stat, tmr.query(quant).unwrap());
                }
            }
        }

        self.aggrs.reset();
    }
}
