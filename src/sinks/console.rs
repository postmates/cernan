use sink::Sink;
use buckets::Buckets;
use metric::{Metric, LogLine};
use chrono;

pub struct Console {
    aggrs: Buckets,
}

impl Console {
    pub fn new() -> Console {
        Console { aggrs: Buckets::default() }
    }
}

impl Default for Console {
    fn default() -> Self {
        Self::new()
    }
}

/// Print a single stats line.
fn fmt_line(key: &str, value: f64) {
    println!("    {}: {}", key, value)
}


impl Sink for Console {
    fn deliver(&mut self, point: Metric) {
        self.aggrs.add(point);
    }

    fn deliver_lines(&mut self, _: Vec<LogLine>) {
        // nothing, intentionally
    }

    fn flush(&mut self) {
        let now = chrono::UTC::now();
        println!("Flushing metrics: {}", now.to_rfc3339());

        println!("  counters:");
        for (key, value) in self.aggrs.counters() {
            for m in value {
                fmt_line(key, m.value);
            }
        }

        println!("  gauges:");
        for (key, value) in self.aggrs.gauges() {
            for m in value {
                fmt_line(key, m.value);
            }
        }

        println!("  raws:");
        for (key, value) in self.aggrs.raws() {
            for m in value {
                fmt_line(key, m.value);
            }
        }

        println!("  histograms:");
        for (key, hists) in self.aggrs.histograms() {
            for &(_, ref hist) in hists {
                for tup in &[("min", 0.0),
                             ("max", 1.0),
                             ("50", 0.5),
                             ("90", 0.90),
                             ("99", 0.99),
                             ("999", 0.999)] {
                    let stat: &str = tup.0;
                    let quant: f64 = tup.1;
                    println!("    {}: {} {}", key, stat, hist.query(quant).unwrap().1);
                }
            }
        }

        println!("  timers:");
        for (key, tmrs) in self.aggrs.timers() {
            for &(_, ref tmr) in tmrs {
                for tup in &[("min", 0.0),
                             ("max", 1.0),
                             ("50", 0.5),
                             ("90", 0.90),
                             ("99", 0.99),
                             ("999", 0.999)] {
                    let stat: &str = tup.0;
                    let quant: f64 = tup.1;
                    println!("    {}: {} {}", key, stat, tmr.query(quant).unwrap().1);
                }
            }
        }

        self.aggrs.reset();
    }
}
