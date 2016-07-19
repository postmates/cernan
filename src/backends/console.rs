use backend::Backend;
use buckets::Buckets;
use metric::Metric;
use chrono;
use std::sync::Arc;

pub struct Console {
    aggrs: Buckets,
}

impl Console {
    /// Create a Console formatter that prints to stdout
    ///
    /// # Examples
    ///
    /// ```
    /// let cons = Console::new();
    /// ```
    pub fn new() -> Console {
        Console { aggrs: Buckets::new() }
    }
}

/// Print a single stats line.
fn fmt_line(key: &str, value: &f64) {
    println!("    {}: {}", key, value)
}


impl Backend for Console {
    fn deliver(&mut self, point: Arc<Metric>) {
        debug!("console deliver");
        self.aggrs.add(&point);
    }

    fn flush(&mut self) {
        debug!("console flush");
        let now = chrono::UTC::now();
        println!("Flushing metrics: {}", now.to_rfc3339());

        println!("  counters:");
        for (key, value) in self.aggrs.counters() {
            fmt_line(key, value);
        }

        println!("  gauges:");
        for (key, value) in self.aggrs.gauges() {
            fmt_line(key, value);
        }

        println!("  histograms:");
        for (key, value) in self.aggrs.histograms() {
            for tup in &[("min", 0.0),
                         ("max", 1.0),
                         ("50", 0.5),
                         ("90", 0.90),
                         ("99", 0.99),
                         ("999", 0.999)] {
                let stat: &str = tup.0;
                let quant: f64 = tup.1;
                println!("    {}: {} {}", key, stat, value.query(quant).unwrap().1);
            }
        }

        println!("  timers:");
        for (key, value) in self.aggrs.timers() {
            for tup in &[("min", 0.0),
                         ("max", 1.0),
                         ("50", 0.5),
                         ("90", 0.90),
                         ("99", 0.99),
                         ("999", 0.999)] {
                let stat: &str = tup.0;
                let quant: f64 = tup.1;
                println!("    {}: {} {}", key, stat, value.query(quant).unwrap().1);
            }
        }

    }
}
