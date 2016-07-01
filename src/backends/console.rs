use super::super::backend::Backend;
use super::super::buckets::Buckets;
use time;

#[derive(Debug)]
pub struct Console;


impl Console {
    /// Create a Console formatter that prints to stdout
    ///
    /// # Examples
    ///
    /// ```
    /// let cons = Console::new();
    /// ```
    pub fn new() -> Console {
        Console
    }
}

/// Print a single stats line.
fn fmt_line(key: &str, value: &f64) {
    println!("    {}: {}", key, value)
}


impl Backend for Console {
    fn flush(&mut self, buckets: &Buckets) {
        let now = time::get_time();
        println!("Flushing metrics: {}", time::at(now).rfc822().to_string());

        println!("  bad_messages: {}", buckets.bad_messages());
        println!("  total_messages: {}", buckets.total_messages());

        println!("  counters:");
        for (key, value) in buckets.counters() {
            fmt_line(&key, &value);
        }

        println!("  gauges:");
        for (key, value) in buckets.gauges() {
            fmt_line(&key, &value);
        }

        println!("  histograms:");
        for (key, value) in buckets.histograms() {
            for tup in [("min", 0.0),
                        ("max", 1.0),
                        ("50", 0.5),
                        ("90", 0.90),
                        ("99", 0.99),
                        ("999", 0.999)]
                .iter() {
                let stat: &str = tup.0;
                let quant: f64 = tup.1;
                println!("    {}: {} {}", key, stat, value.query(quant).unwrap().1);
            }
        }

        println!("  timers:");
        for (key, value) in buckets.timers() {
            for tup in [("min", 0.0),
                        ("max", 1.0),
                        ("50", 0.5),
                        ("90", 0.90),
                        ("99", 0.99),
                        ("999", 0.999)]
                .iter() {
                let stat: &str = tup.0;
                let quant: f64 = tup.1;
                println!("    {}: {} {}", key, stat, value.query(quant).unwrap().1);
            }
        }

    }
}
