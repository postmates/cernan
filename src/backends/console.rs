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
            println!("    {}: {} min", key, value.query(0.0).unwrap().1);
            println!("    {}: {} max", key, value.query(1.0).unwrap().1);

            println!("    {}: {} 50th", key, value.query(0.5).unwrap().1);
            println!("    {}: {} 90th", key, value.query(0.9).unwrap().1);
            println!("    {}: {} 99th", key, value.query(0.99).unwrap().1);
            println!("    {}: {} 99.9th", key, value.query(0.999).unwrap().1);
        }

        println!("  timers:");
        for (key, value) in buckets.timers() {
            println!("    {}: {} min", key, value.query(0.0).unwrap().1);
            println!("    {}: {} max", key, value.query(1.0).unwrap().1);

            println!("    {}: {} 50th", key, value.query(0.5).unwrap().1);
            println!("    {}: {} 90th", key, value.query(0.9).unwrap().1);
            println!("    {}: {} 99th", key, value.query(0.99).unwrap().1);
            println!("    {}: {} 99.9th", key, value.query(0.999).unwrap().1);
        }

    }
}
