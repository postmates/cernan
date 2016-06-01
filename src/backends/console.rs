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
            println!("    {}: {} min", key, value.min().unwrap());
            println!("    {}: {} max", key, value.max().unwrap());
            println!("    {}: {} mean", key, value.mean().unwrap());

            println!("    {}: {} 50th", key, value.percentile(50.0).unwrap());
            println!("    {}: {} 90th", key, value.percentile(90.0).unwrap());
            println!("    {}: {} 99th", key, value.percentile(99.0).unwrap());
            println!("    {}: {} 99.9th", key, value.percentile(99.9).unwrap());
        }

        println!("  timers:");
        for (key, value) in buckets.timers() {
            println!("    {}: {} min", key, value.min().unwrap());
            println!("    {}: {} max", key, value.max().unwrap());
            println!("    {}: {} mean", key, value.mean().unwrap());

            println!("    {}: {} 50th", key, value.percentile(50.0).unwrap());
            println!("    {}: {} 90th", key, value.percentile(90.0).unwrap());
            println!("    {}: {} 99th", key, value.percentile(99.0).unwrap());
            println!("    {}: {} 99.9th", key, value.percentile(99.9).unwrap());
        }

    }
}
