use backends::*;
use metric::Metric;

use regex::Regex;
use std::rc::Rc;
use clap::ArgMatches;
use std::str::FromStr;

/// A 'backend' is a sink for metrics.
pub trait Backend {
    fn flush(&mut self) -> ();
    fn deliver(&mut self, point: Rc<Metric>) -> ();
}

/// Creates the collection of backends based on the paraemeters
///
pub fn factory(args: &ArgMatches) -> Vec<Box<Backend>> {
    let mut backends: Vec<Box<Backend>> = Vec::with_capacity(3);

    let tags = args.value_of("tags").unwrap();
    if args.value_of("console").is_some() {
        backends.push(Box::new(console::Console::new()));
    }
    if args.value_of("wavefront").is_some() {
        let wf_tags: String = tags.replace(",", " ");
        backends.push(Box::new(wavefront::Wavefront::new(args.value_of("wavefront-host").unwrap(),
                                                     u16::from_str(args.value_of("wavefront-port")
                                                             .unwrap())
                                                         .unwrap(),
                                                     args.value_of("wavefront-skip-aggrs")
                                                         .is_some(),
                                                     wf_tags)));
    }
    if args.value_of("librato").is_some() {
        let re = Regex::new(r"(?x)(source=(?P<source>.*),+)?").unwrap();
        let metric_source = re.captures(tags).unwrap().name("source").unwrap_or("cernan");
        // librato does not support arbitrary tags, only a 'source' tag. We have
        // to parse the source tag--if it exists--out and ship only that.
        backends.push(Box::new(librato::Librato::new(args.value_of("librato-username").unwrap(),
                                                     args.value_of("librato-token").unwrap(),
                                                     metric_source,
                                                     args.value_of("librato-host").unwrap())));
    }
    backends
}
