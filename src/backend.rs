use backends::*;
use metric::Metric;

use config::Args;

use regex::Regex;
use std::rc::Rc;

/// A 'backend' is a sink for metrics.
pub trait Backend {
    fn flush(&mut self) -> ();
    fn deliver(&mut self, point: Rc<Metric>) -> ();
}

/// Creates the collection of backends based on the paraemeters
///
pub fn factory(args: Args) -> Vec<Box<Backend>> {
    let mut backends: Vec<Box<Backend>> = Vec::with_capacity(3);

    if args.console {
        backends.push(Box::new(console::Console::new()));
    }
    if args.wavefront {
        let wf_tags: String = args.tags.replace(",", " ");
        backends.push(Box::new(wavefront::Wavefront::new(&args.wavefront_host.unwrap(),
                                                         args.wavefront_port.unwrap(),
                                                         args.wavefront_skip_aggrs,
                                                         wf_tags)));
    }
    if args.librato {
        let re = Regex::new(r"(?x)(source=(?P<source>.*),+)?").unwrap();
        let metric_source = re.captures(&args.tags).unwrap().name("source").unwrap_or("cernan");
        // librato does not support arbitrary tags, only a 'source' tag. We have
        // to parse the source tag--if it exists--out and ship only that.
        backends.push(Box::new(librato::Librato::new(&args.librato_username.unwrap(),
                                                     &args.librato_token.unwrap(),
                                                     metric_source,
                                                     &args.librato_host.unwrap())));
    }
    backends
}
