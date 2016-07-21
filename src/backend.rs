use backends::*;
use metric::Metric;

use config::Args;

use std::sync::Arc;

use std::sync::mpsc::{Receiver, Sender, channel};
use std::thread;

use server;

/// A 'backend' is a sink for metrics.
pub trait Backend {
    fn flush(&mut self) -> ();
    fn deliver(&mut self, point: Arc<Metric>) -> ();
    fn run(&mut self, recv: Receiver<Arc<server::Event>>) {
        for event in recv.recv().iter() {
            match *(*event) {
                server::Event::TimerFlush => self.flush(),
                server::Event::Graphite(ref metrics) => {
                    for metric in metrics {
                        self.deliver(metric.clone());
                    }
                }
                server::Event::Statsd(ref metrics) => {
                    for metric in metrics {
                        self.deliver(metric.clone());
                    }
                }
            }
        }
    }
}

pub fn librato_extract_source(tags: &str) -> &str {
    for tag in tags.split(",") {
        let mut tag_pieces = tag.split("=");
        let name = tag_pieces.next();
        let val = tag_pieces.next();

        if name == Some("source") {
            return val.unwrap()
        }
    }
    "cernan"
}

/// Creates the collection of backends based on the paraemeters
///
pub fn factory(args: Args) -> Vec<Sender<Arc<server::Event>>> {
    // create the channel
    // spawn the thread

    let mut backends = Vec::with_capacity(3);

    if args.console {
        let (send, recv) = channel();
        thread::spawn(move || {
            console::Console::new().run(recv);
        });
        backends.push(send);
    }
    if args.wavefront {
        let (send, recv) = channel();
        let wf_tags: String = args.tags.replace(",", " ");
        let cp_args = args.clone();
        thread::spawn(move || {
            wavefront::Wavefront::new(&cp_args.wavefront_host.unwrap(),
                                      cp_args.wavefront_port.unwrap(),
                                      cp_args.wavefront_skip_aggrs,
                                      wf_tags)
                .run(recv);
        });
        backends.push(send);
    }
    if args.librato {
        let (send, recv) = channel();
        let cp_args = args.clone();

        // librato does not support arbitrary tags, only a 'source' tag. We have
        // to parse the source tag--if it exists--out and ship only that.
        thread::spawn(move || {
            let metric_source = librato_extract_source(&args.tags);
            librato::Librato::new(&cp_args.librato_username.unwrap(),
                                  &cp_args.librato_token.unwrap(),
                                  metric_source,
                                  &cp_args.librato_host.unwrap())
                .run(recv);
        });
        backends.push(send);
    }
    backends
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_librato_source_extraction() {
        assert_eq!("cernan", librato_extract_source("flkjsdf"));
        assert_eq!("testsrc", librato_extract_source("host=hs,source=testsrc"));
        assert_eq!("testsrc", librato_extract_source("source=testsrc,host=hs"));
        assert_eq!("testsrc", librato_extract_source("source=testsrc"));
    }
}
