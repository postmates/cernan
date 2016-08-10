use backends::*;
use metric::Metric;

use config::Args;

use regex::Regex;
use std::sync::Arc;

use std::sync::mpsc::{Receiver, Sender, channel};
use std::thread;

use server;

/// A 'backend' is a sink for metrics.
pub trait Backend {
    fn flush(&mut self) -> ();
    fn deliver(&mut self, point: Arc<Metric>) -> ();
    fn run(&mut self, recv: Receiver<Arc<server::Event>>) {
        for event in recv.iter() {
            match *event {
                server::Event::TimerFlush => self.flush(),
                server::Event::Graphite(ref metrics) => {
                    debug!("Graphite Event!");
                    for metric in metrics {
                        self.deliver(metric.clone());
                    }
                }
                server::Event::Statsd(ref metrics) => {
                    debug!("Statsd Event!");
                    for metric in metrics {
                        self.deliver(metric.clone());
                    }
                }
            }
        }
    }
}

/// Creates the collection of backends based on the paraemeters
///
pub fn factory(args: Args) -> Vec<Sender<Arc<server::Event>>> {
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
            let re = Regex::new(r"(?x)(source=(?P<source>.*),+)?").unwrap();
            let metric_source =
                re.captures(&cp_args.tags).unwrap().name("source").unwrap_or("cernan");
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
