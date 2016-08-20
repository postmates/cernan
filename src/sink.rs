use sinks::*;
use metric::Metric;

use config::Args;

use std::sync::Arc;

use std::sync::mpsc::{Receiver, Sender, channel};
use std::thread;

use server;

/// A 'sink' is a sink for metrics.
pub trait Sink {
    fn flush(&mut self) -> ();
    fn snapshot(&mut self) -> ();
    fn deliver(&mut self, point: Arc<Metric>) -> ();
    fn run(&mut self, recv: Receiver<Arc<server::Event>>) {
        for event in recv.iter() {
            match *event {
                server::Event::TimerFlush => self.flush(),
                server::Event::Snapshot => self.snapshot(),
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

/// Creates the collection of sinks based on the paraemeters
///
pub fn factory(args: Args) -> Vec<Sender<Arc<server::Event>>> {
    let mut sinks = Vec::with_capacity(3);

    if args.console {
        let (send, recv) = channel();
        thread::spawn(move || {
            console::Console::new().run(recv);
        });
        sinks.push(send);
    }
    if args.wavefront {
        let (send, recv) = channel();
        let wf_tags: String = args.tags.replace(",", " ");
        let cp_args = args.clone();
        thread::spawn(move || {
            wavefront::Wavefront::new(&cp_args.wavefront_host.unwrap(),
                                      cp_args.wavefront_port.unwrap(),
                                      wf_tags,
                                      cp_args.qos.clone())
                .run(recv);
        });
        sinks.push(send);
    }
    sinks
}
