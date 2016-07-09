#[macro_use]
extern crate clap;
extern crate quantiles;
extern crate hyper;
extern crate lru_cache;
extern crate mime;
extern crate rustc_serialize;
extern crate chrono;
extern crate url;
extern crate regex;
extern crate string_cache;

use std::str;
use std::sync::mpsc::channel;
use std::thread;
use std::str::FromStr;

mod backend;
mod buckets;
mod cli;
mod metric;
mod metrics {
    pub mod statsd;
}
mod server;
mod backends {
    pub mod console;
    pub mod librato;
    pub mod wavefront;
}

fn main() {
    let args = cli::parse_args();

    let mut backends = backend::factory(&args);

    let (event_send, event_recv) = channel();
    let flush_send = event_send.clone();
    let udp_send = event_send.clone();

    let port = u16::from_str(args.value_of("port").unwrap()).unwrap();
    thread::spawn(move || {
        server::udp_server(udp_send, port);
    });

    let flush_interval = u64::from_str(args.value_of("flush-interval").unwrap()).unwrap();
    thread::spawn(move || {
        server::flush_timer_loop(flush_send, flush_interval);
    });

    loop {
        let result = match event_recv.recv() {
            Ok(res) => res,
            Err(e) => panic!(format!("Event channel has hung up: {:?}", e)),
        };

        match result {
            server::Event::TimerFlush => {
                // TODO improve this, limit here will be backend stalling and
                // holding up all others
                for backend in &mut backends {
                    backend.flush();
                }
            }

            server::Event::UdpMessage(buf) => {
                str::from_utf8(&buf)
                    .map(|val| {
                        match metric::Metric::parse(val) {
                            Some(metrics) => {
                                for metric in &metrics {
                                    for backend in &mut backends {
                                        backend.deliver(metric.clone());
                                    }
                                }
                                Ok(metrics.len())
                            }
                            None => {
                                println!("BAD PACKET: {:?}", val);
                                Err("could not interpret")
                            }
                        }
                    })
                    .ok();
            }
        }
    }
}
