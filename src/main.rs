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
    pub mod graphite;
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
    let statsd_send = event_send.clone();
    let graphite_send = event_send.clone();

    let sport = u16::from_str(args.value_of("statsd-port").unwrap()).unwrap();
    thread::spawn(move || {
        server::udp_server(statsd_send, sport);
    });

    let gport = u16::from_str(args.value_of("graphite-port").unwrap()).unwrap();
    thread::spawn(move || {
        server::tcp_server(graphite_send, gport);
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

            server::Event::TcpMessage(buf) => {
                str::from_utf8(&buf)
                    .map(|val| {
                        match metric::Metric::parse_graphite(val) {
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

            server::Event::UdpMessage(buf) => {
                str::from_utf8(&buf)
                    .map(|val| {
                        match metric::Metric::parse_statsd(val) {
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
