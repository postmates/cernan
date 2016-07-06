// see bench_prs comment
// #![feature(test)]
// #[cfg(test)]
// extern crate test;

extern crate docopt;
extern crate quantiles;
extern crate hyper;
extern crate lru_cache;
extern crate mime;
extern crate rustc_serialize;
extern crate chrono;
extern crate url;
extern crate regex;

use std::str;
use std::sync::mpsc::channel;
use std::thread;
use std::process::exit;

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

const VERSION: Option<&'static str> = option_env!("CARGO_PKG_VERSION");

fn main() {
    let args = cli::parse_args();

    if args.flag_version {
        println!("cernan - {}", VERSION.unwrap_or("unknown"));
        exit(0);
    }

    let mut backends = backend::factory(&args.flag_console,
                                        &args.flag_wavefront,
                                        &args.flag_librato,
                                        &args.flag_tags,
                                        &args.flag_wavefront_host,
                                        &args.flag_wavefront_port,
                                        &args.flag_librato_username,
                                        &args.flag_librato_token,
                                        &args.flag_librato_host);

    let (event_send, event_recv) = channel();
    let flush_send = event_send.clone();
    let udp_send = event_send.clone();

    println!("Starting cernan");
    println!("Data server on 0.0.0.0:{}", args.flag_port);

    let port = args.flag_port;
    thread::spawn(move || {
        server::udp_server(udp_send, port);
    });

    // Run the timer that flushes metrics to the backends.
    let flush_interval = args.flag_flush_interval;
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
                for backend in backends.iter_mut() {
                    backend.flush();
                }
            }

            server::Event::UdpMessage(buf) => {
                str::from_utf8(&buf)
                    .map(|val| {
                        match metric::Metric::parse(val) {
                            Some(metrics) => {
                                for metric in &metrics {
                                    for backend in backends.iter_mut() {
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
