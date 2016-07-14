extern crate toml;
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
extern crate fern;
#[macro_use]
extern crate log;

use std::str;
use std::sync::mpsc::channel;
use std::thread;
use chrono::UTC;

mod backend;
mod buckets;
mod config;
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
    let args = config::parse_args();

    let level = match args.verbose {
        0 => log::LogLevelFilter::Error,
        1 => log::LogLevelFilter::Warn,
        2 => log::LogLevelFilter::Info,
        3 => log::LogLevelFilter::Debug,
        _ => log::LogLevelFilter::Trace,
    };

    let logger_config = fern::DispatchConfig {
        format: Box::new(|msg: &str, level: &log::LogLevel, _location: &log::LogLocation| {
            format!("[{}][{}] {}", level, UTC::now().to_rfc3339(), msg)
        }),
        output: vec![fern::OutputConfig::stdout(), fern::OutputConfig::file("output.log")],
        level: level,
    };

    // In some running environments the logger will not initialize, such as
    // under OSX's Instruments.
    //
    //   IO Error: Permission denied (os error 13)
    //
    // No sense of why.
    let _ = fern::init_global_logger(logger_config, log::LogLevelFilter::Trace);

    debug!("ARGS: {:?}", args);

    info!("cernan - {}", args.version);

    trace!("trace messages enabled");
    debug!("debug messages enabled");
    info!("info messages enabled");
    warn!("warning messages enabled");
    error!("error messages enabled");

    let mut backends = backend::factory(args.clone());
    debug!("total backends: {}", backends.len());

    let (event_send, event_recv) = channel();
    let flush_send = event_send.clone();
    let statsd_send = event_send.clone();
    let graphite_send = event_send.clone();

    let sport = args.statsd_port;
    thread::spawn(move || {
        server::udp_server(statsd_send, sport);
    });

    let gport = args.graphite_port;
    thread::spawn(move || {
        server::tcp_server(graphite_send, gport);
    });

    let flush_interval = args.flush_interval;
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
                trace!("TimerFlush");
                // TODO improve this, limit here will be backend stalling and
                // holding up all others
                for backend in &mut backends {
                    backend.flush();
                }
            }

            server::Event::TcpMessage(buf) => {
                str::from_utf8(&buf)
                    .map(|val| {
                        debug!("graphite - {}", val);
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
                                error!("BAD PACKET: {:?}", val);
                                Err("could not interpret")
                            }
                        }
                    })
                    .ok();
            }

            server::Event::UdpMessage(buf) => {
                str::from_utf8(&buf)
                    .map(|val| {
                        debug!("statsd - {}", val);
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
