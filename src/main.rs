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
extern crate dns_lookup;
#[macro_use]
extern crate log;

use std::str;
use std::sync::Arc;
use std::sync::mpsc::channel;
use std::thread;
use chrono::UTC;

mod sink;
mod buckets;
mod config;
mod metric;
mod metrics {
    pub mod statsd;
    pub mod graphite;
}
mod server;
mod sinks {
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

    let mut sinks = sink::factory(args.clone());
    debug!("total sinks: {}", sinks.len());

    let (event_send, event_recv) = channel();
    let flush_send = event_send.clone();
    let statsd_send_v4 = event_send.clone();
    let statsd_send_v6 = event_send.clone();
    let graphite_send_v4 = event_send.clone();
    let graphite_send_v6 = event_send.clone();

    let sport = args.statsd_port;
    thread::spawn(move || {
        server::udp_server_v4(statsd_send_v4, sport);
    });
    thread::spawn(move || {
        server::udp_server_v6(statsd_send_v6, sport);
    });

    let gport = args.graphite_port;
    thread::spawn(move || {
        server::tcp_server_ipv6(graphite_send_v6, gport);
    });
    thread::spawn(move || {
        server::tcp_server_ipv4(graphite_send_v4, gport);
    });

    let flush_interval = args.flush_interval;
    thread::spawn(move || {
        server::flush_timer_loop(flush_send, flush_interval);
    });

    loop {
        match event_recv.recv() {
            Ok(result) => {
                let arc_res = Arc::new(result);
                for sink in &mut sinks {
                    let ar = arc_res.clone();
                    match sink.send(ar).err() {
                        None => continue,
                        Some(e) => panic!("Hung up! {}", e),
                    }
                }
            }
            Err(e) => panic!(format!("Event channel has hung up: {:?}", e)),
        }
    }
}
