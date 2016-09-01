extern crate toml;
extern crate clap;
extern crate quantiles;
extern crate lru_cache;
extern crate chrono;
extern crate string_cache;
extern crate fern;
extern crate dns_lookup;
extern crate notify;
#[macro_use]
extern crate log;
extern crate bincode;

use std::str;
use std::thread;
use chrono::UTC;

mod mpmc;
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
    pub mod wavefront;
}

use sinks::*;
use sink::Sink;

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
        output: vec![fern::OutputConfig::stdout()],
        level: level,
    };

    // In some running environments the logger will not initialize, such as
    // under OSX's Instruments.
    //
    //   IO Error: Permission denied (os error 13)
    //
    // No sense of why.
    let _ = fern::init_global_logger(logger_config, log::LogLevelFilter::Trace);

    let (event_send, event_recv) = mpmc::channel(&args.data_directory);

    info!("cernan - {}", args.version);
    let mut joins = Vec::new();

    if args.console {
        let console_recv = event_recv.clone();
        joins.push(thread::spawn(move || {
            console::Console::new().run(console_recv);
        }));
    }
    if args.wavefront {
        let wf_tags: String = args.tags.replace(",", " ");
        let cp_args = args.clone();
        let wf_recv = event_recv.clone();
        joins.push(thread::spawn(move || {
            wavefront::Wavefront::new(&cp_args.wavefront_host.unwrap(),
                                      cp_args.wavefront_port.unwrap(),
                                      wf_tags,
                                      cp_args.qos.clone())
                .run(wf_recv);
        }));
    }


    let sport = args.statsd_port;
    let statsd_send_v4 = event_send.clone();
    joins.push(thread::spawn(move || {
        server::udp_server_v4(statsd_send_v4, sport);
    }));
    let statsd_send_v6 = event_send.clone();
    joins.push(thread::spawn(move || {
        server::udp_server_v6(statsd_send_v6, sport);
    }));

    let gport = args.graphite_port;
    let graphite_send_v6 = event_send.clone();
    joins.push(thread::spawn(move || {
        server::tcp_server_ipv6(graphite_send_v6, gport);
    }));
    let graphite_send_v4 = event_send.clone();
    joins.push(thread::spawn(move || {
        server::tcp_server_ipv4(graphite_send_v4, gport);
    }));

    let flush_interval = args.flush_interval;
    let flush_send = event_send.clone();
    joins.push(thread::spawn(move || {
        server::flush_timer_loop(flush_send, flush_interval);
    }));

    match args.files {
        Some(log_files) => {
            for lf in log_files {
                let fp_send = event_send.clone();
                joins.push(thread::spawn(move || {
                    server::file_server(fp_send, lf);
                }));
            }
        }
        None => ()
    }

    joins.push(thread::spawn(move || {
        server::snapshot_loop(event_send);
    }));

    for jh in joins {
        jh.join().expect("Uh oh, child thread paniced!");
    }
}
