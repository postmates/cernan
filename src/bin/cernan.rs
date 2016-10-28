extern crate chrono;
#[macro_use]
pub extern crate slog;
extern crate slog_stdlog;
extern crate slog_term;

use slog::DrainExt;

extern crate cernan;

use std::str;
use std::thread;
use chrono::UTC;

use cernan::source::Source;
use cernan::sink::Sink;

fn main() {
    let args = cernan::config::parse_args();

    // let level = match args.verbose {
    //     0 => log::LogLevelFilter::Error,
    //     1 => log::LogLevelFilter::Warn,
    //     2 => log::LogLevelFilter::Info,
    //     3 => log::LogLevelFilter::Debug,
    //     _ => log::LogLevelFilter::Trace,
    // };

    // let logger_config = fern::DispatchConfig {
    //     format: Box::new(|msg: &str, level: &log::LogLevel, _location: &log::LogLocation| {
    //         format!("[{}][{}] {}", level, UTC::now().to_rfc3339(), msg)
    //     }),
    //     output: vec![fern::OutputConfig::stdout()],
    //     level: level,
    // };

    // In some running environments the logger will not initialize, such as
    // under OSX's Instruments.
    //
    //   IO Error: Permission denied (os error 13)
    //
    // No sense of why.
    let drain = slog_term::streamer().async().full().build();
    let root = slog::Logger::root(drain.fuse(), o!("version" => env!("CARGO_PKG_VERSION")));

    info!(root, "cernan - {}", args.version);
    let mut joins = Vec::new();
    let mut sends = Vec::new();

    // SINKS
    //
    if args.console {
        let (console_send, console_recv) = cernan::mpsc::channel("console", &args.data_directory);
        sends.push(console_send);
        let bin_width = args.console_bin_width;
        joins.push(thread::spawn(move || {
            cernan::sink::console::Console::new(bin_width).run(console_recv);
        }));
    }
    if args.null {
        let (null_send, null_recv) = cernan::mpsc::channel("null", &args.data_directory);
        sends.push(null_send);
        joins.push(thread::spawn(move || {
            cernan::sink::Null::new().run(null_recv);
        }));
    }
    if args.wavefront {
        let cp_args = args.clone();
        let (wf_send, wf_recv) = cernan::mpsc::channel("wf", &args.data_directory);
        sends.push(wf_send);
        joins.push(thread::spawn(move || {
            cernan::sink::Wavefront::new(&cp_args.wavefront_host.unwrap(),
                                         cp_args.wavefront_port.unwrap(),
                                         cp_args.tags.clone(),
                                         cp_args.qos.clone(),
                                         cp_args.flush_interval as i64)
                .run(wf_recv);
        }));
    }

    for ds in &args.firehose_delivery_streams {
        let fh_name = ds.clone();
        let (firehose_send, firehose_recv) = cernan::mpsc::channel(&fh_name, &args.data_directory);
        sends.push(firehose_send);
        joins.push(thread::spawn(move || {
            cernan::sink::Firehose::new(&fh_name).run(firehose_recv);
        }));
    }

    let args_fedtrn = args.clone();
    if args_fedtrn.fed_transmitter {
        let (cernan_send, cernan_recv) = cernan::mpsc::channel("cernan", &args.data_directory);
        sends.push(cernan_send);
        joins.push(thread::spawn(move || {
            cernan::sink::FederationTransmitter::new(args_fedtrn.fed_transmitter_port.unwrap(),
                                                     args_fedtrn.fed_transmitter_host.unwrap())
                .run(cernan_recv);
        }));
    }

    // SOURCES
    //

    let args_fedrcv = args.clone();
    if let Some(crcv_port) = args_fedrcv.fed_receiver_port {
        let crcv_ip = args_fedrcv.fed_receiver_ip.unwrap();
        let fed_tags = args_fedrcv.tags;
        let receiver_server_send = sends.clone();
        joins.push(thread::spawn(move || {
            cernan::source::FederationReceiver::new(receiver_server_send,
                                                    crcv_ip,
                                                    crcv_port,
                                                    fed_tags)
                .run();
        }));
    }

    let stags = args.tags.clone();
    if let Some(sport) = args.statsd_port {
        let udp_server_v4_send = sends.clone();
        let stags_v4 = stags.clone();
        joins.push(thread::spawn(move || {
            cernan::source::Statsd::new(udp_server_v4_send, sport, stags_v4).run();
        }));
        let udp_server_v6_send = sends.clone();
        joins.push(thread::spawn(move || {
            cernan::source::Statsd::new(udp_server_v6_send, sport, stags).run();
        }));
    }


    let gtags = args.tags.clone();
    if let Some(gport) = args.graphite_port {
        let tcp_server_ipv6_sends = sends.clone();
        let gtags_v4 = gtags.clone();
        joins.push(thread::spawn(move || {
            cernan::source::Graphite::new(tcp_server_ipv6_sends, gport, gtags_v4).run();
        }));
        let tcp_server_ipv4_sends = sends.clone();
        joins.push(thread::spawn(move || {
            cernan::source::Graphite::new(tcp_server_ipv4_sends, gport, gtags).run();
        }));
    }

    if let Some(log_files) = args.files {
        for lf in log_files {
            let fp_sends = sends.clone();
            let ftags = args.tags.clone();
            joins.push(thread::spawn(move || {
                cernan::source::FileServer::new(fp_sends, lf, ftags);
            }));
        }
    }

    // BACKGROUND
    //

    let flush_interval = args.flush_interval;
    let flush_interval_sends = sends.clone();
    joins.push(thread::spawn(move || {
        cernan::source::FlushTimer::new(flush_interval_sends, flush_interval);
    }));

    joins.push(thread::spawn(move || {
        cernan::time::update_time();
    }));

    for jh in joins {
        // TODO Having sub-threads panic will not cause a bubble-up if that
        // thread is not the currently examined one. We're going to have to have
        // some manner of sub-thread communication going on.
        jh.join().expect("Uh oh, child thread paniced!");
    }
}
