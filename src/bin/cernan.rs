extern crate chrono;
extern crate cernan;
extern crate fern;
#[macro_use]
extern crate log;

use std::str;
use std::thread;
use chrono::UTC;

use cernan::source::Source;
use cernan::filter::Filter;
use cernan::sink::{Sink, FirehoseConfig};

fn main() {
    let args = cernan::config::parse_args();

    let level = match args.verbose {
        0 => log::LogLevelFilter::Error,
        1 => log::LogLevelFilter::Warn,
        2 => log::LogLevelFilter::Info,
        3 => log::LogLevelFilter::Debug,
        _ => log::LogLevelFilter::Trace,
    };

    let logger_config = fern::DispatchConfig {
        format: Box::new(|msg: &str, level: &log::LogLevel, location: &log::LogLocation| {
            format!("[{}][{}][{}][{}] {}",
                    location.module_path(),
                    location.line(),
                    UTC::now().to_rfc3339(),
                    level,
                    msg)
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

    info!("cernan - {}", args.version);
    let mut joins = Vec::new();
    let mut sends = Vec::new();

    // SINKS
    //
    if let Some(config) = args.console {
        let (console_send, console_recv) = cernan::mpsc::channel("console", &args.data_directory);
        sends.push(console_send);
        joins.push(thread::spawn(move || {
            cernan::sink::Console::new(config).run(console_recv);
        }));
    }
    if let Some(config) = args.null {
        let (null_send, null_recv) = cernan::mpsc::channel("null", &args.data_directory);
        sends.push(null_send);
        joins.push(thread::spawn(move || {
            cernan::sink::Null::new(config).run(null_recv);
        }));
    }
    if let Some(config) = args.wavefront {
        let (wf_send, wf_recv) = cernan::mpsc::channel("wf", &args.data_directory);
        sends.push(wf_send);
        joins.push(thread::spawn(move || {
            cernan::sink::Wavefront::new(config).run(wf_recv);
        }));
    }
    if let Some(config) = args.fed_transmitter {
        let (cernan_send, cernan_recv) = cernan::mpsc::channel("cernan", &args.data_directory);
        sends.push(cernan_send);
        joins.push(thread::spawn(move || {
            cernan::sink::FederationTransmitter::new(config).run(cernan_recv);
        }));
    }

    for fh in &args.firehosen {
        let f: FirehoseConfig = fh.clone();
        let ds: String = f.delivery_stream.clone();
        let (firehose_send, firehose_recv) = cernan::mpsc::channel(&ds, &args.data_directory);
        sends.push(firehose_send);
        joins.push(thread::spawn(move || {
            cernan::sink::Firehose::new(f).run(firehose_recv);
        }));
    }

    // SOURCES
    //

    if let Some(config) = args.fed_receiver_config {
        let receiver_server_send = sends.clone();
        joins.push(thread::spawn(move || {
            cernan::source::FederationReceiver::new(receiver_server_send, config).run();
        }))
    }

    if let Some(config) = args.statsd_config {
        let statsd_sends = sends.clone();
        joins.push(thread::spawn(move || {
            cernan::source::Statsd::new(statsd_sends, config).run();
        }));
    }

    let (graphite_scrub_send, graphite_scrub_recv) = cernan::mpsc::channel("graphite_scrub",
                                                                           &args.data_directory);
    let graphite_sends = sends.clone();
    joins.push(thread::spawn(move || {
        cernan::filter::CollectdScrub::new("foo").run(graphite_scrub_recv, graphite_sends)
    }));
    if let Some(config) = args.graphite_config {
        joins.push(thread::spawn(move || {
            cernan::source::Graphite::new(vec![graphite_scrub_send], config).run();
        }));
    }

    for config in args.files {
        let fp_sends = sends.clone();
        joins.push(thread::spawn(move || {
            cernan::source::FileServer::new(fp_sends, config).run();
        }));
    }

    // BACKGROUND
    //

    let flush_interval = args.flush_interval;
    let flush_interval_sends = sends.clone();
    joins.push(thread::spawn(move || {
        cernan::source::FlushTimer::new(flush_interval_sends, flush_interval).run();
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
