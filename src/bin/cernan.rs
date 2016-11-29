extern crate chrono;
extern crate cernan;
extern crate fern;
#[macro_use]
extern crate log;

use std::str;
use std::thread;
use std::process;
use std::collections::HashMap;
use chrono::UTC;

use cernan::source::Source;
use cernan::filter::{Filter, ProgrammableFilterConfig};
use cernan::sink::{Sink, FirehoseConfig};
use cernan::mpsc;
use cernan::metric;

fn populate_forwards(sends: &mut Vec<mpsc::Sender<metric::Event>>,
                     forwards: &[String],
                     config_path: &str,
                     available_sends: &HashMap<String, mpsc::Sender<metric::Event>>) {
    for fwd in forwards {
        match available_sends.get(fwd) {
            Some(snd) => sends.push(snd.clone()),
            None => {
                error!("Unable to fulfill configured forward: {} => {}",
                       config_path,
                       fwd);
                process::exit(0);
            }
        }
    }
}

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
    let mut sends = HashMap::new();

    // SINKS
    //
    if let Some(config) = args.console {
        let (console_send, console_recv) = cernan::mpsc::channel(&config.config_path,
                                                                 &args.data_directory);
        sends.insert(config.config_path.clone(), console_send);
        joins.push(thread::spawn(move || {
            cernan::sink::Console::new(config).run(console_recv);
        }));
    }
    if let Some(config) = args.null {
        let (null_send, null_recv) = cernan::mpsc::channel(&config.config_path,
                                                           &args.data_directory);
        sends.insert(config.config_path.clone(), null_send);
        joins.push(thread::spawn(move || {
            cernan::sink::Null::new(config).run(null_recv);
        }));
    }
    if let Some(config) = args.wavefront {
        let (wf_send, wf_recv) = cernan::mpsc::channel(&config.config_path, &args.data_directory);
        sends.insert(config.config_path.clone(), wf_send);
        joins.push(thread::spawn(move || {
            cernan::sink::Wavefront::new(config).run(wf_recv);
        }));
    }
    if let Some(config) = args.fed_transmitter {
        let (cernan_send, cernan_recv) = cernan::mpsc::channel(&config.config_path,
                                                               &args.data_directory);
        sends.insert(config.config_path.clone(), cernan_send);
        joins.push(thread::spawn(move || {
            cernan::sink::FederationTransmitter::new(config).run(cernan_recv);
        }));
    }

    for config in &args.firehosen {
        let f: FirehoseConfig = config.clone();
        let (firehose_send, firehose_recv) = cernan::mpsc::channel(&config.config_path,
                                                                   &args.data_directory);
        sends.insert(config.config_path.clone(), firehose_send);
        joins.push(thread::spawn(move || {
            cernan::sink::Firehose::new(f).run(firehose_recv);
        }));
    }

    // FILTERS
    //
    for config in args.filters.values() {
        let c: ProgrammableFilterConfig = (*config).clone();
        let (flt_send, flt_recv) = cernan::mpsc::channel(&config.config_path, &args.data_directory);
        sends.insert(config.config_path.clone(), flt_send);
        let mut upstream_sends = Vec::new();
        populate_forwards(&mut upstream_sends,
                          &config.forwards,
                          &config.config_path,
                          &sends);
        joins.push(thread::spawn(move || {
            cernan::filter::ProgrammableFilter::new(c).run(flt_recv, upstream_sends);
        }));
    }

    // SOURCES
    //

    if let Some(config) = args.fed_receiver_config {
        let mut receiver_server_send = Vec::new();
        populate_forwards(&mut receiver_server_send,
                          &config.forwards,
                          &config.config_path,
                          &sends);
        joins.push(thread::spawn(move || {
            cernan::source::FederationReceiver::new(receiver_server_send, config).run();
        }))
    }

    if let Some(config) = args.statsd_config {
        let mut statsd_sends = Vec::new();
        populate_forwards(&mut statsd_sends,
                          &config.forwards,
                          &config.config_path,
                          &sends);
        joins.push(thread::spawn(move || {
            cernan::source::Statsd::new(statsd_sends, config).run();
        }));
    }

    if let Some(config) = args.graphite_config {
        let mut graphite_sends = Vec::new();
        populate_forwards(&mut graphite_sends,
                          &config.forwards,
                          &config.config_path,
                          &sends);
        joins.push(thread::spawn(move || {
            cernan::source::Graphite::new(graphite_sends, config).run();
        }));
    }

    for config in args.files {
        let mut fp_sends = Vec::new();
        populate_forwards(&mut fp_sends, &config.forwards, &config.config_path, &sends);
        joins.push(thread::spawn(move || {
            cernan::source::FileServer::new(fp_sends, config).run();
        }));
    }

    // BACKGROUND
    //

    let flush_interval = args.flush_interval;
    let mut flush_interval_sends = Vec::new();
    for snd in sends.values() {
        flush_interval_sends.push(snd.clone());
    }
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
