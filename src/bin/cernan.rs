extern crate chrono;
extern crate cernan;
extern crate fern;
#[macro_use]
extern crate log;
extern crate hopper;

use cernan::entry::Entry;
use cernan::filter::{Filter, ProgrammableFilterConfig};
use cernan::metric;
use cernan::sink;
use cernan::source::Source;
use cernan::util;
use chrono::UTC;
use std::collections::HashMap;
use std::process;
use std::str;
use std::thread;

fn populate_forwards(sends: &mut util::Channel,
                     forwards: &[String],
                     config_path: &str,
                     available_sends: &HashMap<String, hopper::Sender<metric::Event>>) {
    for fwd in forwards {
        match available_sends.get(fwd) {
            Some(snd) => {
                sends.push(snd.clone());
            }
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
    let mut sends: HashMap<String, hopper::Sender<metric::Event>> = HashMap::new();

    // SINKS
    //
    let flush_sends = Vec::new();

    let mut send_channels: HashMap<String, hopper::Sender<metric::Event>> = HashMap::new();
    let mut recv_channels: HashMap<String, hopper::Receiver<metric::Event>> = HashMap::new();
    let mut all_sinks: Vec<Box<Entry + Send>> = Vec::new();
    if let Some(config) = args.console {
        all_sinks.push(Box::new(cernan::sink::Console::new(config)));
    };
    if let Some(config) = args.null {
        all_sinks.push(Box::new(sink::Null::new(config)));
    };
    if let Some(config) = args.wavefront {
        all_sinks.push(Box::new(cernan::sink::Wavefront::new(config)));
    }
    if let Some(config) = args.prometheus {
        all_sinks.push(Box::new(cernan::sink::Prometheus::new(config)));
    }
    if let Some(config) = args.influxdb {
        all_sinks.push(Box::new(cernan::sink::InfluxDB::new(config)));
    }
    if let Some(config) = args.native_sink_config {
        all_sinks.push(Box::new(cernan::sink::Native::new(config)));
    }
    for config in &args.firehosen {
        all_sinks.push(Box::new(cernan::sink::Firehose::new(config.clone())));
    }

    for sink in &mut all_sinks {
        let (send, recv) =
            hopper::channel(sink.get_config().get_config_path(), &args.data_directory).unwrap();
        send_channels.insert(sink.get_config().get_config_path().clone(), send);
        recv_channels.insert(sink.get_config().get_config_path().clone(), recv);
    }

    let mut forward_channels: HashMap<String, Vec<hopper::Sender<metric::Event>>> = HashMap::new();
    while let Some(mut sink) = all_sinks.pop() {
        let mut forwards = Vec::new();
        populate_forwards(&mut forwards,
                          &sink.get_config().get_forwards(),
                          &sink.get_config().get_config_path(),
                          &send_channels);
        forward_channels.insert(sink.get_config().get_config_path().clone(),
                                forwards.clone());
        let recv = recv_channels.remove(sink.get_config().get_config_path()).unwrap();
        joins.push(thread::spawn(move || { sink.run1(forwards, recv); }));
    }

    // FILTERS
    //
    for config in args.filters.values() {
        let c: ProgrammableFilterConfig = (*config).clone();
        let (flt_send, flt_recv) = hopper::channel(&config.config_path, &args.data_directory)
            .unwrap();
        sends.insert(config.config_path.clone(), flt_send);
        let mut downstream_sends = Vec::new();
        populate_forwards(&mut downstream_sends,
                          &config.forwards,
                          &config.config_path,
                          &sends);
        joins.push(thread::spawn(move || {
            cernan::filter::ProgrammableFilter::new(c).run(flt_recv, downstream_sends);
        }));
    }

    // SOURCES
    //
    if let Some(config) = args.native_server_config {
        let mut native_server_send = Vec::new();
        populate_forwards(&mut native_server_send,
                          &config.forwards,
                          &config.config_path,
                          &sends);
        joins.push(thread::spawn(move || {
            cernan::source::NativeServer::new(native_server_send, config).run();
        }))
    }
    for config in args.statsds.values() {
        let c = (*config).clone();
        let mut statsd_sends = Vec::new();
        populate_forwards(&mut statsd_sends,
                          &config.forwards,
                          &config.config_path,
                          &sends);
        joins.push(thread::spawn(move || { cernan::source::Statsd::new(statsd_sends, c).run(); }));
    }

    for config in args.graphites.values() {
        let c = (*config).clone();
        let mut graphite_sends = Vec::new();
        populate_forwards(&mut graphite_sends,
                          &config.forwards,
                          &config.config_path,
                          &sends);
        joins.push(thread::spawn(move || {
            cernan::source::Graphite::new(graphite_sends, c).run();
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
    joins.push(thread::spawn(move || {
        cernan::source::FlushTimer::new(flush_sends, flush_interval).run();
    }));

    joins.push(thread::spawn(move || { cernan::time::update_time(); }));

    for jh in joins {
        // TODO Having sub-threads panic will not cause a bubble-up if that
        // thread is not the currently examined one. We're going to have to have
        // some manner of sub-thread communication going on.
        jh.join().expect("Uh oh, child thread paniced!");
    }
}
