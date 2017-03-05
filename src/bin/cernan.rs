extern crate chrono;
extern crate cernan;
extern crate fern;
#[macro_use]
extern crate log;
extern crate hopper;

use cernan::entry::Entry;
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
    let sends: HashMap<String, hopper::Sender<metric::Event>> = HashMap::new();

    let flush_sends = Vec::new();

    let mut send_channels: HashMap<String, hopper::Sender<metric::Event>> = HashMap::new();
    let mut recv_channels: HashMap<String, hopper::Receiver<metric::Event>> = HashMap::new();
    let mut all_entries: Vec<Box<Entry + Send>> = Vec::new();

    // SINKS
    if let Some(config) = args.console {
        all_entries.push(Box::new(cernan::sink::Console::new(config)));
    };
    if let Some(config) = args.null {
        all_entries.push(Box::new(sink::Null::new(config)));
    };
    if let Some(config) = args.wavefront {
        all_entries.push(Box::new(cernan::sink::Wavefront::new(config)));
    }
    if let Some(config) = args.prometheus {
        all_entries.push(Box::new(cernan::sink::Prometheus::new(config)));
    }
    if let Some(config) = args.influxdb {
        all_entries.push(Box::new(cernan::sink::InfluxDB::new(config)));
    }
    if let Some(config) = args.native_sink_config {
        all_entries.push(Box::new(cernan::sink::Native::new(config)));
    }
    for config in &args.firehosen {
        all_entries.push(Box::new(cernan::sink::Firehose::new(config.clone())));
    }

    // FILTERS
    for config in args.filters.values() {
        all_entries.push(Box::new(cernan::filter::ProgrammableFilter::new(config.clone())));
    }

    // SOURCES
    if let Some(config) = args.native_server_config {
        all_entries.push(Box::new(cernan::source::NativeServer::new(config)));
    }
    for config in args.statsds.values() {
        all_entries.push(Box::new(cernan::source::Statsd::new(config.clone())));
    }
    for config in args.graphites.values() {
        all_entries.push(Box::new(cernan::source::Graphite::new(config.clone())));
    }


    for sink in &mut all_entries {
        let (send, recv) =
            hopper::channel(sink.get_config().get_config_path(), &args.data_directory).unwrap();
        send_channels.insert(sink.get_config().get_config_path().clone(), send);
        recv_channels.insert(sink.get_config().get_config_path().clone(), recv);
    }

    let mut forward_channels: HashMap<String, Vec<hopper::Sender<metric::Event>>> = HashMap::new();
    while let Some(mut sink) = all_entries.pop() {
        let mut forwards = Vec::new();
        populate_forwards(&mut forwards,
                          &sink.get_config().get_forwards(),
                          &sink.get_config().get_config_path(),
                          &send_channels);
        forward_channels.insert(sink.get_config().get_config_path().clone(),
                                forwards.clone());
        let recv = recv_channels.remove(sink.get_config().get_config_path()).unwrap(); // removing to move ownership
        joins.push(thread::spawn(move || { sink.run1(forwards, recv); }));
    }

    //
    for config in args.files {
        let mut fp_sends = Vec::new();
        populate_forwards(&mut fp_sends, &config.forwards, &config.config_path, &sends);
        joins.push(thread::spawn(move || {
            cernan::source::FileServer::new(config).run(fp_sends);
        }));
    }

    // BACKGROUND
    //

    let flush_interval = args.flush_interval;
    joins.push(thread::spawn(move || {
        cernan::source::FlushTimer::new(flush_interval).run(flush_sends);
    }));

    joins.push(thread::spawn(move || { cernan::time::update_time(); }));

    for jh in joins {
        // TODO Having sub-threads panic will not cause a bubble-up if that
        // thread is not the currently examined one. We're going to have to have
        // some manner of sub-thread communication going on.
        jh.join().expect("Uh oh, child thread paniced!");
    }
}
