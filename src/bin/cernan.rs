extern crate chrono;
extern crate cernan;
extern crate fern;
#[macro_use]
extern crate log;
extern crate hopper;

use cernan::filter::{Filter, ProgrammableFilterConfig};
use cernan::metric;
use cernan::sink::{FirehoseConfig, Sink};
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

fn propagate_flushes(flush_reachability: &mut HashMap<String, bool>, forwards: &[String]) {
    for fwd in forwards {
        flush_reachability.insert(fwd.clone(), true);
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
    let mut flush_reachability: HashMap<String, bool> = HashMap::new();

    // SINKS
    //
    let mut flush_channels: HashMap<String, hopper::Sender<metric::Event>> = HashMap::new();
    if let Some(config) = args.console {
        let (console_send, console_recv) =
            hopper::channel(&config.config_path, &args.data_directory).unwrap();
        flush_channels.insert(config.config_path.clone(), console_send.clone());
        sends.insert(config.config_path.clone(), console_send);
        flush_reachability.insert(config.config_path.clone(), false);
        joins.push(thread::spawn(move || {
            cernan::sink::Console::new(config).run(console_recv);
        }));
    }
    if let Some(config) = args.null {
        let (null_send, null_recv) = hopper::channel(&config.config_path, &args.data_directory)
            .unwrap();
        flush_channels.insert(config.config_path.clone(), null_send.clone());
        sends.insert(config.config_path.clone(), null_send);
        flush_reachability.insert(config.config_path.clone(), false);
        joins.push(thread::spawn(move || { cernan::sink::Null::new(config).run(null_recv); }));
    }
    if let Some(config) = args.wavefront {
        let (wf_send, wf_recv) = hopper::channel(&config.config_path, &args.data_directory)
            .unwrap();
        flush_channels.insert(config.config_path.clone(), wf_send.clone());
        sends.insert(config.config_path.clone(), wf_send);
        flush_reachability.insert(config.config_path.clone(), false);
        joins.push(thread::spawn(move || { cernan::sink::Wavefront::new(config).run(wf_recv); }));
    }
    if let Some(config) = args.prometheus {
        let (prometheus_send, prometheus_recv) =
            hopper::channel(&config.config_path, &args.data_directory).unwrap();
        flush_channels.insert(config.config_path.clone(), prometheus_send.clone());
        sends.insert(config.config_path.clone(), prometheus_send);
        flush_reachability.insert(config.config_path.clone(), false);
        joins.push(thread::spawn(move || {
            cernan::sink::Prometheus::new(config).run(prometheus_recv);
        }));
    }
    if let Some(config) = args.influxdb {
        let (flx_send, flx_recv) = hopper::channel(&config.config_path, &args.data_directory)
            .unwrap();
        flush_channels.insert(config.config_path.clone(), flx_send.clone());
        sends.insert(config.config_path.clone(), flx_send);
        flush_reachability.insert(config.config_path.clone(), false);
        joins.push(thread::spawn(move || { cernan::sink::InfluxDB::new(config).run(flx_recv); }));
    }
    if let Some(config) = args.native_sink_config {
        let (cernan_send, cernan_recv) = hopper::channel(&config.config_path, &args.data_directory)
            .unwrap();
        flush_channels.insert(config.config_path.clone(), cernan_send.clone());
        sends.insert(config.config_path.clone(), cernan_send);
        flush_reachability.insert(config.config_path.clone(), false);
        joins.push(thread::spawn(move || { cernan::sink::Native::new(config).run(cernan_recv); }));
    }
    for config in &args.firehosen {
        let f: FirehoseConfig = config.clone();
        let (firehose_send, firehose_recv) =
            hopper::channel(&config.config_path, &args.data_directory).unwrap();
        flush_channels.insert(config.config_path.clone(), firehose_send.clone());
        sends.insert(config.config_path.clone(), firehose_send);
        flush_reachability.insert(config.config_path.clone(), false);
        joins.push(thread::spawn(move || { cernan::sink::Firehose::new(f).run(firehose_recv); }));
    }

    // FILTERS
    //
    for config in args.filters.values() {
        let c: ProgrammableFilterConfig = (*config).clone();
        let (flt_send, flt_recv) = hopper::channel(&config.config_path, &args.data_directory)
            .unwrap();
        sends.insert(config.config_path.clone(), flt_send);
        flush_reachability.insert(config.config_path.clone(), true);
        let mut downstream_sends = Vec::new();
        populate_forwards(&mut downstream_sends,
                          &config.forwards,
                          &config.config_path,
                          &sends);
        propagate_flushes(&mut flush_reachability, &config.forwards);
        joins.push(thread::spawn(move || {
            cernan::filter::ProgrammableFilter::new(c).run(flt_recv, downstream_sends);
        }));
    }

    let mut flush_sends: util::Channel = Vec::new();
    for (config_path, reachable) in &flush_reachability {
        if !reachable {
            match flush_channels.get(config_path) {
                Some(channel) => flush_sends.push(channel.clone()),
                None => {
                    error!("Can't find sink channel to propagate flush: {} => {}",
                           config_path,
                           reachable);
                    process::exit(0);
                }
            }
        }
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

    let flush_interval = 1;
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
