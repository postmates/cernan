#![allow(unknown_lints)]

extern crate cernan;
extern crate chan_signal;
extern crate chrono;
extern crate fern;
extern crate hopper;
extern crate mio;

#[macro_use]
extern crate log;
extern crate openssl_probe;

use cernan::constants;
use cernan::filter::{DelayFilterConfig, Filter, FlushBoundaryFilterConfig,
                     ProgrammableFilterConfig};
use cernan::metric;
use cernan::sink::Sink;
use cernan::source::Source;
use cernan::util;
use chrono::Utc;
use std::collections::{HashMap, HashSet};
use std::mem;
use std::process;
use std::str;
use std::thread;

#[derive(Debug)]
struct SourceWorker {
    thread: std::thread::JoinHandle<()>,
    readiness: mio::SetReadiness,
}

#[derive(Debug)]
struct SinkWorker {
    thread: std::thread::JoinHandle<()>,
    sender: hopper::Sender<metric::Event>,
}

fn populate_forwards(
    sends: &mut util::Channel,
    mut top_level_forwards: Option<&mut HashSet<String>>,
    forwards: &[String],
    config_path: &str,
    available_sends: &HashMap<String, hopper::Sender<metric::Event>>,
) {
    for fwd in forwards {
        if let Some(tlf) = top_level_forwards.as_mut() {
            let _ = (*tlf).insert(fwd.clone());
        }
        match available_sends.get(fwd) {
            Some(snd) => {
                sends.push(snd.clone());
            }
            None => {
                error!(
                    "Unable to fulfill configured forward: {} => {}",
                    config_path, fwd
                );
                process::exit(0);
            }
        }
    }
}

fn join_all(workers: HashMap<String, SinkWorker>) {
    for (_worker_id, worker) in workers {
        worker.thread.join().expect("Failed to join worker");
    }
}

fn broadcast_shutdown(workers: &HashMap<String, SinkWorker>) {
    let mut source_channels = Vec::new();
    for (id, worker) in workers.iter() {
        println!("Signaling shutdown to {:?}", id);
        source_channels.push(worker.sender.clone());
    }

    if !source_channels.is_empty() {
        cernan::util::send(&mut source_channels, cernan::metric::Event::Shutdown);
    }

    drop(source_channels);
}

macro_rules! cfg_conf {
    ($config:ident) => {
        $config.config_path.clone().expect("[INTERNAL ERROR] no config_path")
    }
}

#[allow(cyclomatic_complexity)]
fn main() {
    openssl_probe::init_ssl_cert_env_vars();

    let mut args = cernan::config::parse_args();

    let level = match args.verbose {
        0 => log::LogLevelFilter::Error,
        1 => log::LogLevelFilter::Warn,
        2 => log::LogLevelFilter::Info,
        3 => log::LogLevelFilter::Debug,
        _ => log::LogLevelFilter::Trace,
    };

    let signal =
        chan_signal::notify(&[chan_signal::Signal::INT, chan_signal::Signal::TERM]);

    fern::Dispatch::new()
        .format(|out, message, record| {
            out.finish(format_args!(
                "[{}][{}][{}][{}] {}",
                record.location().module_path(),
                record.location().line(),
                Utc::now().to_rfc3339(),
                record.level(),
                message
            ))
        })
        .level(level)
        .chain(std::io::stdout())
        .apply()
        .expect("could not set up logging");

    info!("cernan - {}", args.version);

    // We track the various child threads in order to support graceful shutdown.
    // There are currently two paths used to communicate shutdown:
    //
    // 1) A semaphore is used to signal shutdown to sources.  As generates of
    //    events, sources are shutdown first to ensure at least once processing.
    //
    // 2) metrics::Event::Shutdown is sent to Hopper channels for Filters and Sinks
    //    after all sources have shutdown.  Shutdown events serve to bookend queued
    //    events, once read it is safe for these workers to flush any pending writes
    //    and shutdown.
    let mut sources: HashMap<String, SourceWorker> = HashMap::new();
    let mut sinks: HashMap<String, SinkWorker> = HashMap::new();
    let mut filters: HashMap<String, SinkWorker> = HashMap::new();
    let mut senders: HashMap<String, hopper::Sender<metric::Event>> = HashMap::new();
    let mut receivers: HashMap<String, hopper::Receiver<metric::Event>> =
        HashMap::new();
    let mut flush_sends = HashSet::new();

    let mut config_topology: HashMap<String, Vec<String>> = HashMap::new();

    // We have to build up the mapping from source / filter / sink to its
    // forwards. We do that here. Once completed we'll have populated:
    //
    //  * senders
    //  * receivers
    //  * config_topology
    //
    // config_topology gives us the whole graph as requested by the user. We'll
    // use this to pull information from senders / receivers when we start
    // spinning up threads. We DO NOT check that config_topology has sufficient
    // information while we build it, otherwise users would have to carefully
    // order their configuration. That would stink.
    //
    // SINKS
    if let Some(ref config) = args.null {
        let (send, recv) = hopper::channel_with_max_bytes(
            &config.config_path,
            &args.data_directory,
            args.max_hopper_queue_bytes,
        ).unwrap();
        senders.insert(config.config_path.clone(), send);
        receivers.insert(config.config_path.clone(), recv);
        config_topology.insert(config.config_path.clone(), Default::default());
    }
    if let Some(ref config) = args.console {
        let config_path = cfg_conf!(config);
        let (send, recv) = hopper::channel_with_max_bytes(
            &config_path,
            &args.data_directory,
            args.max_hopper_queue_bytes,
        ).unwrap();
        senders.insert(config_path.clone(), send);
        receivers.insert(config_path.clone(), recv);
        config_topology.insert(config_path.clone(), Default::default());
    }
    if let Some(ref config) = args.wavefront {
        let config_path = cfg_conf!(config);
        let (send, recv) = hopper::channel_with_max_bytes(
            &config_path,
            &args.data_directory,
            args.max_hopper_queue_bytes,
        ).unwrap();
        senders.insert(config_path.clone(), send);
        receivers.insert(config_path.clone(), recv);
        config_topology.insert(config_path.clone(), Default::default());
    }
    if let Some(ref config) = args.prometheus {
        let config_path = cfg_conf!(config);
        let (send, recv) = hopper::channel_with_max_bytes(
            &config_path,
            &args.data_directory,
            args.max_hopper_queue_bytes,
        ).unwrap();
        senders.insert(config_path.clone(), send);
        receivers.insert(config_path.clone(), recv);
        config_topology.insert(config_path.clone(), Default::default());
    }
    if let Some(ref config) = args.influxdb {
        let config_path = cfg_conf!(config);
        let (send, recv) = hopper::channel_with_max_bytes(
            &config_path,
            &args.data_directory,
            args.max_hopper_queue_bytes,
        ).unwrap();
        senders.insert(config_path.clone(), send);
        receivers.insert(config_path.clone(), recv);
        config_topology.insert(config_path.clone(), Default::default());
    }
    if let Some(ref config) = args.native_sink_config {
        let config_path = cfg_conf!(config);
        let (send, recv) = hopper::channel_with_max_bytes(
            &config_path,
            &args.data_directory,
            args.max_hopper_queue_bytes,
        ).unwrap();
        senders.insert(config_path.clone(), send);
        receivers.insert(config_path.clone(), recv);
        config_topology.insert(config_path.clone(), Default::default());
    }
    if let Some(ref config) = args.elasticsearch {
        let config_path = cfg_conf!(config);
        let (send, recv) = hopper::channel_with_max_bytes(
            &config_path,
            &args.data_directory,
            args.max_hopper_queue_bytes,
        ).unwrap();
        senders.insert(config_path.clone(), send);
        receivers.insert(config_path.clone(), recv);
        config_topology.insert(config_path.clone(), Default::default());
    }
    if let Some(ref configs) = args.firehosen {
        for config in configs {
            let config_path = cfg_conf!(config);
            let (send, recv) = hopper::channel_with_max_bytes(
                &config_path,
                &args.data_directory,
                args.max_hopper_queue_bytes,
            ).unwrap();
            senders.insert(config_path.clone(), send);
            receivers.insert(config_path.clone(), recv);
            config_topology.insert(config_path.clone(), Default::default());
        }
    }
    // FILTERS
    if let Some(ref configs) = args.programmable_filters {
        for (config_path, config) in configs {
            let (send, recv) = hopper::channel_with_max_bytes(
                config_path,
                &args.data_directory,
                args.max_hopper_queue_bytes,
            ).unwrap();
            senders.insert(config_path.clone(), send);
            receivers.insert(config_path.clone(), recv);
            config_topology.insert(config_path.clone(), config.forwards.clone());
        }
    }
    if let Some(ref configs) = args.delay_filters {
        for (config_path, config) in configs {
            let (send, recv) = hopper::channel_with_max_bytes(
                config_path,
                &args.data_directory,
                args.max_hopper_queue_bytes,
            ).unwrap();
            senders.insert(config_path.clone(), send);
            receivers.insert(config_path.clone(), recv);
            config_topology.insert(config_path.clone(), config.forwards.clone());
        }
    }
    if let Some(ref configs) = args.flush_boundary_filters {
        for (config_path, config) in configs {
            let (send, recv) = hopper::channel_with_max_bytes(
                config_path,
                &args.data_directory,
                args.max_hopper_queue_bytes,
            ).unwrap();
            senders.insert(config_path.clone(), send);
            receivers.insert(config_path.clone(), recv);
            config_topology.insert(config_path.clone(), config.forwards.clone());
        }
    }
    // SOURCES
    //
    if let Some(ref configs) = args.native_server_config {
        for (config_path, config) in configs {
            config_topology.insert(config_path.clone(), config.forwards.clone());
        }
    }
    {
        let internal_config = &args.internal;
        config_topology
            .insert(cfg_conf!(internal_config), internal_config.forwards.clone());
    }
    if let Some(ref configs) = args.statsds {
        for (config_path, config) in configs {
            config_topology.insert(config_path.clone(), config.forwards.clone());
        }
    }
    if let Some(ref configs) = args.graphites {
        for (config_path, config) in configs {
            config_topology.insert(config_path.clone(), config.forwards.clone());
        }
    }
    if let Some(ref configs) = args.files {
        for config in configs {
            let config_path = cfg_conf!(config);
            config_topology.insert(config_path.clone(), config.forwards.clone());
        }
    }

    // Now we validate the topology. We search the outer keys and assert that
    // for every forward there's another key in the map. If not, invalid.
    {
        for (key, forwards) in &config_topology {
            for forward in forwards {
                if config_topology.get(forward).is_none() {
                    error!(
                        "Unable to fulfill configured forward: {} => {}",
                        key, forward,
                    );
                    process::exit(1);
                }
            }
        }
    }

    // SINKS
    //
    if let Some(config) = mem::replace(&mut args.null, None) {
        let recv = receivers.remove(&config.config_path).unwrap();
        sinks.insert(
            config.config_path.clone(),
            SinkWorker {
                sender: senders
                    .get(&config.config_path.clone())
                    .expect("Oops")
                    .clone(),
                thread: thread::spawn(move || {
                    cernan::sink::Null::new(&config).run(recv);
                }),
            },
        );
    }
    if let Some(config) = mem::replace(&mut args.console, None) {
        let recv = receivers
            .remove(&config.config_path.clone().unwrap())
            .unwrap();
        sinks.insert(
            config.config_path.clone().unwrap(),
            SinkWorker {
                sender: senders
                    .get(&config.config_path.clone().unwrap())
                    .expect("Oops")
                    .clone(),
                thread: thread::spawn(move || {
                    cernan::sink::Console::new(&config).run(recv);
                }),
            },
        );
    }
    if let Some(config) = mem::replace(&mut args.wavefront, None) {
        let recv = receivers
            .remove(&config.config_path.clone().unwrap())
            .unwrap();
        sinks.insert(
            config.config_path.clone().unwrap(),
            SinkWorker {
                sender: senders
                    .get(&config.config_path.clone().unwrap())
                    .expect("Oops")
                    .clone(),
                thread: thread::spawn(move || {
                    match cernan::sink::Wavefront::new(config) {
                        Ok(mut w) => {
                            w.run(recv);
                        }
                        Err(e) => {
                            error!("Configuration error for Wavefront: {}", e);
                            process::exit(1);
                        }
                    }
                }),
            },
        );
    }
    if let Some(config) = mem::replace(&mut args.prometheus, None) {
        let recv = receivers
            .remove(&config.config_path.clone().unwrap())
            .unwrap();
        sinks.insert(
            config.config_path.clone().unwrap(),
            SinkWorker {
                sender: senders
                    .get(&config.config_path.clone().unwrap())
                    .expect("Oops")
                    .clone(),
                thread: thread::spawn(move || {
                    cernan::sink::Prometheus::new(&config).run(recv);
                }),
            },
        );
    }
    if let Some(config) = mem::replace(&mut args.influxdb, None) {
        let recv = receivers
            .remove(&config.config_path.clone().unwrap())
            .unwrap();
        sinks.insert(
            config.config_path.clone().unwrap(),
            SinkWorker {
                sender: senders
                    .get(&config.config_path.clone().unwrap())
                    .expect("Oops")
                    .clone(),
                thread: thread::spawn(move || {
                    cernan::sink::InfluxDB::new(&config).run(recv);
                }),
            },
        );
    }
    if let Some(config) = mem::replace(&mut args.native_sink_config, None) {
        let recv = receivers
            .remove(&config.config_path.clone().unwrap())
            .unwrap();
        sinks.insert(
            config.config_path.clone().unwrap(),
            SinkWorker {
                sender: senders
                    .get(&config.config_path.clone().unwrap())
                    .expect("Oops")
                    .clone(),
                thread: thread::spawn(move || {
                    cernan::sink::Native::new(config).run(recv);
                }),
            },
        );
    }
    if let Some(config) = mem::replace(&mut args.elasticsearch, None) {
        let recv = receivers
            .remove(&config.config_path.clone().unwrap())
            .unwrap();
        sinks.insert(
            config.config_path.clone().unwrap(),
            SinkWorker {
                sender: senders
                    .get(&config.config_path.clone().unwrap())
                    .expect("Oops")
                    .clone(),
                thread: thread::spawn(move || {
                    cernan::sink::Elasticsearch::new(config).run(recv);
                }),
            },
        );
    }
    if let Some(cfgs) = mem::replace(&mut args.firehosen, None) {
        for config in cfgs {
            let recv = receivers
                .remove(&config.config_path.clone().unwrap())
                .unwrap();
            sinks.insert(
                config.config_path.clone().unwrap(),
                SinkWorker {
                    sender: senders
                        .get(&config.config_path.clone().unwrap())
                        .expect("Oops")
                        .clone(),
                    thread: thread::spawn(move || {
                        cernan::sink::Firehose::new(config).run(recv);
                    }),
                },
            );
        }
    }

    // FILTERS
    //
    mem::replace(&mut args.programmable_filters, None).map(|cfg_map| {
        for config in cfg_map.values() {
            let c: ProgrammableFilterConfig = (*config).clone();
            let recv = receivers
                .remove(&config.config_path.clone().unwrap())
                .unwrap();
            let mut downstream_sends = Vec::new();
            populate_forwards(
                &mut downstream_sends,
                None,
                &config.forwards,
                &config
                    .config_path
                    .clone()
                    .expect("[INTERNAL ERROR] no config_path"),
                &senders,
            );
            filters.insert(
                config.config_path.clone().unwrap(),
                SinkWorker {
                    sender: senders
                        .get(&config.config_path.clone().unwrap())
                        .expect("Oops")
                        .clone(),
                    thread: thread::spawn(move || {
                        cernan::filter::ProgrammableFilter::new(c)
                            .run(recv, downstream_sends);
                    }),
                },
            );
        }
    });

    mem::replace(&mut args.delay_filters, None).map(|cfg_map| {
        for config in cfg_map.values() {
            let c: DelayFilterConfig = (*config).clone();
            let recv = receivers
                .remove(&config.config_path.clone().unwrap())
                .unwrap();
            let mut downstream_sends = Vec::new();
            populate_forwards(
                &mut downstream_sends,
                None,
                &config.forwards,
                &config
                    .config_path
                    .clone()
                    .expect("[INTERNAL ERROR] no config_path"),
                &senders,
            );
            filters.insert(
                config.config_path.clone().unwrap(),
                SinkWorker {
                    sender: senders
                        .get(&config.config_path.clone().unwrap())
                        .expect("Oops")
                        .clone(),
                    thread: thread::spawn(move || {
                        cernan::filter::DelayFilter::new(&c)
                            .run(recv, downstream_sends);
                    }),
                },
            );
        }
    });

    mem::replace(&mut args.flush_boundary_filters, None).map(|cfg_map| {
        for config in cfg_map.values() {
            let c: FlushBoundaryFilterConfig = (*config).clone();
            let recv = receivers
                .remove(&config.config_path.clone().unwrap())
                .unwrap();
            let mut downstream_sends = Vec::new();
            populate_forwards(
                &mut downstream_sends,
                None,
                &config.forwards,
                &config
                    .config_path
                    .clone()
                    .expect("[INTERNAL ERROR] no config_path"),
                &senders,
            );
            filters.insert(
                config.config_path.clone().unwrap(),
                SinkWorker {
                    sender: senders
                        .get(&config.config_path.clone().unwrap())
                        .expect("Oops")
                        .clone(),
                    thread: thread::spawn(move || {
                        cernan::filter::FlushBoundaryFilter::new(&c)
                            .run(recv, downstream_sends);
                    }),
                },
            );
        }
    });

    // SOURCES
    //
    mem::replace(&mut args.native_server_config, None).map(|cfg_map| {
        for (config_path, config) in cfg_map {
            let mut native_server_send = Vec::new();
            let poll = mio::Poll::new().unwrap();
            let (registration, readiness) = mio::Registration::new2();
            populate_forwards(
                &mut native_server_send,
                Some(&mut flush_sends),
                &config.forwards,
                &cfg_conf!(config),
                &senders,
            );
            sources.insert(
                config_path.clone(),
                SourceWorker {
                    readiness: readiness,
                    thread: thread::spawn(move || {
                        poll.register(
                            &registration,
                            constants::SYSTEM,
                            mio::Ready::readable(),
                            mio::PollOpt::edge(),
                        ).expect("Poll failed to return a result!");
                        cernan::source::NativeServer::new(native_server_send, config)
                            .run(poll);
                    }),
                },
            );
        }
    });

    let internal_config = mem::replace(&mut args.internal, Default::default());
    let poll = mio::Poll::new().unwrap();
    let (registration, readiness) = mio::Registration::new2();
    let mut internal_send = Vec::new();
    populate_forwards(
        &mut internal_send,
        Some(&mut flush_sends),
        &internal_config.forwards,
        &cfg_conf!(internal_config),
        &senders,
    );
    sources.insert(
        internal_config.config_path.clone().unwrap(),
        SourceWorker {
            readiness: readiness,
            thread: thread::spawn(move || {
                poll.register(
                    &registration,
                    constants::SYSTEM,
                    mio::Ready::readable(),
                    mio::PollOpt::edge(),
                ).expect("Poll failed to return a result!");
                cernan::source::Internal::new(internal_send, internal_config)
                    .run(poll);
            }),
        },
    );

    mem::replace(&mut args.statsds, None).map(|cfg_map| {
        for (config_path, config) in cfg_map {
            let mut statsd_sends = Vec::new();
            let poll = mio::Poll::new().unwrap();
            let (registration, readiness) = mio::Registration::new2();
            populate_forwards(
                &mut statsd_sends,
                Some(&mut flush_sends),
                &config.forwards,
                &cfg_conf!(config),
                &senders,
            );
            sources.insert(
                config_path.clone(),
                SourceWorker {
                    readiness: readiness.clone(),
                    thread: thread::spawn(move || {
                        poll.register(
                            &registration,
                            constants::SYSTEM,
                            mio::Ready::readable(),
                            mio::PollOpt::edge(),
                        ).expect("Poll failed to return a result!");
                        cernan::source::Statsd::new(statsd_sends, config).run(poll);
                    }),
                },
            );
        }
    });

    mem::replace(&mut args.graphites, None).map(|cfg_map| {
        for (config_path, config) in cfg_map {
            let mut graphite_sends = Vec::new();
            let poll = mio::Poll::new().unwrap();
            let (registration, readiness) = mio::Registration::new2();
            populate_forwards(
                &mut graphite_sends,
                Some(&mut flush_sends),
                &config.forwards,
                &cfg_conf!(config),
                &senders,
            );
            sources.insert(
                config_path.clone(),
                SourceWorker {
                    readiness: readiness,
                    thread: thread::spawn(move || {
                        poll.register(
                            &registration,
                            constants::SYSTEM,
                            mio::Ready::readable(),
                            mio::PollOpt::edge(),
                        ).expect("Poll failed to return a result!");
                        cernan::source::Graphite::new(graphite_sends, config)
                            .run(poll);
                    }),
                },
            );
        }
    });

    mem::replace(&mut args.files, None).map(|cfg| {
        for config in cfg {
            let mut fp_sends = Vec::new();
            let poll = mio::Poll::new().unwrap();
            let (registration, readiness) = mio::Registration::new2();
            populate_forwards(
                &mut fp_sends,
                Some(&mut flush_sends),
                &config.forwards,
                &cfg_conf!(config),
                &senders,
            );
            sources.insert(
                cfg_conf!(config).clone(),
                SourceWorker {
                    readiness: readiness,
                    thread: thread::spawn(move || {
                        poll.register(
                            &registration,
                            constants::SYSTEM,
                            mio::Ready::readable(),
                            mio::PollOpt::edge(),
                        ).expect("Poll failed to return a result!");
                        cernan::source::FileServer::new(fp_sends, config).run(poll);
                    }),
                },
            );
        }
    });

    // BACKGROUND
    //
    thread::spawn(move || {
        let mut flush_channels = Vec::new();
        let poll = mio::Poll::new().unwrap();
        for destination in &flush_sends {
            match senders.get(destination) {
                Some(snd) => {
                    flush_channels.push(snd.clone());
                }
                None => {
                    error!(
                        "Unable to fulfill configured top-level flush to {}",
                        destination
                    );
                    process::exit(0);
                }
            }
        }
        drop(flush_sends);
        drop(senders);
        cernan::source::FlushTimer::new(flush_channels).run(poll);
    });

    thread::spawn(move || {
        cernan::time::update_time();
    });

    drop(args);
    drop(config_topology);
    drop(receivers);

    signal.recv().unwrap();

    // First, we shut down source to quiesce event generation.
    for (id, source_worker) in sources {
        println!("Signaling shutdown to {:?}", id);
        source_worker
            .readiness
            .set_readiness(mio::Ready::readable())
            .expect("Oops!");
        source_worker.thread.join().expect("Failed during join!");
    }

    broadcast_shutdown(&filters);
    broadcast_shutdown(&sinks);
    join_all(sinks);
    join_all(filters);
}
