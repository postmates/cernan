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

use cernan::filter::{DelayFilterConfig, Filter, FlushBoundaryFilterConfig,
                     ProgrammableFilterConfig, JSONEncodeFilterConfig};
use cernan::matrix;
use cernan::metric;
use cernan::sink::Sink;
use cernan::source::Source;
use cernan::thread::Stoppable;
use chrono::Utc;
use std::collections::{HashMap, HashSet};
use std::mem;
use std::process;
use std::str;

fn populate_forwards(
    mut top_level_forwards: Option<&mut HashSet<String>>,
    forwards: &[String],
    config_path: &str,
    available_sends: &HashMap<String, hopper::Sender<metric::Event>>,
    adjacency_matrix: &mut matrix::Adjacency<hopper::Sender<metric::Event>>,
) {
    for fwd in forwards {
        if let Some(tlf) = top_level_forwards.as_mut() {
            let _ = (*tlf).insert(fwd.clone());
        }

        match available_sends.get(fwd) {
            Some(snd) => {
                trace!("Populating sender from {:?} to {:?}", config_path, fwd);
                adjacency_matrix.add_asymmetric_edge(
                    config_path,
                    &fwd.clone(),
                    Some(snd.clone()),
                );
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

fn join_all(workers: HashMap<String, cernan::thread::ThreadHandle>) {
    for (_worker_id, worker) in workers {
        worker.join();
    }
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
        0 => log::LevelFilter::Error,
        1 => log::LevelFilter::Warn,
        2 => log::LevelFilter::Info,
        3 => log::LevelFilter::Debug,
        _ => log::LevelFilter::Trace,
    };

    let signal =
        chan_signal::notify(&[chan_signal::Signal::INT, chan_signal::Signal::TERM]);

    fern::Dispatch::new()
        .format(|out, message, record| {
            out.finish(format_args!(
                "[{}][{}][{}][{}] {}",
                record.module_path().unwrap(),
                record.line().unwrap(),
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
    let mut sources: HashMap<String, cernan::thread::ThreadHandle> = HashMap::new();
    let mut sinks: HashMap<String, cernan::thread::ThreadHandle> = HashMap::new();
    let mut filters: HashMap<String, cernan::thread::ThreadHandle> = HashMap::new();
    let mut senders: HashMap<String, hopper::Sender<metric::Event>> = HashMap::new();
    let mut receivers: HashMap<String, hopper::Receiver<metric::Event>> =
        HashMap::new();
    let mut flush_sends = HashSet::new();

    let mut config_topology: HashMap<String, Vec<String>> = HashMap::new();
    let mut adjacency_matrix =
        matrix::Adjacency::<hopper::Sender<metric::Event>>::new();

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
        let (send, recv) = hopper::channel_with_explicit_capacity(
            &config.config_path,
            &args.data_directory,
            args.max_hopper_in_memory_bytes,
            args.max_hopper_queue_bytes,
            args.max_hopper_queue_files,
        ).unwrap();
        senders.insert(config.config_path.clone(), send);
        receivers.insert(config.config_path.clone(), recv);
        config_topology.insert(config.config_path.clone(), Default::default());
    }
    if let Some(ref config) = args.console {
        let config_path = cfg_conf!(config);
        let (send, recv) = hopper::channel_with_explicit_capacity(
            &config_path,
            &args.data_directory,
            args.max_hopper_in_memory_bytes,
            args.max_hopper_queue_bytes,
            args.max_hopper_queue_files,
        ).unwrap();
        senders.insert(config_path.clone(), send);
        receivers.insert(config_path.clone(), recv);
        config_topology.insert(config_path.clone(), Default::default());
    }
    if let Some(ref config) = args.wavefront {
        let config_path = cfg_conf!(config);
        let (send, recv) = hopper::channel_with_explicit_capacity(
            &config_path,
            &args.data_directory,
            args.max_hopper_in_memory_bytes,
            args.max_hopper_queue_bytes,
            args.max_hopper_queue_files,
        ).unwrap();
        senders.insert(config_path.clone(), send);
        receivers.insert(config_path.clone(), recv);
        config_topology.insert(config_path.clone(), Default::default());
    }
    if let Some(ref config) = args.prometheus {
        let config_path = cfg_conf!(config);
        let (send, recv) = hopper::channel_with_explicit_capacity(
            &config_path,
            &args.data_directory,
            args.max_hopper_in_memory_bytes,
            args.max_hopper_queue_bytes,
            args.max_hopper_queue_files,
        ).unwrap();
        senders.insert(config_path.clone(), send);
        receivers.insert(config_path.clone(), recv);
        config_topology.insert(config_path.clone(), Default::default());
    }
    if let Some(ref config) = args.influxdb {
        let config_path = cfg_conf!(config);
        let (send, recv) = hopper::channel_with_explicit_capacity(
            &config_path,
            &args.data_directory,
            args.max_hopper_in_memory_bytes,
            args.max_hopper_queue_bytes,
            args.max_hopper_queue_files,
        ).unwrap();
        senders.insert(config_path.clone(), send);
        receivers.insert(config_path.clone(), recv);
        config_topology.insert(config_path.clone(), Default::default());
    }
    if let Some(ref config) = args.native_sink_config {
        let config_path = cfg_conf!(config);
        let (send, recv) = hopper::channel_with_explicit_capacity(
            &config_path,
            &args.data_directory,
            args.max_hopper_in_memory_bytes,
            args.max_hopper_queue_bytes,
            args.max_hopper_queue_files,
        ).unwrap();
        senders.insert(config_path.clone(), send);
        receivers.insert(config_path.clone(), recv);
        config_topology.insert(config_path.clone(), Default::default());
    }
    if let Some(ref config) = args.elasticsearch {
        let config_path = cfg_conf!(config);
        let (send, recv) = hopper::channel_with_explicit_capacity(
            &config_path,
            &args.data_directory,
            args.max_hopper_in_memory_bytes,
            args.max_hopper_queue_bytes,
            args.max_hopper_queue_files,
        ).unwrap();
        senders.insert(config_path.clone(), send);
        receivers.insert(config_path.clone(), recv);
        config_topology.insert(config_path.clone(), Default::default());
    }
    if let Some(ref configs) = args.firehosen {
        for config in configs {
            let config_path = cfg_conf!(config);
            let (send, recv) = hopper::channel_with_explicit_capacity(
                &config_path,
                &args.data_directory,
                args.max_hopper_in_memory_bytes,
                args.max_hopper_queue_bytes,
                args.max_hopper_queue_files,
            ).unwrap();
            senders.insert(config_path.clone(), send);
            receivers.insert(config_path.clone(), recv);
            config_topology.insert(config_path.clone(), Default::default());
        }
    }
    if let Some(ref configs) = args.kafkas {
        for config in configs {
            let config_path = cfg_conf!(config);
            let (send, recv) = hopper::channel_with_explicit_capacity(
                &config_path,
                &args.data_directory,
                args.max_hopper_in_memory_bytes,
                args.max_hopper_queue_bytes,
                args.max_hopper_queue_files,
            ).unwrap();
            senders.insert(config_path.clone(), send);
            receivers.insert(config_path.clone(), recv);
            config_topology.insert(config_path.clone(), Default::default());
        }
    }
    if let Some(ref configs) = args.kinesises {
        for config in configs {
            let config_path = cfg_conf!(config);
            let (send, recv) = hopper::channel_with_explicit_capacity(
                &config_path,
                &args.data_directory,
                args.max_hopper_in_memory_bytes,
                args.max_hopper_queue_bytes,
                args.max_hopper_queue_files,
            ).unwrap();
            senders.insert(config_path.clone(), send);
            receivers.insert(config_path.clone(), recv);
            config_topology.insert(config_path.clone(), Default::default());
        }
    }
    // FILTERS
    if let Some(ref configs) = args.programmable_filters {
        for (config_path, config) in configs {
            let (send, recv) = hopper::channel_with_explicit_capacity(
                config_path,
                &args.data_directory,
                args.max_hopper_in_memory_bytes,
                args.max_hopper_queue_bytes,
                args.max_hopper_queue_files,
            ).unwrap();
            senders.insert(config_path.clone(), send);
            receivers.insert(config_path.clone(), recv);
            config_topology.insert(config_path.clone(), config.forwards.clone());
            adjacency_matrix.add_edges(
                &config_path.clone(),
                config.forwards.clone(),
                None,
            );
        }
    }
    if let Some(ref configs) = args.delay_filters {
        for (config_path, config) in configs {
            let (send, recv) = hopper::channel_with_explicit_capacity(
                config_path,
                &args.data_directory,
                args.max_hopper_in_memory_bytes,
                args.max_hopper_queue_bytes,
                args.max_hopper_queue_files,
            ).unwrap();
            senders.insert(config_path.clone(), send);
            receivers.insert(config_path.clone(), recv);
            config_topology.insert(config_path.clone(), config.forwards.clone());
            adjacency_matrix.add_edges(
                &config_path.clone(),
                config.forwards.clone(),
                None,
            );
        }
    }
    if let Some(ref configs) = args.json_encode_filters {
        for (config_path, config) in configs {
            let (send, recv) = hopper::channel_with_explicit_capacity(
                config_path,
                &args.data_directory,
                args.max_hopper_in_memory_bytes,
                args.max_hopper_queue_bytes,
                args.max_hopper_queue_files,
            ).unwrap();
            senders.insert(config_path.clone(), send);
            receivers.insert(config_path.clone(), recv);
            config_topology.insert(config_path.clone(), config.forwards.clone());
            adjacency_matrix.add_edges(
                &config_path.clone(),
                config.forwards.clone(),
                None,
            );
        }
    }
    if let Some(ref configs) = args.flush_boundary_filters {
        for (config_path, config) in configs {
            let (send, recv) = hopper::channel_with_explicit_capacity(
                config_path,
                &args.data_directory,
                args.max_hopper_in_memory_bytes,
                args.max_hopper_queue_bytes,
                args.max_hopper_queue_files,
            ).unwrap();
            senders.insert(config_path.clone(), send);
            receivers.insert(config_path.clone(), recv);
            config_topology.insert(config_path.clone(), config.forwards.clone());
            adjacency_matrix.add_edges(
                &config_path.clone(),
                config.forwards.clone(),
                None,
            );
        }
    }
    // SOURCES
    //
    if let Some(ref configs) = args.native_server_config {
        for (config_path, config) in configs {
            adjacency_matrix.add_edges(
                &config_path.clone(),
                config.forwards.clone(),
                None,
            );
        }
    }
    {
        let internal_config = &args.internal;
        config_topology
            .insert(cfg_conf!(internal_config), internal_config.forwards.clone());
        adjacency_matrix.add_edges(
            &cfg_conf!(internal_config),
            internal_config.forwards.clone(),
            None,
        );
    }
    if let Some(ref configs) = args.statsds {
        for (config_path, config) in configs {
            config_topology.insert(config_path.clone(), config.forwards.clone());
            adjacency_matrix.add_edges(
                &config_path.clone(),
                config.forwards.clone(),
                None,
            );
        }
    }
    if let Some(ref configs) = args.graphites {
        for (config_path, config) in configs {
            config_topology.insert(config_path.clone(), config.forwards.clone());
            adjacency_matrix.add_edges(
                &config_path.clone(),
                config.forwards.clone(),
                None,
            );
        }
    }
    if let Some(ref configs) = args.avros {
        for (config_path, config) in configs {
            config_topology.insert(config_path.clone(), config.forwards.clone());
            adjacency_matrix.add_edges(
                &config_path.clone(),
                config.forwards.clone(),
                None,
            );
        }
    }
    if let Some(ref configs) = args.files {
        for config in configs {
            let config_path = cfg_conf!(config);
            config_topology.insert(config_path.clone(), config.forwards.clone());
            adjacency_matrix.add_edges(
                &config_path.clone(),
                config.forwards.clone(),
                None,
            );
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
        let sources = adjacency_matrix.pop_nodes(&config.config_path);
        sinks.insert(
            config.config_path.clone(),
            cernan::sink::Null::new(recv, sources, config).run(),
        );
    }
    if let Some(config) = mem::replace(&mut args.console, None) {
        let recv = receivers
            .remove(&config.config_path.clone().unwrap())
            .unwrap();
        let sources = adjacency_matrix.pop_nodes(&config.config_path.clone().unwrap());
        sinks.insert(
            config.config_path.clone().unwrap(),
            cernan::sink::Console::new(recv, sources, config).run(),
        );
    }
    if let Some(config) = mem::replace(&mut args.wavefront, None) {
        let recv = receivers
            .remove(&config.config_path.clone().unwrap())
            .unwrap();
        let sources = adjacency_matrix.pop_nodes(&config.config_path.clone().unwrap());
        sinks.insert(
            config.config_path.clone().unwrap(),
            cernan::sink::Wavefront::new(recv, sources, config).run(),
        );
    }
    if let Some(config) = mem::replace(&mut args.prometheus, None) {
        let recv = receivers
            .remove(&config.config_path.clone().unwrap())
            .unwrap();
        let sources = adjacency_matrix.pop_nodes(&config.config_path.clone().unwrap());
        sinks.insert(
            config.config_path.clone().unwrap(),
            cernan::sink::Prometheus::new(recv, sources, config).run(),
        );
    }
    if let Some(config) = mem::replace(&mut args.influxdb, None) {
        let recv = receivers
            .remove(&config.config_path.clone().unwrap())
            .unwrap();
        let sources = adjacency_matrix.pop_nodes(&config.config_path.clone().unwrap());
        sinks.insert(
            config.config_path.clone().unwrap(),
            cernan::sink::InfluxDB::new(recv, sources, config).run(),
        );
    }
    if let Some(config) = mem::replace(&mut args.native_sink_config, None) {
        let recv = receivers
            .remove(&config.config_path.clone().unwrap())
            .unwrap();
        let sources = adjacency_matrix.pop_nodes(&config.config_path.clone().unwrap());
        sinks.insert(
            config.config_path.clone().unwrap(),
            cernan::sink::Native::new(recv, sources, config).run(),
        );
    }
    if let Some(config) = mem::replace(&mut args.elasticsearch, None) {
        let recv = receivers
            .remove(&config.config_path.clone().unwrap())
            .unwrap();
        let sources = adjacency_matrix.pop_nodes(&config.config_path.clone().unwrap());
        sinks.insert(
            config.config_path.clone().unwrap(),
            cernan::sink::Elasticsearch::new(recv, sources, config).run(),
        );
    }
    if let Some(cfgs) = mem::replace(&mut args.firehosen, None) {
        for config in cfgs {
            let recv = receivers
                .remove(&config.config_path.clone().unwrap())
                .unwrap();
            let sources =
                adjacency_matrix.pop_nodes(&config.config_path.clone().unwrap());
            sinks.insert(
                config.config_path.clone().unwrap(),
                cernan::sink::Firehose::new(recv, sources, config).run(),
            );
        }
    }
    if let Some(cfgs) = mem::replace(&mut args.kafkas, None) {
        for config in cfgs {
            let recv = receivers
                .remove(&config.config_path.clone().unwrap())
                .unwrap();
            let sources =
                adjacency_matrix.pop_nodes(&config.config_path.clone().unwrap());
            sinks.insert(
                config.config_path.clone().unwrap(),
                cernan::sink::Kafka::new(recv, sources, config).run(),
            );
        }
    }
    if let Some(cfgs) = mem::replace(&mut args.kinesises, None) {
        for config in cfgs {
            let recv = receivers
                .remove(&config.config_path.clone().unwrap())
                .unwrap();
            let sources =
                adjacency_matrix.pop_nodes(&config.config_path.clone().unwrap());
            sinks.insert(
                config.config_path.clone().unwrap(),
                cernan::sink::Kinesis::new(recv, sources, config).run(),
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
            let config_path = config
                .config_path
                .clone()
                .expect("[INTERNAL ERROR] no config_path");
            populate_forwards(
                None,
                &config.forwards,
                &config_path,
                &senders,
                &mut adjacency_matrix,
            );

            let sources = adjacency_matrix.filter_nodes(
                &config.config_path.clone().unwrap(),
                |&(_k, option_v)| option_v.is_none(),
            );
            let downstream_sends = adjacency_matrix.pop_metadata(&config_path);
            filters.insert(
                config.config_path.clone().unwrap(),
                cernan::thread::spawn(move |_poll| {
                    cernan::filter::ProgrammableFilter::new(c).run(
                        recv,
                        sources,
                        downstream_sends,
                    );
                }),
            );
        }
    });

    mem::replace(&mut args.delay_filters, None).map(|cfg_map| {
        for config in cfg_map.values() {
            let c: DelayFilterConfig = (*config).clone();
            let recv = receivers
                .remove(&config.config_path.clone().unwrap())
                .unwrap();

            let config_path = config
                .config_path
                .clone()
                .expect("[INTERNAL ERROR] no config_path");
            populate_forwards(
                None,
                &config.forwards,
                &config_path,
                &senders,
                &mut adjacency_matrix,
            );

            let sources = adjacency_matrix.filter_nodes(
                &config.config_path.clone().unwrap(),
                |&(_k, option_v)| option_v.is_none(),
            );
            let downstream_sends = adjacency_matrix.pop_metadata(&config_path);
            filters.insert(
                config.config_path.clone().unwrap(),
                cernan::thread::spawn(move |_poll| {
                    cernan::filter::DelayFilter::new(&c).run(
                        recv,
                        sources,
                        downstream_sends,
                    );
                }),
            );
        }
    });

    mem::replace(&mut args.json_encode_filters, None).map(|cfg_map| {
        for config in cfg_map.values() {
            let c: JSONEncodeFilterConfig = (*config).clone();
            let recv = receivers
                .remove(&config.config_path.clone().unwrap())
                .unwrap();

            let config_path = config
                .config_path
                .clone()
                .expect("[INTERNAL ERROR] no config_path");
            populate_forwards(
                None,
                &config.forwards,
                &config_path,
                &senders,
                &mut adjacency_matrix,
            );

            let sources = adjacency_matrix.filter_nodes(
                &config.config_path.clone().unwrap(),
                |&(_k, option_v)| option_v.is_none(),
            );
            let downstream_sends = adjacency_matrix.pop_metadata(&config_path);
            filters.insert(
                config.config_path.clone().unwrap(),
                cernan::thread::spawn(move |_poll| {
                    cernan::filter::JSONEncodeFilter::new(&c).run(
                        recv,
                        sources,
                        downstream_sends,
                    );
                }),
            );
        }
    });

    mem::replace(&mut args.flush_boundary_filters, None).map(|cfg_map| {
        for config in cfg_map.values() {
            let c: FlushBoundaryFilterConfig = (*config).clone();
            let recv = receivers
                .remove(&config.config_path.clone().unwrap())
                .unwrap();
            let config_path = config
                .config_path
                .clone()
                .expect("[INTERNAL ERROR] no config_path");
            populate_forwards(
                None,
                &config.forwards,
                &config_path,
                &senders,
                &mut adjacency_matrix,
            );

            let sources = adjacency_matrix.filter_nodes(
                &config.config_path.clone().unwrap(),
                |&(_k, option_v)| option_v.is_none(),
            );
            let downstream_sends = adjacency_matrix.pop_metadata(&config_path);
            filters.insert(
                config.config_path.clone().unwrap(),
                cernan::thread::spawn(move |_poll| {
                    cernan::filter::FlushBoundaryFilter::new(&c).run(
                        recv,
                        sources,
                        downstream_sends,
                    );
                }),
            );
        }
    });

    // SOURCES
    //
    mem::replace(&mut args.native_server_config, None).map(|cfg_map| {
        for (config_path, config) in cfg_map {
            populate_forwards(
                Some(&mut flush_sends),
                &config.forwards,
                &config_path,
                &senders,
                &mut adjacency_matrix,
            );

            let native_server_send = adjacency_matrix.pop_metadata(&config_path);
            sources.insert(
                config_path.clone(),
                cernan::source::NativeServer::new(native_server_send, config.into())
                    .run(),
            );
        }
    });

    let internal_config = mem::replace(&mut args.internal, Default::default());
    let internal_config_path = internal_config.config_path.clone().unwrap();
    populate_forwards(
        Some(&mut flush_sends),
        &internal_config.forwards,
        &internal_config_path,
        &senders,
        &mut adjacency_matrix,
    );

    let internal_send = adjacency_matrix.pop_metadata(&internal_config_path);
    sources.insert(
        internal_config.config_path.clone().unwrap(),
        cernan::source::Internal::new(internal_send, internal_config).run(),
    );

    mem::replace(&mut args.statsds, None).map(|cfg_map| {
        for (config_path, config) in cfg_map {
            populate_forwards(
                Some(&mut flush_sends),
                &config.forwards,
                &config_path,
                &senders,
                &mut adjacency_matrix,
            );

            let statsd_sends = adjacency_matrix.pop_metadata(&config_path);
            sources.insert(
                config_path.clone(),
                cernan::source::Statsd::new(statsd_sends, config).run(),
            );
        }
    });

    mem::replace(&mut args.graphites, None).map(|cfg_map| {
        for (config_path, config) in cfg_map {
            populate_forwards(
                Some(&mut flush_sends),
                &config.forwards,
                &config_path,
                &senders,
                &mut adjacency_matrix,
            );

            let graphite_sends = adjacency_matrix.pop_metadata(&config_path);
            sources.insert(
                config_path.clone(),
                cernan::source::Graphite::new(graphite_sends, config.into()).run(),
            );
        }
    });

    mem::replace(&mut args.avros, None).map(|cfg_map| {
        for (config_path, config) in cfg_map {
            populate_forwards(
                Some(&mut flush_sends),
                &config.forwards,
                &config_path,
                &senders,
                &mut adjacency_matrix,
            );

            let avro_sends = adjacency_matrix.pop_metadata(&config_path);
            sources.insert(
                config_path.clone(),
                cernan::source::Avro::new(avro_sends, config).run(),
            );
        }
    });

    mem::replace(&mut args.files, None).map(|cfg| {
        for config in cfg {
            let config_path = config.config_path.clone().unwrap();
            populate_forwards(
                Some(&mut flush_sends),
                &config.forwards,
                &config_path,
                &senders,
                &mut adjacency_matrix,
            );

            let fp_sends = adjacency_matrix.pop_metadata(&config_path);
            sources.insert(
                cfg_conf!(config).clone(),
                cernan::source::FileServer::new(fp_sends, config).run(),
            );
        }
    });

    // BACKGROUND
    //
    let mut flush_channels = Vec::new();
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
    cernan::source::FlushTimer::new(flush_channels, cernan::source::FlushTimerConfig)
        .run();

    cernan::thread::spawn(move |_poll| {
        cernan::time::update_time();
    });

    drop(args);
    drop(config_topology);
    drop(receivers);

    signal.recv().unwrap();

    // Shut down source to quiesce event generation.
    // During shutdown sources will propgate metric::Event::Shutdown
    // to all of its downstream consumers.
    for (id, source_worker) in sources {
        info!("Signaling shutdown to {:?}", id);
        source_worker.shutdown();
    }

    join_all(sinks);
    join_all(filters);
}
