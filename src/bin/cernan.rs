extern crate chrono;
extern crate cernan;
extern crate fern;
#[macro_use]
extern crate log;
extern crate hopper;

// use cernan::filter::{Filter, ProgrammableFilterConfig};
// use cernan::metric;

#[cfg(feature = "firehose")]
use cernan::sink::FirehoseConfig;
// use cernan::sink::Sink;
// use cernan::source::Source;
// use cernan::util;
use chrono::UTC;
// use std::collections::{HashMap, HashSet};
// use std::process;
use std::str;
use std::thread;

// fn populate_forwards(sends: &mut util::Channel,
//                      mut top_level_forwards: Option<&mut HashSet<String>>,
//                      forwards: &[String],
//                      config_path: &str,
// available_sends: &HashMap<String,
// hopper::Sender<metric::Event>>) {
//     for fwd in forwards {
//         match top_level_forwards.as_mut() {
//             Some(tlf) => {
//                 let _ = (*tlf).insert(fwd.clone());
//             }
//             None => {}
//         }
//         match available_sends.get(fwd) {
//             Some(snd) => {
//                 sends.push(snd.clone());
//             }
//             None => {
//                 error!("Unable to fulfill configured forward: {} => {}",
//                        config_path,
//                        fwd);
//                 process::exit(0);
//             }
//         }
//     }
// }

fn main() {
    let args = cernan::config::parse_args();

    let level = match args.verbose {
        0 => log::LogLevelFilter::Error,
        1 => log::LogLevelFilter::Warn,
        2 => log::LogLevelFilter::Info,
        3 => log::LogLevelFilter::Debug,
        _ => log::LogLevelFilter::Trace,
    };

    fern::Dispatch::new()
        .format(|out, message, record| {
                    out.finish(format_args!("[{}][{}][{}][{}] {}",
                                            record.location().module_path(),
                                            record.location().line(),
                                            UTC::now().to_rfc3339(),
                                            record.level(),
                                            message))
                })
        .level(level)
        .chain(std::io::stdout())
        .apply()
        .expect("could not set up logging");

    info!("cernan - {}", args.version);
    let mut joins = Vec::new();
    // let mut sends: HashMap<String, hopper::Sender<metric::Event>> =
    // HashMap::new();
    // let mut flush_sends = HashSet::new();

    // // SINKS
    // //
    // if let Some(config) = args.console {
    //     let (console_send, console_recv) =
    //         hopper::channel(&config.config_path, &args.data_directory).unwrap();
    //     sends.insert(config.config_path.clone(), console_send);
    //     joins.push(thread::spawn(move || {
    //
    // cernan::sink::Console::new(config).run(console_recv);
    //                              }));
    // }
    // if let Some(config) = args.null {
    // let (null_send, null_recv) = hopper::channel(&config.config_path,
    // &args.data_directory)
    //         .unwrap();
    //     sends.insert(config.config_path.clone(), null_send);
    // joins.push(thread::spawn(move || {
    // cernan::sink::Null::new(config).run(null_recv); }));
    // }
    // if let Some(config) = args.wavefront {
    // let (wf_send, wf_recv) = hopper::channel(&config.config_path,
    // &args.data_directory)
    //         .unwrap();
    //     sends.insert(config.config_path.clone(), wf_send);
    // joins.push(thread::spawn(move || {
    // cernan::sink::Wavefront::new(config).run(wf_recv); }));
    // }
    // if let Some(config) = args.prometheus {
    //     let (prometheus_send, prometheus_recv) =
    //         hopper::channel(&config.config_path, &args.data_directory).unwrap();
    //     sends.insert(config.config_path.clone(), prometheus_send);
    //     joins.push(thread::spawn(move || {
    //
    // cernan::sink::Prometheus::new(config).run(prometheus_recv);
    //                              }));
    // }
    // if let Some(config) = args.influxdb {
    // let (flx_send, flx_recv) = hopper::channel(&config.config_path,
    // &args.data_directory)
    //         .unwrap();
    //     sends.insert(config.config_path.clone(), flx_send);
    // joins.push(thread::spawn(move || {
    // cernan::sink::InfluxDB::new(config).run(flx_recv); }));
    // }
    // if let Some(config) = args.native_sink_config {
    // let (cernan_send, cernan_recv) = hopper::channel(&config.config_path,
    // &args.data_directory)
    //         .unwrap();
    //     sends.insert(config.config_path.clone(), cernan_send);
    // joins.push(thread::spawn(move || {
    // cernan::sink::Native::new(config).run(cernan_recv); }));
    // }

    // #[cfg(feature = "firehose")]
    // {
    //     for config in &args.firehosen {
    //         let f: FirehoseConfig = config.clone();
    //         let (firehose_send, firehose_recv) =
    // hopper::channel(&config.config_path,
    // &args.data_directory).unwrap();
    //         sends.insert(config.config_path.clone(), firehose_send);
    //         joins.push(thread::spawn(move || {
    //
    // cernan::sink::Firehose::new(f).run(firehose_recv);
    //                                  }));
    //     }
    // }

    // // FILTERS
    // //
    // for config in args.filters.values() {
    //     let c: ProgrammableFilterConfig = (*config).clone();
    // let (flt_send, flt_recv) = hopper::channel(&config.config_path,
    // &args.data_directory)
    //         .unwrap();
    //     sends.insert(config.config_path.clone(), flt_send);
    //     let mut downstream_sends = Vec::new();
    //     populate_forwards(&mut downstream_sends,
    //                       None,
    //                       &config.forwards,
    //                       &config.config_path,
    //                       &sends);
    //     joins.push(thread::spawn(move || {
    //                                  cernan::filter::ProgrammableFilter::new(c)
    //                                      .run(flt_recv, downstream_sends);
    //                              }));
    // }

    // // SOURCES
    // //
    // if let Some(config) = args.native_server_config {
    //     let mut native_server_send = Vec::new();
    //     populate_forwards(&mut native_server_send,
    //                       Some(&mut flush_sends),
    //                       &config.forwards,
    //                       &config.config_path,
    //                       &sends);
    //     joins.push(thread::spawn(move || {
    //
    // cernan::source::NativeServer::new(native_server_send, config)
    //                                      .run();
    //                              }))
    // }

    // let internal_config = args.internal;
    // let mut internal_send = Vec::new();
    // populate_forwards(&mut internal_send,
    //                   Some(&mut flush_sends),
    //                   &internal_config.forwards,
    //                   &internal_config.config_path,
    //                   &sends);
    // joins.push(thread::spawn(move || {
    // cernan::source::Internal::new(internal_send,
    // internal_config)
    //                                  .run();
    //                          }));

    // for config in args.statsds.values() {
    //     let c = (*config).clone();
    //     let mut statsd_sends = Vec::new();
    //     populate_forwards(&mut statsd_sends,
    //                       Some(&mut flush_sends),
    //                       &config.forwards,
    //                       &config.config_path,
    //                       &sends);
    // joins.push(thread::spawn(move || {
    // cernan::source::Statsd::new(statsd_sends, c).run(); }));
    // }

    // for config in args.graphites.values() {
    //     let c = (*config).clone();
    //     let mut graphite_sends = Vec::new();
    //     populate_forwards(&mut graphite_sends,
    //                       Some(&mut flush_sends),
    //                       &config.forwards,
    //                       &config.config_path,
    //                       &sends);
    //     joins.push(thread::spawn(move || {
    //
    // cernan::source::Graphite::new(graphite_sends, c).run();
    //                              }));
    // }

    // for config in args.files {
    //     let mut fp_sends = Vec::new();
    //     populate_forwards(&mut fp_sends,
    //                       Some(&mut flush_sends),
    //                       &config.forwards,
    //                       &config.config_path,
    //                       &sends);
    //     joins.push(thread::spawn(move || {
    // cernan::source::FileServer::new(fp_sends,
    // config).run();
    //                              }));
    // }

    // // BACKGROUND
    // //

    // joins.push(thread::spawn(move || {
    //     let mut flush_channels = Vec::new();
    //     for destination in flush_sends.iter() {
    //         match sends.get(destination) {
    //             Some(snd) => {
    //                 flush_channels.push(snd.clone());
    //             }
    //             None => {
    //                 error!("Unable to fulfill configured top-level flush to {}",
    //                        destination);
    //                 process::exit(0);
    //             }
    //         }
    //     }
    //     cernan::source::FlushTimer::new(flush_channels).run();
    // }));

    joins.push(thread::spawn(move || { cernan::time::update_time(); }));

    for jh in joins {
        // TODO Having sub-threads panic will not cause a bubble-up if that
        // thread is not the currently examined one. We're going to have to have
        // some manner of sub-thread communication going on.
        jh.join().expect("Uh oh, child thread paniced!");
    }
}
