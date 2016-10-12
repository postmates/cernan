extern crate chrono;
extern crate fern;
#[macro_use]
extern crate log;

extern crate cernan;

use std::str;
use std::thread;
use chrono::UTC;
use std::fmt::Write;

use cernan::sink::Sink;

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

    info!("cernan - {}", args.version);
    let mut joins = Vec::new();
    let mut sends = Vec::new();

    if args.console {
        let (console_send, console_recv) = cernan::mpsc::channel("console", &args.data_directory);
        sends.push(console_send);
        joins.push(thread::spawn(move || {
            cernan::sinks::console::Console::new().run(console_recv);
        }));
    }
    if args.null {
        let (null_send, null_recv) = cernan::mpsc::channel("null", &args.data_directory);
        sends.push(null_send);
        joins.push(thread::spawn(move || {
            cernan::sinks::null::Null::new().run(null_recv);
        }));
    }
    if args.wavefront {
        let mut wf_tags = String::new();
        for (k, v) in args.tags.iter() {
            write!(wf_tags, "{}={} ", k, v).unwrap();
        }
        let cp_args = args.clone();
        let (wf_send, wf_recv) = cernan::mpsc::channel("wf", &args.data_directory);
        sends.push(wf_send);
        joins.push(thread::spawn(move || {
            cernan::sinks::wavefront::Wavefront::new(&cp_args.wavefront_host.unwrap(),
                                                     cp_args.wavefront_port.unwrap(),
                                                     wf_tags,
                                                     cp_args.qos.clone(),
                                                     cp_args.flush_interval as i64)
                .run(wf_recv);
        }));
    }

    let args_fedtrn = args.clone();
    if args_fedtrn.fed_transmitter {
        let (cernan_send, cernan_recv) = cernan::mpsc::channel("cernan", &args.data_directory);
        sends.push(cernan_send);
        joins.push(thread::spawn(move || {
            cernan::sinks::federation_receiver::FederationReceiver::new(args_fedtrn.fed_transmitter_port.unwrap(),
                                                          args_fedtrn.fed_transmitter_host.unwrap()).run(cernan_recv);
        }));
    }

    let args_fedrcv = args.clone();
    if let Some(crcv_port) = args_fedrcv.fed_receiver_port {
        let crcv_ip = args_fedrcv.fed_receiver_ip.unwrap();
        let receiver_server_send = sends.clone();
        joins.push(thread::spawn(move || {
            cernan::server::receiver_sink_server(receiver_server_send, &crcv_ip, crcv_port);
        }));
    }

    for ds in &args.firehose_delivery_streams {
        let fh_name = ds.clone();
        let (firehose_send, firehose_recv) = cernan::mpsc::channel(&fh_name, &args.data_directory);
        sends.push(firehose_send);
        let fh_args = args.clone();
        joins.push(thread::spawn(move || {
            cernan::sinks::firehose::Firehose::new(&fh_name, fh_args.tags).run(firehose_recv);
        }));
    }

    let sport = args.statsd_port;
    if sport.is_some() {
        let sport = sport.unwrap();
        let udp_server_v4_send = sends.clone();
        joins.push(thread::spawn(move || {
            cernan::server::udp_server_v4(udp_server_v4_send, sport);
        }));
        let udp_server_v6_send = sends.clone();
        joins.push(thread::spawn(move || {
            cernan::server::udp_server_v6(udp_server_v6_send, sport);
        }));
    }


    let gport = args.graphite_port;
    if gport.is_some() {
        let gport = gport.unwrap();
        let tcp_server_ipv6_sends = sends.clone();
        joins.push(thread::spawn(move || {
            cernan::server::tcp_server_ipv6(tcp_server_ipv6_sends, gport);
        }));
        let tcp_server_ipv4_sends = sends.clone();
        joins.push(thread::spawn(move || {
            cernan::server::tcp_server_ipv4(tcp_server_ipv4_sends, gport);
        }));
    }

    let flush_interval = args.flush_interval;
    let flush_interval_sends = sends.clone();
    joins.push(thread::spawn(move || {
        cernan::server::flush_timer_loop(flush_interval_sends, flush_interval);
    }));

    joins.push(thread::spawn(move || {
        cernan::time::update_time();
    }));

    if let Some(log_files) = args.files {
        for lf in log_files {
            let fp_sends = sends.clone();
            joins.push(thread::spawn(move || {
                cernan::server::file_server(fp_sends, lf);
            }));
        }
    }

    for jh in joins {
        jh.join().expect("Uh oh, child thread paniced!");
    }
}
