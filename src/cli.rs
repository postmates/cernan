//! Provides the CLI option parser
//!
//! Used to parse the argv/config file into a struct that
//! the server can consume and use as configuration data.

use clap::{Arg, App};
use std::str::FromStr;

const VERSION: Option<&'static str> = option_env!("CARGO_PKG_VERSION");

#[derive(Clone)]
pub struct Args {
    pub statsd_port: u16,
    pub graphite_port: u16,
    pub flush_interval: u64,
    pub console: bool,
    pub wavefront: bool,
    pub librato: bool,
    pub wavefront_port: Option<u16>,
    pub wavefront_host: Option<String>,
    pub wavefront_skip_aggrs: bool,
    pub librato_username: Option<String>,
    pub librato_host: Option<String>,
    pub librato_token: Option<String>,
    pub tags: String,
    pub verbose: u64,
    pub version: String,
}

pub fn parse_args() -> Args {
    let args = App::new("cernan")
        .version(VERSION.unwrap_or("unknown"))
        .author("Brian L. Troutwine <blt@postmates.com>")
        .about("telemetry aggregation and shipping, last up the ladder")
        .arg(Arg::with_name("statsd-port")
             .long("statsd-port")
             .value_name("port")
             .help("The UDP port to bind to for statsd traffic.")
             .takes_value(true)
             .default_value("8125"))
        .arg(Arg::with_name("graphite-port")
             .long("graphite-port")
             .value_name("port")
             .help("The TCP port to bind to for graphite traffic.")
             .takes_value(true)
             .default_value("2003"))
        .arg(Arg::with_name("flush-interval")
             .long("flush-interval")
             .value_name("interval")
             .help("How frequently to flush metrics to the backends in seconds.")
             .takes_value(true)
             .default_value("10"))
        .arg(Arg::with_name("console")
             .long("console")
             .help("Enable the console backend."))
        .arg(Arg::with_name("wavefront")
             .long("wavefront")
             .help("Enable the wavefront backend."))
        .arg(Arg::with_name("librato")
             .long("librato")
             .help("Enable the librato backend."))
        .arg(Arg::with_name("wavefront-port")
             .long("wavefront-port")
             .help("The port wavefront proxy is running on")
             .takes_value(true)
             .default_value("2878"))
        .arg(Arg::with_name("wavefront-host")
             .long("wavefront-host")
             .help("The host wavefront proxy is running on")
             .takes_value(true)
             .default_value("127.0.0.1"))
        .arg(Arg::with_name("wavefront-skip-aggrs")
             .long("wavefront-skip-aggrs")
             .help("Skip sending aggregate metrics to wavefront")) // default false
        .arg(Arg::with_name("librato-username")
             .long("librato-username")
             .help("The librato username for authentication.")
             .takes_value(true)
             .default_value("statsd"))
        .arg(Arg::with_name("librato-host")
             .long("librato-host")
             .help("The librato host to report to.")
             .takes_value(true)
             .default_value("https://metrics-api.librato.com/v1/metrics"))
        .arg(Arg::with_name("librato-token")
             .long("librato-token")
             .help("The librato token for authentication.")
             .takes_value(true)
             .default_value("statsd"))
        .arg(Arg::with_name("tags")
             .long("tags")
             .help("A comma separated list of tags to report to supporting backends.")
             .takes_value(true)
             .use_delimiter(false)
             .default_value("source=cernan"))
        .arg(Arg::with_name("verbose")
             .short("v")
             .multiple(true)
             .help("Turn on verbose output."))
        .get_matches();

    let mk_wavefront = args.is_present("wavefront");
    let mk_console = args.is_present("console");
    let mk_librato = args.is_present("librato");

    let (wport, whost, wskpaggr) = if mk_wavefront {
        (Some(u16::from_str(args.value_of("wavefront-port").unwrap()).unwrap()),
         Some(args.value_of("wavefront-host").unwrap().to_string()),
         args.value_of("wavefront-skip-aggrs").is_some())
    } else {
        (None, None, false)
    };

    let (luser, lhost, ltoken) = if mk_librato {
        (Some(args.value_of("librato-username").unwrap().to_string()),
         Some(args.value_of("librato-token").unwrap().to_string()),
         Some(args.value_of("librato-host").unwrap().to_string()))
    } else {
        (None, None, None)
    };

    let verb = if args.is_present("verbose") {
        args.occurrences_of("verbose")
    } else {
        0
    };

    Args {
        statsd_port: u16::from_str(args.value_of("statsd-port").unwrap()).unwrap(),
        graphite_port: u16::from_str(args.value_of("graphite-port").unwrap()).unwrap(),
        flush_interval: u64::from_str(args.value_of("flush-interval").unwrap()).unwrap(),
        console: mk_console,
        wavefront: mk_wavefront,
        librato: mk_librato,
        wavefront_port: wport,
        wavefront_host: whost,
        wavefront_skip_aggrs: wskpaggr,
        librato_username: luser,
        librato_host: lhost,
        librato_token: ltoken,
        tags: args.value_of("tags").unwrap().to_string(),
        verbose: verb,
        version: VERSION.unwrap().to_string(),
    }
}
