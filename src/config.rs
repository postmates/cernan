//! Provides the CLI option parser
//!
//! Used to parse the argv/config file into a struct that
//! the server can consume and use as configuration data.

use clap::{Arg, App};
use std::fs::File;
use toml;
use toml::Value;
use std::io::Read;
use std::fmt::Write;
use std::str::FromStr;
use metric::MetricQOS;

const VERSION: Option<&'static str> = option_env!("CARGO_PKG_VERSION");

#[derive(Clone, Debug)]
pub struct Args {
    pub statsd_port: u16,
    pub graphite_port: u16,
    pub flush_interval: u64,
    pub console: bool,
    pub wavefront: bool,
    pub wavefront_port: Option<u16>,
    pub wavefront_host: Option<String>,
    pub qos: MetricQOS,
    pub tags: String,
    pub verbose: u64,
    pub version: String,
}

pub fn parse_args() -> Args {
    let args = App::new("cernan")
        .version(VERSION.unwrap_or("unknown"))
        .author("Brian L. Troutwine <blt@postmates.com>")
        .about("telemetry aggregation and shipping, last up the ladder")
        .arg(Arg::with_name("config-file")
             .long("config")
             .short("C")
             .value_name("config")
             .help("The config file to feed in.")
             .takes_value(true))
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
             .help("How frequently to flush metrics to the sinks in seconds.")
             .takes_value(true)
             .default_value("10"))
        .arg(Arg::with_name("console")
             .long("console")
             .help("Enable the console sink."))
        .arg(Arg::with_name("wavefront")
             .long("wavefront")
             .help("Enable the wavefront sink."))
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
             .help("Skip sending aggregate metrics to wavefront")) // NOT USED, REMOVE 0.4.x
        .arg(Arg::with_name("tags")
             .long("tags")
             .help("A comma separated list of tags to report to supporting sinks.")
             .takes_value(true)
             .use_delimiter(false)
             .default_value("source=cernan"))
        .arg(Arg::with_name("verbose")
            .short("v")
            .multiple(true)
            .help("Turn on verbose output."))
        .get_matches();

    let verb = if args.is_present("verbose") {
        args.occurrences_of("verbose")
    } else {
        0
    };

    match args.value_of("config-file") {
        // We read from a configuration file
        Some(filename) => {
            let mut fp = match File::open(filename) {
                Err(e) => panic!("Could not open file {} with error {}", filename, e),
                Ok(fp) => fp,
            };

            let mut buffer = String::new();
            fp.read_to_string(&mut buffer).unwrap();
            let value: toml::Value = buffer.parse().unwrap();

            let tags = match value.lookup("tags") {
                Some(tbl) => {
                    let mut tags = String::new();
                    let ttbl = tbl.as_table().unwrap();
                    for (k, v) in (*ttbl).iter() {
                        write!(tags, "{}={},", k, v.as_str().unwrap()).unwrap();
                    }
                    tags.pop();
                    tags
                }
                None => "".to_string(),
            };

            let qos = match value.lookup("quality-of-service") {
                Some(tbl) => {
                    let ttbl = tbl.as_table().unwrap();
                    let mut hm = MetricQOS::default();
                    for (k, v) in (*ttbl).iter() {
                        let rate = v.as_integer().expect("value must be an integer") as u64;
                        match k.as_ref() {
                            "gauge" => hm.gauge = rate,
                            "counter" => hm.counter = rate,
                            "timer" => hm.timer = rate,
                            "histogram" => hm.histogram = rate,
                            "raw" => hm.raw = rate,
                            _ => panic!("Unknown quality-of-service value!"),
                        };
                    }
                    hm
                }
                None => MetricQOS::default(),
            };

            let mk_wavefront = value.lookup("wavefront").is_some();
            let mk_console = value.lookup("console").is_some();

            let (wport, whost) = if mk_wavefront {
                (// wavefront port
                 value.lookup("wavefront.port")
                    .unwrap_or(&Value::Integer(2878))
                    .as_integer()
                    .map(|i| i as u16),
                 // wavefront host
                 value.lookup("wavefront.host")
                    .unwrap_or(&Value::String("127.0.0.1".to_string()))
                    .as_str()
                    .map(|s| s.to_string()))
            } else {
                (None, None)
            };

            Args {
                statsd_port: value.lookup("statsd-port")
                    .unwrap_or(&Value::Integer(8125))
                    .as_integer()
                    .expect("statsd-port must be integer") as u16,
                graphite_port: value.lookup("graphite-port")
                    .unwrap_or(&Value::Integer(2003))
                    .as_integer()
                    .expect("graphite-port must be integer") as u16,
                flush_interval: value.lookup("flush-interval")
                    .unwrap_or(&Value::Integer(10))
                    .as_integer()
                    .expect("flush-interval must be \
                             integer") as u64,
                console: mk_console,
                wavefront: mk_wavefront,
                wavefront_port: wport,
                wavefront_host: whost,
                tags: tags,
                qos: qos,
                verbose: verb,
                version: VERSION.unwrap().to_string(),
            }
        }
        // We read from CLI arguments
        None => {
            let mk_wavefront = args.is_present("wavefront");
            let mk_console = args.is_present("console");

            let (wport, whost) = if mk_wavefront {
                (Some(u16::from_str(args.value_of("wavefront-port").unwrap()).unwrap()),
                 Some(args.value_of("wavefront-host").unwrap().to_string()))
            } else {
                (None, None)
            };

            let verb = if args.is_present("verbose") {
                args.occurrences_of("verbose")
            } else {
                0
            };

            let qos = MetricQOS::default();

            Args {
                statsd_port: u16::from_str(args.value_of("statsd-port").unwrap())
                    .expect("statsd-port must be an integer"),
                graphite_port: u16::from_str(args.value_of("graphite-port").unwrap())
                    .expect("graphite-port must be an integer"),
                flush_interval: u64::from_str(args.value_of("flush-interval").unwrap())
                    .expect("flush-interval must be an integer"),
                console: mk_console,
                wavefront: mk_wavefront,
                wavefront_port: wport,
                wavefront_host: whost,
                tags: args.value_of("tags").unwrap().to_string(),
                qos: qos,
                verbose: verb,
                version: VERSION.unwrap().to_string(),
            }
        }
    }
}
