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

const VERSION: Option<&'static str> = option_env!("CARGO_PKG_VERSION");

#[derive(Clone, Debug)]
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
        .arg(Arg::with_name("config-file")
             .long("config")
             .short("C")
             .value_name("config")
             .help("The config file to feed in.")
             .takes_value(true))
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
                    for (k,v) in (*ttbl).iter() {
                        write!(tags, "{}={},", k, v.as_str().unwrap()).unwrap();
                    }
                    tags.pop();
                    tags
                },
                None => "".to_string()
            };

            let mk_wavefront = value.lookup("backends.wavefront").is_some();
            let mk_console = value.lookup("backends.console").is_some();
            let mk_librato = value.lookup("backends.librato").is_some();

            let (wport, whost, wskpaggr) = if mk_wavefront {
                (
                    // wavefront port
                    value.lookup("backends.wavefront.port").unwrap_or(&Value::Integer(2878)).as_integer().map(|i| i as u16),
                    // wavefront host
                    value.lookup("backends.wavefront.host").unwrap_or(&Value::String("127.0.0.1".to_string())).as_str().map(|s| s.to_string()),
                    // wavefront skip aggrs
                    value.lookup("backends.wavefront.skip-aggrs").unwrap_or(&Value::Boolean(false)).as_bool().unwrap_or(false),
                )
            } else {
                (None, None, false)
            };

            let (luser, lhost, ltoken) = if mk_librato {
                (
                    // librato username
                    value.lookup("backends.librato.username").unwrap_or(&Value::String("statsd".to_string())).as_str().map(|s| s.to_string()),
                    // librato token
                    value.lookup("backends.librato.token").unwrap_or(&Value::String("statsd".to_string())).as_str().map(|s| s.to_string()),
                    // librato host
                    value.lookup("backends.librato.host").unwrap_or(&Value::String("https://metrics-api.librato.com/v1/metrics".to_string())).as_str().map(|s| s.to_string()),
                )
            } else {
                (None, None, None)
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
                    .expect("flush-interval must be integer") as u64,
                console: mk_console,
                wavefront: mk_wavefront,
                librato: mk_librato,
                wavefront_port: wport,
                wavefront_host: whost,
                wavefront_skip_aggrs: wskpaggr,
                librato_username: luser,
                librato_host: lhost,
                librato_token: ltoken,
                tags: tags,
                verbose: verb,
                version: VERSION.unwrap().to_string(),
            }
        }
        None => {
            Args {
                statsd_port: 8125,
                graphite_port: 2003,
                flush_interval: 10,
                console: false,
                wavefront: false,
                librato: false,
                wavefront_port: None,
                wavefront_host: None,
                wavefront_skip_aggrs: true,
                librato_username: None,
                librato_host: None,
                librato_token: None,
                tags: "source=cernan".to_string(),
                verbose: verb,
                version: VERSION.unwrap().to_string(),
            }
        }
    }
}
