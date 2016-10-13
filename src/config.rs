//! Provides the CLI option parser
//!
//! Used to parse the argv/config file into a struct that
//! the server can consume and use as configuration data.

use clap::{Arg, App};
use std::fs::File;
use toml;
use toml::Value;
use std::io::Read;
use std::str::FromStr;
use metric::{MetricQOS, TagMap};
use std::path::{Path,PathBuf};

const VERSION: Option<&'static str> = option_env!("CARGO_PKG_VERSION");

#[derive(Clone, Debug)]
pub struct Args {
    pub data_directory: PathBuf,
    pub statsd_port: Option<u16>,
    pub graphite_port: Option<u16>,
    pub fed_receiver_port: Option<u16>,
    pub fed_receiver_ip: Option<String>,
    pub flush_interval: u64,
    pub console: bool,
    pub null: bool,
    pub firehose_delivery_streams: Vec<String>,
    pub wavefront: bool,
    pub wavefront_port: Option<u16>,
    pub wavefront_host: Option<String>,
    pub fed_transmitter: bool,
    pub fed_transmitter_host: Option<String>,
    pub fed_transmitter_port: Option<u16>,
    pub qos: MetricQOS,
    pub tags: TagMap,
    pub files: Option<Vec<PathBuf>>,
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
        // We read from a configuration file
        Some(filename) => {
            let mut fp = match File::open(filename) {
                Err(e) => panic!("Could not open file {} with error {}", filename, e),
                Ok(fp) => fp,
            };

            let mut buffer = String::new();
            fp.read_to_string(&mut buffer).unwrap();
            parse_config_file(buffer, verb)
        }
        // We read from CLI arguments
        None => {
            let mk_wavefront = args.is_present("wavefront");
            let mk_null = args.is_present("null");
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
                data_directory: Path::new("/tmp/cernan-data").to_path_buf(),
                statsd_port: Some(u16::from_str(args.value_of("statsd-port").unwrap())
                    .expect("statsd-port must be an integer")),
                graphite_port: Some(u16::from_str(args.value_of("graphite-port").unwrap())
                    .expect("graphite-port must be an integer")),
                flush_interval: u64::from_str(args.value_of("flush-interval").unwrap())
                    .expect("flush-interval must be an integer"),
                fed_receiver_port: None,
                fed_receiver_ip: None,
                console: mk_console,
                null: mk_null,
                wavefront: mk_wavefront,
                firehose_delivery_streams: Vec::default(),
                wavefront_port: wport,
                wavefront_host: whost,
                fed_transmitter: false,
                fed_transmitter_port: None,
                fed_transmitter_host: None,
                tags: TagMap::default(),
                qos: qos,
                files: None,
                verbose: verb,
                version: VERSION.unwrap().to_string(),
            }
        }
    }
}

pub fn parse_config_file(buffer: String, verbosity: u64) -> Args {
    let value: toml::Value = buffer.parse().unwrap();

    let tags : TagMap = match value.lookup("tags") {
        Some(tbl) => {
            let mut tags = TagMap::default();
            let ttbl = tbl.as_table().unwrap();
            for (k, v) in ttbl.iter() {
                tags.insert(String::from(k.clone()), String::from(v.as_str().unwrap().to_string()));
            }
            tags
        }
        None => TagMap::default(),
    };

    let qos = match value.lookup("quality-of-service") {
        Some(tbl) => {
            let ttbl = tbl.as_table().unwrap();
            let mut hm = MetricQOS::default();
            for (k, v) in &(*ttbl) {
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
    let mk_fedtrn = value.lookup("federation_transmitter").is_some();
    let mk_null = value.lookup("null").is_some();
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

    let (fedtrn_port, fedtrn_host) = if mk_fedtrn {
        ( // fed_receiver port
            value.lookup("federation_transmitter.port")
                .unwrap_or(&Value::Integer(1972))
                .as_integer()
                .map(|i| i as u16),
            // fed_receiver host
            value.lookup("federation_transmitter.host")
                .unwrap_or(&Value::String("127.0.0.1".to_string()))
                .as_str()
                .map(|s| s.to_string()))
    } else {
        (None, None)
    };

    let mut paths = Vec::new();
    if let Some(array) = value.lookup("file") {
        for tbl in array.as_slice().unwrap() {
            match tbl.lookup("path") {
                Some(pth) => {
                    let path = Path::new(pth.as_str().unwrap());
                    paths.push(path.to_path_buf())
                }
                None => continue,
            }
        }
    }

    let mut fh_delivery_streams : Vec<String> = Vec::new();
    if let Some(array) = value.lookup("firehose") {
        for tbl in array.as_slice() {
            for val in tbl {
                match val.lookup("delivery_stream") {
                    Some(ds) => {
                        fh_delivery_streams.push(ds.as_str().unwrap().to_owned())
                    }
                    None => continue,
                }
            }
        }
    }

    let statsd_port = match value.lookup("statsd-port") {
        Some(p) => Some(p.as_integer().expect("statsd-port must be integer") as u16),
        None => {
            let is_enabled = value.lookup("statsd.enabled").unwrap_or(&Value::Boolean(true)).as_bool().expect("must be a bool");
            if is_enabled {
                match value.lookup("statsd.port") {
                    Some(p) => Some(p.as_integer().expect("statsd-port must be integer") as u16),
                    None => Some(8125),
                }
            } else {
                None
            }
        }
    };

    let graphite_port = match value.lookup("graphite-port") {
        Some(p) => Some(p.as_integer().expect("graphite-port must be integer") as u16),
        None => {
            let is_enabled = value.lookup("graphite.enabled").unwrap_or(&Value::Boolean(true)).as_bool().expect("must be a bool");
            if is_enabled {
                match value.lookup("graphite.port") {
                    Some(p) => Some(p.as_integer().expect("graphite-port must be integer") as u16),
                    None => Some(2003),
                }
            } else {
                None
            }
        }
    };

    let (fed_receiver_port, fed_receiver_ip) = if value.lookup("federation_receiver").is_some() {
        (
            match value.lookup("federation_receiver.port") {
                Some(p) => Some(p.as_integer().expect("fed_receiver.port must be integer") as u16),
                None => Some(1972),
            },
            match value.lookup("federation_receiver.ip") {
                Some(p) => Some(p.as_str().unwrap().to_owned()),
                None => Some(String::from("0.0.0.0")),
            }
            )
    } else {
        (None, None)
    };

    Args {
        data_directory: value.lookup("data-directory")
            .unwrap_or(&Value::String("/tmp/cernan-data".to_string()))
            .as_str()
            .map(|s| Path::new(s).to_path_buf())
            .unwrap(),
        statsd_port: statsd_port,
        graphite_port: graphite_port,
        fed_receiver_port: fed_receiver_port,
        fed_receiver_ip: fed_receiver_ip,
        flush_interval: value.lookup("flush-interval")
            .unwrap_or(&Value::Integer(60))
            .as_integer()
            .expect("flush-interval must be integer") as u64,
        console: mk_console,
        null: mk_null,
        wavefront: mk_wavefront,
        firehose_delivery_streams: fh_delivery_streams,
        wavefront_port: wport,
        wavefront_host: whost,
        fed_transmitter: mk_fedtrn,
        fed_transmitter_port: fedtrn_port,
        fed_transmitter_host: fedtrn_host,
        tags: tags,
        qos: qos,
        files: Some(paths),
        verbose: verbosity,
        version: VERSION.unwrap().to_string(),
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use metric::{MetricQOS,TagMap};
    use std::path::PathBuf;

    #[test]
    fn config_file_default() {
        let config = "".to_string();

        let args = parse_config_file(config, 4);

        assert_eq!(args.statsd_port, Some(8125));
        assert_eq!(args.graphite_port, Some(2003));
        assert_eq!(args.fed_receiver_port, None);
        assert_eq!(args.fed_receiver_ip, None);
        assert_eq!(args.flush_interval, 60);
        assert_eq!(args.console, false);
        assert_eq!(args.null, false);
        assert_eq!(true, args.firehose_delivery_streams.is_empty());
        assert_eq!(args.wavefront, false);
        assert_eq!(args.wavefront_host, None);
        assert_eq!(args.wavefront_port, None);
        assert_eq!(args.fed_transmitter, false);
        assert_eq!(args.fed_transmitter_port, None);
        assert_eq!(args.tags, TagMap::default());
        assert_eq!(args.qos, MetricQOS::default());
        assert_eq!(args.verbose, 4);
    }

    #[test]
    fn config_fed_receiver() {
        let config = r#"
[federation_receiver]
port = 1987
"#.to_string();

        let args = parse_config_file(config, 4);

        assert_eq!(args.statsd_port, Some(8125));
        assert_eq!(args.graphite_port, Some(2003));
        assert_eq!(args.fed_receiver_port, Some(1987));
        assert_eq!(args.fed_receiver_ip, Some(String::from("0.0.0.0")));
        assert_eq!(args.flush_interval, 60);
        assert_eq!(args.console, false);
        assert_eq!(args.null, false);
        assert_eq!(true, args.firehose_delivery_streams.is_empty());
        assert_eq!(args.wavefront, false);
        assert_eq!(args.wavefront_host, None);
        assert_eq!(args.wavefront_port, None);
        assert_eq!(args.tags, TagMap::default());
        assert_eq!(args.qos, MetricQOS::default());
        assert_eq!(args.verbose, 4);
    }

    #[test]
    fn config_fed_receiver_ip() {
        let config = r#"
[federation_receiver]
ip = "127.0.0.1"
"#.to_string();

        let args = parse_config_file(config, 4);

        assert_eq!(args.statsd_port, Some(8125));
        assert_eq!(args.graphite_port, Some(2003));
        assert_eq!(args.fed_receiver_port, Some(1972));
        assert_eq!(args.fed_receiver_ip, Some(String::from("127.0.0.1")));
        assert_eq!(args.flush_interval, 60);
        assert_eq!(args.console, false);
        assert_eq!(args.null, false);
        assert_eq!(true, args.firehose_delivery_streams.is_empty());
        assert_eq!(args.wavefront, false);
        assert_eq!(args.wavefront_host, None);
        assert_eq!(args.wavefront_port, None);
        assert_eq!(args.tags, TagMap::default());
        assert_eq!(args.qos, MetricQOS::default());
        assert_eq!(args.verbose, 4);
    }

    #[test]
    fn config_fed_transmitter() {
        let config = r#"
[federation_transmitter]
port = 1987
"#.to_string();

        let args = parse_config_file(config, 4);

        assert_eq!(args.statsd_port, Some(8125));
        assert_eq!(args.graphite_port, Some(2003));
        assert_eq!(args.fed_receiver_port, None);
        assert_eq!(args.flush_interval, 60);
        assert_eq!(args.console, false);
        assert_eq!(args.null, false);
        assert_eq!(true, args.firehose_delivery_streams.is_empty());
        assert_eq!(args.wavefront, false);
        assert_eq!(args.wavefront_host, None);
        assert_eq!(args.wavefront_port, None);
        assert_eq!(args.fed_transmitter, true);
        assert_eq!(args.fed_transmitter_host, Some(String::from("127.0.0.1")));
        assert_eq!(args.fed_transmitter_port, Some(1987));
        assert_eq!(args.tags, TagMap::default());
        assert_eq!(args.qos, MetricQOS::default());
        assert_eq!(args.verbose, 4);
    }

    #[test]
    fn config_fed_transmitter_distinct_host() {
        let config = r#"
[federation_transmitter]
host = "foo.example.com"
"#.to_string();

        let args = parse_config_file(config, 4);

        assert_eq!(args.statsd_port, Some(8125));
        assert_eq!(args.graphite_port, Some(2003));
        assert_eq!(args.fed_receiver_port, None);
        assert_eq!(args.flush_interval, 60);
        assert_eq!(args.console, false);
        assert_eq!(args.null, false);
        assert_eq!(true, args.firehose_delivery_streams.is_empty());
        assert_eq!(args.wavefront, false);
        assert_eq!(args.wavefront_host, None);
        assert_eq!(args.wavefront_port, None);
        assert_eq!(args.fed_transmitter, true);
        assert_eq!(args.fed_transmitter_host, Some(String::from("foo.example.com")));
        assert_eq!(args.fed_transmitter_port, Some(1972));
        assert_eq!(args.tags, TagMap::default());
        assert_eq!(args.qos, MetricQOS::default());
        assert_eq!(args.verbose, 4);
    }

    #[test]
    fn config_statsd_backward_compat() {
        let config = r#"
statsd-port = 1024
"#
            .to_string();

        let args = parse_config_file(config, 4);

        assert_eq!(args.statsd_port, Some(1024));
        assert_eq!(args.graphite_port, Some(2003));
        assert_eq!(args.flush_interval, 60);
        assert_eq!(args.console, false);
        assert_eq!(args.null, false);
        assert_eq!(true, args.firehose_delivery_streams.is_empty());
        assert_eq!(args.wavefront, false);
        assert_eq!(args.wavefront_host, None);
        assert_eq!(args.wavefront_port, None);
        assert_eq!(args.tags, TagMap::default());
        assert_eq!(args.qos, MetricQOS::default());
        assert_eq!(args.verbose, 4);
    }

    #[test]
    fn config_statsd() {
        let config = r#"
[statsd]
port = 1024
"#
            .to_string();

        let args = parse_config_file(config, 4);

        assert_eq!(args.statsd_port, Some(1024));
        assert_eq!(args.graphite_port, Some(2003));
        assert_eq!(args.flush_interval, 60);
        assert_eq!(args.console, false);
        assert_eq!(args.null, false);
        assert_eq!(true, args.firehose_delivery_streams.is_empty());
        assert_eq!(args.wavefront, false);
        assert_eq!(args.wavefront_host, None);
        assert_eq!(args.wavefront_port, None);
        assert_eq!(args.tags, TagMap::default());
        assert_eq!(args.qos, MetricQOS::default());
        assert_eq!(args.verbose, 4);
    }

    #[test]
    fn config_statsd_disabled() {
        let config = r#"
[statsd]
enabled = false
port = 1024
"#
            .to_string();

        let args = parse_config_file(config, 4);

        assert_eq!(args.statsd_port, None);
        assert_eq!(args.graphite_port, Some(2003));
        assert_eq!(args.flush_interval, 60);
        assert_eq!(args.console, false);
        assert_eq!(args.null, false);
        assert_eq!(true, args.firehose_delivery_streams.is_empty());
        assert_eq!(args.wavefront, false);
        assert_eq!(args.wavefront_host, None);
        assert_eq!(args.wavefront_port, None);
        assert_eq!(args.tags, TagMap::default());
        assert_eq!(args.qos, MetricQOS::default());
        assert_eq!(args.verbose, 4);
    }

    #[test]
    fn config_graphite_backward_compat() {
        let config = r#"
graphite-port = 1024
"#
            .to_string();

        let args = parse_config_file(config, 4);

        assert_eq!(args.statsd_port, Some(8125));
        assert_eq!(args.graphite_port, Some(1024));
        assert_eq!(args.flush_interval, 60);
        assert_eq!(args.console, false);
        assert_eq!(args.null, false);
        assert_eq!(true, args.firehose_delivery_streams.is_empty());
        assert_eq!(args.wavefront, false);
        assert_eq!(args.wavefront_host, None);
        assert_eq!(args.wavefront_port, None);
        assert_eq!(args.tags, TagMap::default());
        assert_eq!(args.qos, MetricQOS::default());
        assert_eq!(args.verbose, 4);
    }

    #[test]
    fn config_graphite() {
        let config = r#"
[graphite]
port = 1024
"#
            .to_string();

        let args = parse_config_file(config, 4);

        assert_eq!(args.statsd_port, Some(8125));
        assert_eq!(args.graphite_port, Some(1024));
        assert_eq!(args.flush_interval, 60);
        assert_eq!(args.console, false);
        assert_eq!(args.null, false);
        assert_eq!(true, args.firehose_delivery_streams.is_empty());
        assert_eq!(args.wavefront, false);
        assert_eq!(args.wavefront_host, None);
        assert_eq!(args.wavefront_port, None);
        assert_eq!(args.tags, TagMap::default());
        assert_eq!(args.qos, MetricQOS::default());
        assert_eq!(args.verbose, 4);
    }

    #[test]
    fn config_graphite_disabled() {
        let config = r#"
[graphite]
enabled = false
port = 1024
"#
            .to_string();

        let args = parse_config_file(config, 4);

        assert_eq!(args.statsd_port, Some(8125));
        assert_eq!(args.graphite_port, None);
        assert_eq!(args.flush_interval, 60);
        assert_eq!(args.console, false);
        assert_eq!(args.null, false);
        assert_eq!(true, args.firehose_delivery_streams.is_empty());
        assert_eq!(args.wavefront, false);
        assert_eq!(args.wavefront_host, None);
        assert_eq!(args.wavefront_port, None);
        assert_eq!(args.tags, TagMap::default());
        assert_eq!(args.qos, MetricQOS::default());
        assert_eq!(args.verbose, 4);
    }

    #[test]
    fn config_file_wavefront() {
        let config = r#"
[wavefront]
port = 3131
host = "example.com"
"#
            .to_string();

        let args = parse_config_file(config, 4);

        assert_eq!(args.statsd_port, Some(8125));
        assert_eq!(args.graphite_port, Some(2003));
        assert_eq!(args.flush_interval, 60);
        assert_eq!(args.console, false);
        assert_eq!(args.null, false);
        assert_eq!(true, args.firehose_delivery_streams.is_empty());
        assert_eq!(args.wavefront, true);
        assert_eq!(args.wavefront_host, Some("example.com".to_string()));
        assert_eq!(args.wavefront_port, Some(3131));
        assert_eq!(args.tags, TagMap::default());
        assert_eq!(args.qos, MetricQOS::default());
        assert_eq!(args.verbose, 4);
    }

    #[test]
    fn config_file_console() {
        let config = r#"
[console]
"#
            .to_string();

        let args = parse_config_file(config, 4);

        assert_eq!(args.statsd_port, Some(8125));
        assert_eq!(args.graphite_port, Some(2003));
        assert_eq!(args.flush_interval, 60);
        assert_eq!(args.console, true);
        assert_eq!(args.null, false);
        assert_eq!(true, args.firehose_delivery_streams.is_empty());
        assert_eq!(args.wavefront, false);
        assert_eq!(args.wavefront_host, None);
        assert_eq!(args.wavefront_port, None);
        assert_eq!(args.tags, TagMap::default());
        assert_eq!(args.qos, MetricQOS::default());
        assert_eq!(args.verbose, 4);
    }

    #[test]
    fn config_file_null() {
        let config = r#"
[null]
"#
            .to_string();

        let args = parse_config_file(config, 4);

        assert_eq!(args.statsd_port, Some(8125));
        assert_eq!(args.graphite_port, Some(2003));
        assert_eq!(args.flush_interval, 60);
        assert_eq!(args.console, false);
        assert_eq!(args.null, true);
        assert_eq!(true, args.firehose_delivery_streams.is_empty());
        assert_eq!(args.wavefront, false);
        assert_eq!(args.wavefront_host, None);
        assert_eq!(args.wavefront_port, None);
        assert_eq!(args.tags, TagMap::default());
        assert_eq!(args.qos, MetricQOS::default());
        assert_eq!(args.verbose, 4);
    }

    #[test]
    fn config_file_firehose() {
        let config = r#"
[[firehose]]
delivery_stream = "stream_one"

[[firehose]]
delivery_stream = "stream_two"
"#
            .to_string();

        let args = parse_config_file(config, 4);

        assert_eq!(args.statsd_port, Some(8125));
        assert_eq!(args.graphite_port, Some(2003));
        assert_eq!(args.flush_interval, 60);
        assert_eq!(args.console, false);
        assert_eq!(args.null, false);
        assert_eq!(args.firehose_delivery_streams, vec!["stream_one", "stream_two"]);
        assert_eq!(args.wavefront, false);
        assert_eq!(args.wavefront_host, None);
        assert_eq!(args.wavefront_port, None);
        assert_eq!(args.tags, TagMap::default());
        assert_eq!(args.qos, MetricQOS::default());
        assert_eq!(args.verbose, 4);
    }

    #[test]
    fn config_file_file_source_single() {
        let config = r#"
[[file]]
path = "/foo/bar.txt"
"#
            .to_string();

        let args = parse_config_file(config, 4);

        assert_eq!(args.statsd_port, Some(8125));
        assert_eq!(args.graphite_port, Some(2003));
        assert_eq!(args.flush_interval, 60);
        assert_eq!(args.console, false);
        assert_eq!(args.null, false);
        assert_eq!(true, args.firehose_delivery_streams.is_empty());
        assert_eq!(args.files, Some(vec![PathBuf::from("/foo/bar.txt")]));
        assert_eq!(args.wavefront, false);
        assert_eq!(args.wavefront_host, None);
        assert_eq!(args.wavefront_port, None);
        assert_eq!(args.tags, TagMap::default());
        assert_eq!(args.qos, MetricQOS::default());
        assert_eq!(args.verbose, 4);
    }

    #[test]
    fn config_file_file_source_multiple() {
        let config = r#"
[[file]]
path = "/foo/bar.txt"

[[file]]
path = "/bar.txt"
"#
            .to_string();

        let args = parse_config_file(config, 4);

        assert_eq!(args.statsd_port, Some(8125));
        assert_eq!(args.graphite_port, Some(2003));
        assert_eq!(args.flush_interval, 60);
        assert_eq!(args.console, false);
        assert_eq!(args.null, false);
        assert_eq!(true, args.firehose_delivery_streams.is_empty());
        assert_eq!(args.files, Some(vec![PathBuf::from("/foo/bar.txt"), PathBuf::from("/bar.txt")]));
        assert_eq!(args.wavefront, false);
        assert_eq!(args.wavefront_host, None);
        assert_eq!(args.wavefront_port, None);
        assert_eq!(args.tags, TagMap::default());
        assert_eq!(args.qos, MetricQOS::default());
        assert_eq!(args.verbose, 4);
    }

    #[test]
    fn config_file_tags() {
        let config = r#"
[tags]
source = "cernan"
purpose = "serious_business"
mission = "from_gad"
"#
            .to_string();

        let args = parse_config_file(config, 4);
        let mut tags = TagMap::default();
        tags.insert(String::from("mission"), String::from("from_gad"));
        tags.insert(String::from("purpose"), String::from("serious_business"));
        tags.insert(String::from("source"), String::from("cernan"));

        assert_eq!(args.statsd_port, Some(8125));
        assert_eq!(args.graphite_port, Some(2003));
        assert_eq!(args.flush_interval, 60);
        assert_eq!(args.console, false);
        assert_eq!(args.null, false);
        assert_eq!(true, args.firehose_delivery_streams.is_empty());
        assert_eq!(args.wavefront, false);
        assert_eq!(args.wavefront_host, None);
        assert_eq!(args.wavefront_port, None);
        assert_eq!(args.tags, tags);
        assert_eq!(args.qos, MetricQOS::default());
        assert_eq!(args.verbose, 4);
    }

    #[test]
    fn config_file_qos() {
        let config = r#"
[quality-of-service]
gauge = 110
counter = 12
timer = 605
histogram = 609
raw = 42
"#
            .to_string();

        let args = parse_config_file(config, 4);

        assert_eq!(args.statsd_port, Some(8125));
        assert_eq!(args.graphite_port, Some(2003));
        assert_eq!(args.flush_interval, 60);
        assert_eq!(args.console, false);
        assert_eq!(args.null, false);
        assert_eq!(true, args.firehose_delivery_streams.is_empty());
        assert_eq!(args.wavefront, false);
        assert_eq!(args.wavefront_host, None);
        assert_eq!(args.wavefront_port, None);
        assert_eq!(args.tags, TagMap::default());
        assert_eq!(args.qos,
                   MetricQOS {
                       gauge: 110,
                       counter: 12,
                       timer: 605,
                       histogram: 609,
                       raw: 42,
                   });
        assert_eq!(args.verbose, 4);
    }


    #[test]
    fn config_file_full() {
        let config = r#"
statsd-port = 1024
graphite-port = 1034

flush-interval = 128

[wavefront]
port = 3131
host = "example.com"

[console]

[null]

[firehose]

[tags]
source = "cernan"
purpose = "serious_business"
mission = "from_gad"

[quality-of-service]
gauge = 110
counter = 12
timer = 605
histogram = 609
raw = 42
"#
            .to_string();

        let args = parse_config_file(config, 4);
        println!("ARGS : {:?}", args);
        let mut tags = TagMap::default();
        tags.insert(String::from("mission"), String::from("from_gad"));
        tags.insert(String::from("purpose"), String::from("serious_business"));
        tags.insert(String::from("source"), String::from("cernan"));

        assert_eq!(args.statsd_port, Some(1024));
        assert_eq!(args.graphite_port, Some(1034));
        assert_eq!(args.flush_interval, 128);
        assert_eq!(args.console, true);
        assert_eq!(args.null, true);
        assert_eq!(true, args.firehose_delivery_streams.is_empty());
        assert_eq!(args.wavefront, true);
        assert_eq!(args.wavefront_host, Some("example.com".to_string()));
        assert_eq!(args.wavefront_port, Some(3131));
        assert_eq!(args.tags, tags);
        assert_eq!(args.qos,
                   MetricQOS {
                       gauge: 110,
                       counter: 12,
                       timer: 605,
                       histogram: 609,
                       raw: 42,
                   });
        assert_eq!(args.verbose, 4);
    }
}
