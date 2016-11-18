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
use metric::TagMap;
use std::path::{Path, PathBuf};
use rusoto::Region;

const VERSION: Option<&'static str> = option_env!("CARGO_PKG_VERSION");

use super::source::{FederationReceiverConfig, GraphiteConfig, StatsdConfig, FileServerConfig};
use super::sink::{WavefrontConfig, ConsoleConfig, FederationTransmitterConfig, NullConfig,
                  FirehoseConfig};

#[derive(Debug)]
pub struct Args {
    pub data_directory: PathBuf,
    pub statsd_config: Option<StatsdConfig>,
    pub graphite_config: Option<GraphiteConfig>,
    pub fed_receiver_config: Option<FederationReceiverConfig>,
    pub flush_interval: u64,
    pub console: Option<ConsoleConfig>,
    pub null: Option<NullConfig>,
    pub wavefront: Option<WavefrontConfig>,
    pub fed_transmitter: Option<FederationTransmitterConfig>,
    pub firehosen: Vec<FirehoseConfig>,
    pub files: Vec<FileServerConfig>,
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
            let wavefront = if args.is_present("wavefront") {
                Some(WavefrontConfig {
                    port: u16::from_str(args.value_of("wavefront-port").unwrap()).unwrap(),
                    host: args.value_of("wavefront-host").unwrap().to_string(),
                    bin_width: 1,
                })
            } else {
                None
            };
            let null = if args.is_present("null") {
                Some(NullConfig::default())
            } else {
                None
            };
            let console = if args.is_present("console") {
                Some(ConsoleConfig::default())
            } else {
                None
            };

            let verb = if args.is_present("verbose") {
                args.occurrences_of("verbose")
            } else {
                0
            };

            let mut graphite_config = GraphiteConfig::default();
            if let Some(gport_str) = args.value_of("graphite-port") {
                graphite_config.port = u16::from_str(gport_str)
                    .expect("graphite-port must be an integer");
            }

            let mut statsd_config = StatsdConfig::default();
            if let Some(sport_str) = args.value_of("statsd-port") {
                statsd_config.port = u16::from_str(sport_str)
                    .expect("statsd-port must be an integer");
            }

            Args {
                data_directory: Path::new("/tmp/cernan-data").to_path_buf(),
                statsd_config: Some(statsd_config),
                graphite_config: Some(graphite_config),
                flush_interval: u64::from_str(args.value_of("flush-interval").unwrap())
                    .expect("flush-interval must be an integer"),
                fed_receiver_config: None,
                console: console,
                null: null,
                wavefront: wavefront,
                firehosen: Vec::default(),
                fed_transmitter: None,
                files: Default::default(),
                verbose: verb,
                version: VERSION.unwrap().to_string(),
            }
        }
    }
}

pub fn parse_config_file(buffer: String, verbosity: u64) -> Args {
    let value: toml::Value = buffer.parse().unwrap();

    let tags: TagMap = match value.lookup("tags") {
        Some(tbl) => {
            let mut tags = TagMap::default();
            let ttbl = tbl.as_table().unwrap();
            for (k, v) in ttbl.iter() {
                tags.insert(String::from(k.clone()),
                            String::from(v.as_str().unwrap().to_string()));
            }
            tags
        }
        None => TagMap::default(),
    };

    let null = if value.lookup("null").is_some() {
        Some(NullConfig {})
    } else {
        None
    };

    let console = if value.lookup("console").is_some() {
        Some(ConsoleConfig {
            bin_width: value.lookup("console.bin_width")
                .unwrap_or(&Value::Integer(1))
                .as_integer()
                .map(|i| i as i64)
                .unwrap(),
        })
    } else {
        None
    };

    let wavefront = if value.lookup("wavefront").is_some() {
        Some(WavefrontConfig {
            port: value.lookup("wavefront.port")
                .unwrap_or(&Value::Integer(2878))
                .as_integer()
                .map(|i| i as u16)
                .unwrap(),
            host: value.lookup("wavefront.host")
                .unwrap_or(&Value::String("127.0.0.1".to_string()))
                .as_str()
                .map(|s| s.to_string())
                .unwrap(),
            bin_width: value.lookup("wavefront.bin_width")
                .unwrap_or(&Value::Integer(1))
                .as_integer()
                .map(|i| i as i64)
                .unwrap(),
        })
    } else {
        None
    };

    let fedtrn = if value.lookup("federation_transmitter").is_some() {
        Some(FederationTransmitterConfig {
            port: value.lookup("federation_transmitter.port")
                .unwrap_or(&Value::Integer(1972))
                .as_integer()
                .map(|i| i as u16)
                .unwrap(),
            host: value.lookup("federation_transmitter.host")
                .unwrap_or(&Value::String("127.0.0.1".to_string()))
                .as_str()
                .map(|s| s.to_string())
                .unwrap(),
        })
    } else {
        None
    };

    let mut files = Vec::new();
    if let Some(array) = value.lookup("file") {
        for tbl in array.as_slice().unwrap() {
            match tbl.lookup("path") {
                Some(pth) => {
                    let path = Path::new(pth.as_str().unwrap());
                    let config = FileServerConfig {
                        path: path.to_path_buf(),
                        tags: tags.clone(),
                    };
                    files.push(config)
                }
                None => continue,
            }
        }
    }

    let mut firehosen: Vec<FirehoseConfig> = Vec::new();
    if let Some(array) = value.lookup("firehose") {
        if let Some(tbl) = array.as_slice() {
            for val in tbl {
                match val.lookup("delivery_stream").map(|x| x.as_str()) {
                    Some(ds) => {
                        let bs = val.lookup("batch_size")
                            .unwrap_or(&Value::Integer(450))
                            .as_integer()
                            .map(|i| i as usize)
                            .unwrap();
                        let r = val.lookup("region")
                            .unwrap_or(&Value::String("us-west-2".into()))
                            .as_str()
                            .map(|s| match s {
                                "ap-northeast-1" => Region::ApNortheast1,
                                "ap-northeast-2" => Region::ApNortheast2,
                                "ap-south-1" => Region::ApSouth1,
                                "ap-southeast-1" => Region::ApSoutheast1,
                                "ap-southeast-2" => Region::ApSoutheast2,
                                "cn-north-1" => Region::CnNorth1,
                                "eu-central-1" => Region::EuCentral1,
                                "eu-west-1" => Region::EuWest1,
                                "sa-east-1" => Region::SaEast1,
                                "us-east-1" => Region::UsEast1,
                                "us-west-1" => Region::UsWest1,
                                "us-west-2" => Region::UsWest2,
                                _ => Region::UsWest2,
                            });
                        firehosen.push(FirehoseConfig {
                            delivery_stream: ds.unwrap().to_string(),
                            batch_size: bs,
                            region: r.unwrap(),
                        })
                    }
                    None => continue,
                }
            }
        }
    }

    let statsd_config = match value.lookup("statsd-port") {
        Some(p) => {
            let mut sconfig = StatsdConfig::default();
            sconfig.port = p.as_integer().expect("statsd-port must be integer") as u16;
            sconfig.tags = tags.clone();
            Some(sconfig)
        }
        None => {
            let is_enabled = value.lookup("statsd.enabled")
                .unwrap_or(&Value::Boolean(true))
                .as_bool()
                .expect("must be a bool");
            if is_enabled {
                let mut sconfig = StatsdConfig::default();
                if let Some(p) = value.lookup("statsd.port") {
                    sconfig.port = p.as_integer().expect("statsd-port must be integer") as u16;
                }
                sconfig.tags = tags.clone();
                Some(sconfig)
            } else {
                None
            }
        }
    };

    let graphite_config = match value.lookup("graphite-port") {
        Some(p) => {
            let mut gconfig = GraphiteConfig::default();
            gconfig.port = p.as_integer().expect("graphite-port must be integer") as u16;
            gconfig.tags = tags.clone();
            Some(gconfig)
        }
        None => {
            let is_enabled = value.lookup("graphite.enabled")
                .unwrap_or(&Value::Boolean(true))
                .as_bool()
                .expect("must be a bool");
            if is_enabled {
                let mut gconfig = GraphiteConfig::default();
                if let Some(p) = value.lookup("graphite.port") {
                    gconfig.port = p.as_integer().expect("graphite-port must be integer") as u16;
                }
                gconfig.tags = tags.clone();
                Some(gconfig)
            } else {
                None
            }
        }
    };

    let federation_receiver_config = if value.lookup("federation_receiver").is_some() {
        let port = match value.lookup("federation_receiver.port") {
            Some(p) => p.as_integer().expect("fed_receiver.port must be integer") as u16,
            None => 1972,
        };
        let ip = match value.lookup("federation_receiver.ip") {
            Some(p) => p.as_str().unwrap(),
            None => "0.0.0.0",
        };
        Some(FederationReceiverConfig {
            ip: ip.to_owned(),
            port: port,
            tags: tags.clone(),
        })
    } else {
        None
    };

    Args {
        data_directory: value.lookup("data-directory")
            .unwrap_or(&Value::String("/tmp/cernan-data".to_string()))
            .as_str()
            .map(|s| Path::new(s).to_path_buf())
            .unwrap(),
        statsd_config: statsd_config,
        graphite_config: graphite_config,
        fed_receiver_config: federation_receiver_config,
        flush_interval: value.lookup("flush-interval")
            .unwrap_or(&Value::Integer(60))
            .as_integer()
            .expect("flush-interval must be integer") as u64,
        console: console,
        null: null,
        wavefront: wavefront,
        firehosen: firehosen,
        fed_transmitter: fedtrn,
        files: files,
        verbose: verbosity,
        version: VERSION.unwrap().to_string(),
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use metric::TagMap;
    use std::path::PathBuf;
    use rusoto::Region;

    #[test]
    fn config_file_default() {
        let config = "".to_string();

        let args = parse_config_file(config, 4);

        assert!(args.statsd_config.is_some());
        assert_eq!(args.statsd_config.unwrap().port, 8125);
        assert!(args.graphite_config.is_some());
        assert_eq!(args.graphite_config.unwrap().port, 2003);
        assert!(args.fed_receiver_config.is_none());
        assert_eq!(args.flush_interval, 60);
        assert!(args.console.is_none());
        assert!(args.null.is_none());
        assert_eq!(true, args.firehosen.is_empty());
        assert!(args.wavefront.is_none());
        assert!(args.fed_transmitter.is_none());
        assert_eq!(args.verbose, 4);
    }

    #[test]
    fn config_fed_receiver() {
        let config = r#"
[federation_receiver]
port = 1987
"#
            .to_string();

        let args = parse_config_file(config, 4);

        assert!(args.fed_receiver_config.is_some());
        let fed_receiver_config = args.fed_receiver_config.unwrap();
        assert_eq!(fed_receiver_config.port, 1987);
        assert_eq!(fed_receiver_config.ip, String::from("0.0.0.0"));
    }

    #[test]
    fn config_fed_receiver_ip() {
        let config = r#"
[federation_receiver]
ip = "127.0.0.1"
"#
            .to_string();

        let args = parse_config_file(config, 4);

        assert!(args.fed_receiver_config.is_some());
        let fed_receiver_config = args.fed_receiver_config.unwrap();
        assert_eq!(fed_receiver_config.port, 1972);
        assert_eq!(fed_receiver_config.ip, String::from("127.0.0.1"));
    }

    #[test]
    fn config_fed_transmitter() {
        let config = r#"
[federation_transmitter]
port = 1987
"#
            .to_string();

        let args = parse_config_file(config, 4);

        assert!(args.fed_transmitter.is_some());
        let fed_transmitter = args.fed_transmitter.unwrap();
        assert_eq!(fed_transmitter.host, String::from("127.0.0.1"));
        assert_eq!(fed_transmitter.port, 1987);
    }

    #[test]
    fn config_fed_transmitter_distinct_host() {
        let config = r#"
[federation_transmitter]
host = "foo.example.com"
"#
            .to_string();

        let args = parse_config_file(config, 4);

        assert!(args.fed_transmitter.is_some());
        let fed_transmitter = args.fed_transmitter.unwrap();
        assert_eq!(fed_transmitter.host, String::from("foo.example.com"));
        assert_eq!(fed_transmitter.port, 1972);
    }

    #[test]
    fn config_statsd_backward_compat() {
        let config = r#"
statsd-port = 1024
"#
            .to_string();

        let args = parse_config_file(config, 4);

        assert!(args.statsd_config.is_some());
        assert_eq!(args.statsd_config.unwrap().port, 1024);
    }

    #[test]
    fn config_statsd() {
        let config = r#"
[statsd]
port = 1024
"#
            .to_string();

        let args = parse_config_file(config, 4);

        assert!(args.statsd_config.is_some());
        assert_eq!(args.statsd_config.unwrap().port, 1024);
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

        assert!(args.statsd_config.is_none());
    }

    #[test]
    fn config_graphite_backward_compat() {
        let config = r#"
graphite-port = 1024
"#
            .to_string();

        let args = parse_config_file(config, 4);

        assert!(args.graphite_config.is_some());
        assert_eq!(args.graphite_config.unwrap().port, 1024);
    }

    #[test]
    fn config_graphite() {
        let config = r#"
[graphite]
port = 1024
"#
            .to_string();

        let args = parse_config_file(config, 4);

        assert!(args.graphite_config.is_some());
        assert_eq!(args.graphite_config.unwrap().port, 1024);
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

        assert!(args.graphite_config.is_none());
    }

    #[test]
    fn config_file_wavefront() {
        let config = r#"
[wavefront]
port = 3131
host = "example.com"
bin_width = 9
"#
            .to_string();

        let args = parse_config_file(config, 4);

        assert!(args.wavefront.is_some());
        let wavefront = args.wavefront.unwrap();
        assert_eq!(wavefront.host, String::from("example.com"));
        assert_eq!(wavefront.port, 3131);
        assert_eq!(wavefront.bin_width, 9);
    }

    #[test]
    fn config_file_console() {
        let config = r#"
[console]
"#
            .to_string();

        let args = parse_config_file(config, 4);

        assert!(args.console.is_some());
        assert_eq!(args.console.unwrap().bin_width, 1);
    }

    #[test]
    fn config_file_console_explicit_bin_width() {
        let config = r#"
[console]
bin_width = 9
"#
            .to_string();

        let args = parse_config_file(config, 4);

        assert!(args.console.is_some());
        assert_eq!(args.console.unwrap().bin_width, 9);
    }

    #[test]
    fn config_file_null() {
        let config = r#"
[null]
"#
            .to_string();

        let args = parse_config_file(config, 4);

        assert!(args.null.is_some());
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

        assert_eq!(args.firehosen.len(), 2);

        assert_eq!(args.firehosen[0].delivery_stream, "stream_one");
        assert_eq!(args.firehosen[0].batch_size, 450);
        assert_eq!(args.firehosen[0].region, Region::UsWest2);

        assert_eq!(args.firehosen[1].delivery_stream, "stream_two");
        assert_eq!(args.firehosen[1].batch_size, 450);
        assert_eq!(args.firehosen[1].region, Region::UsWest2);
    }

    #[test]
    fn config_file_firehose_complicated() {
        let config = r#"
[[firehose]]
delivery_stream = "stream_one"
batch_size = 20

[[firehose]]
delivery_stream = "stream_two"
batch_size = 800
region = "us-east-1"
"#
            .to_string();

        let args = parse_config_file(config, 4);

        assert_eq!(args.firehosen.len(), 2);

        assert_eq!(args.firehosen[0].delivery_stream, "stream_one");
        assert_eq!(args.firehosen[0].batch_size, 20);
        assert_eq!(args.firehosen[0].region, Region::UsWest2);

        assert_eq!(args.firehosen[1].delivery_stream, "stream_two");
        assert_eq!(args.firehosen[1].batch_size, 800);
        assert_eq!(args.firehosen[1].region, Region::UsEast1);
    }

    #[test]
    fn config_file_file_source_single() {
        let config = r#"
[[file]]
path = "/foo/bar.txt"
"#
            .to_string();

        let args = parse_config_file(config, 4);

        assert!(!args.files.is_empty());
        assert_eq!(args.files[0].path, PathBuf::from("/foo/bar.txt"));
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

        assert!(!args.files.is_empty());
        assert_eq!(args.files[0].path, PathBuf::from("/foo/bar.txt"));
        assert_eq!(args.files[1].path, PathBuf::from("/bar.txt"));
    }

    #[test]
    fn config_file_tags() {
        let config = r#"
[tags]
source = "cernan"
purpose = "serious_business"
mission = "from_gad"

[wavefront]
"#
            .to_string();

        let args = parse_config_file(config, 4);
        let mut tags = TagMap::default();
        tags.insert(String::from("mission"), String::from("from_gad"));
        tags.insert(String::from("purpose"), String::from("serious_business"));
        tags.insert(String::from("source"), String::from("cernan"));

        assert_eq!(args.graphite_config.unwrap().tags, tags);
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

"#
            .to_string();

        let args = parse_config_file(config, 4);
        println!("ARGS : {:?}", args);
        let mut tags = TagMap::default();
        tags.insert(String::from("mission"), String::from("from_gad"));
        tags.insert(String::from("purpose"), String::from("serious_business"));
        tags.insert(String::from("source"), String::from("cernan"));

        assert!(args.statsd_config.is_some());
        let statsd_config = args.statsd_config.unwrap();
        assert_eq!(statsd_config.port, 1024);
        assert_eq!(statsd_config.tags, tags);
        assert!(args.graphite_config.is_some());
        let graphite_config = args.graphite_config.unwrap();
        assert_eq!(graphite_config.port, 1034);
        assert_eq!(graphite_config.tags, tags);
        assert_eq!(args.flush_interval, 128);
        assert!(args.console.is_some());
        assert!(args.null.is_some());
        assert!(args.firehosen.is_empty());
        assert!(args.wavefront.is_some());
        let wavefront = args.wavefront.unwrap();
        assert_eq!(wavefront.host, String::from("example.com"));
        assert_eq!(wavefront.port, 3131);
        assert_eq!(args.verbose, 4);
    }
}
