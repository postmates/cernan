//! Provides the CLI option parser
//!
//! Used to parse the argv/config file into a struct that
//! the server can consume and use as configuration data.

use clap::{App, Arg};
use metric::TagMap;
use rusoto::Region;
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use toml;
use toml::Value;

const VERSION: Option<&'static str> = option_env!("CARGO_PKG_VERSION");

use super::filter::ProgrammableFilterConfig;
use super::sink::{ConsoleConfig, FirehoseConfig, InfluxDBConfig, NativeConfig, NullConfig,
                  PrometheusConfig, WavefrontConfig};
use super::source::{FileServerConfig, GraphiteConfig, NativeServerConfig, StatsdConfig};

#[derive(Debug)]
pub struct Args {
    pub console: Option<ConsoleConfig>,
    pub data_directory: PathBuf,
    pub influxdb: Option<InfluxDBConfig>,
    pub prometheus: Option<PrometheusConfig>,
    pub files: Vec<FileServerConfig>,
    pub filters: HashMap<String, ProgrammableFilterConfig>,
    pub firehosen: Vec<FirehoseConfig>,
    pub global_flush_interval: u64,
    pub graphites: HashMap<String, GraphiteConfig>,
    pub native_sink_config: Option<NativeConfig>,
    pub native_server_config: Option<NativeServerConfig>,
    pub null: Option<NullConfig>,
    pub scripts_directory: PathBuf,
    pub statsds: HashMap<String, StatsdConfig>,
    pub verbose: u64,
    pub version: String,
    pub wavefront: Option<WavefrontConfig>,
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
            let global_flush_interval = 60;
            let wavefront = if args.is_present("wavefront") {
                let percentiles = vec![("min".to_string(), 0.0),
                                       ("max".to_string(), 1.0),
                                       ("2".to_string(), 0.02),
                                       ("9".to_string(), 0.09),
                                       ("25".to_string(), 0.25),
                                       ("50".to_string(), 0.5),
                                       ("75".to_string(), 0.75),
                                       ("90".to_string(), 0.90),
                                       ("91".to_string(), 0.91),
                                       ("95".to_string(), 0.95),
                                       ("98".to_string(), 0.98),
                                       ("99".to_string(), 0.99),
                                       ("999".to_string(), 0.999)];
                Some(WavefrontConfig {
                    port: u16::from_str(args.value_of("wavefront-port").unwrap()).unwrap(),
                    host: args.value_of("wavefront-host").unwrap().to_string(),
                    bin_width: 1,
                    config_path: "sinks.wavefront".to_string(),
                    percentiles: percentiles,
                    tags: Default::default(),
                    flush_interval: global_flush_interval,
                })
            } else {
                None
            };
            let null = if args.is_present("null") {
                Some(NullConfig::new("sinks.null".to_string()))
            } else {
                None
            };
            let console = if args.is_present("console") {
                Some(ConsoleConfig::new("sinks.console".to_string(), global_flush_interval))
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
            let mut graphites = HashMap::new();
            graphites.insert("sources.graphite".to_string(), graphite_config);

            let mut statsd_config = StatsdConfig::default();
            if let Some(sport_str) = args.value_of("statsd-port") {
                statsd_config.port = u16::from_str(sport_str)
                    .expect("statsd-port must be an integer");
            }
            let mut statsds = HashMap::new();
            statsds.insert("sources.statsd".to_string(), statsd_config);

            Args {
                data_directory: Path::new("/tmp/cernan-data").to_path_buf(),
                scripts_directory: Path::new("/tmp/cernan-scripts").to_path_buf(),
                statsds: statsds,
                graphites: graphites,
                native_server_config: None,
                native_sink_config: None,
                console: console,
                null: null,
                wavefront: wavefront,
                influxdb: None,
                prometheus: None,
                firehosen: Vec::default(),
                global_flush_interval: global_flush_interval,
                files: Default::default(),
                filters: Default::default(),
                verbose: verb,
                version: VERSION.unwrap().to_string(),
            }
        }
    }
}

pub fn parse_config_file(buffer: String, verbosity: u64) -> Args {
    let value: toml::Value = buffer.parse().unwrap();

    let scripts_dir: PathBuf = value.lookup("scripts-directory")
        .unwrap_or(&Value::String("/tmp/cernan-scripts".to_string()))
        .as_str()
        .map(|s| Path::new(s).to_path_buf())
        .expect("could not parse scripts-directory");

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

    let global_flush_interval = value.lookup("global_flush_interval")
        .unwrap_or(&Value::Integer(60))
        .as_integer()
        .unwrap();

    let null = if value.lookup("null").or(value.lookup("sinks.null")).is_some() {
        Some(NullConfig { config_path: "sinks.null".to_string() })
    } else {
        None
    };

    let console = if value.lookup("console").or(value.lookup("sinks.console")).is_some() {
        Some(ConsoleConfig {
            bin_width: value.lookup("console.bin_width")
                .or(value.lookup("sinks.console.bin_width"))
                .unwrap_or(&Value::Integer(1))
                .as_integer()
                .expect("could not parse sinks.console.bin_width"),
            config_path: "sinks.console".to_string(),
            flush_interval: value.lookup("console.flush_interval")
                .or(value.lookup("sinks.console.flush_interval"))
                .unwrap_or(&Value::Integer(global_flush_interval))
                .as_integer()
                .map(|i| i as u64)
                .unwrap(),
        })
    } else {
        None
    };

    let wavefront = if value.lookup("wavefront").or(value.lookup("sinks.wavefront")).is_some() {
        let mut prcnt = Vec::new();
        if let Some(tbl) = value.lookup("wavefront.percentiles")
            .or(value.lookup("sinks.wavefront.percentiles"))
            .and_then(|t| t.as_slice()) {
            for opt_val in tbl.iter().map(|x| x.as_slice()) {
                match opt_val {
                    Some(val) => {
                        let k: String =
                            val[0].as_str().expect("percentile name must be a string").to_string();
                        let v: f64 = val[1]
                            .as_str()
                            .expect("percentile value must be a string of a float")
                            .parse()
                            .expect("percentile value must be a float");
                        prcnt.push((k.clone(), v));
                    }
                    None => {}
                }
            }
        }
        let percentiles = if prcnt.is_empty() {
            vec![("min".to_string(), 0.0),
                 ("max".to_string(), 1.0),
                 ("2".to_string(), 0.02),
                 ("9".to_string(), 0.09),
                 ("25".to_string(), 0.25),
                 ("50".to_string(), 0.5),
                 ("75".to_string(), 0.75),
                 ("90".to_string(), 0.90),
                 ("91".to_string(), 0.91),
                 ("95".to_string(), 0.95),
                 ("98".to_string(), 0.98),
                 ("99".to_string(), 0.99),
                 ("999".to_string(), 0.999)]
        } else {
            prcnt
        };

        Some(WavefrontConfig {
            port: value.lookup("wavefront.port")
                .or(value.lookup("sinks.wavefront.port"))
                .unwrap_or(&Value::Integer(2878))
                .as_integer()
                .map(|i| i as u16)
                .expect("could not parse sinks.wavefront.port"),
            host: value.lookup("wavefront.host")
                .or(value.lookup("sinks.wavefront.host"))
                .unwrap_or(&Value::String("127.0.0.1".to_string()))
                .as_str()
                .map(|s| s.to_string())
                .expect("could not parse sinks.wavefront.host"),
            bin_width: value.lookup("wavefront.bin_width")
                .or(value.lookup("sinks.wavefront.bin_width"))
                .unwrap_or(&Value::Integer(1))
                .as_integer()
                .expect("could not parse sinks.wavefront.bin_width"),
            config_path: "sinks.wavefront".to_string(),
            percentiles: percentiles,
            tags: tags.clone(),
            flush_interval: value.lookup("wavefront.flush_interval")
                .or(value.lookup("sinks.wavefront.flush_interval"))
                .unwrap_or(&Value::Integer(global_flush_interval))
                .as_integer()
                .map(|i| i as u64)
                .unwrap(),
        })
    } else {
        None
    };

    let influxdb = if value.lookup("influxdb").or(value.lookup("sinks.influxdb")).is_some() {
        Some(InfluxDBConfig {
            port: value.lookup("influxdb.port")
                .or(value.lookup("sinks.influxdb.port"))
                .unwrap_or(&Value::Integer(8089))
                .as_integer()
                .map(|i| i as u16)
                .expect("could not parse sinks.influxdb.port"),
            host: value.lookup("influxdb.host")
                .or(value.lookup("sinks.influxdb.host"))
                .unwrap_or(&Value::String("127.0.0.1".to_string()))
                .as_str()
                .map(|s| s.to_string())
                .expect("could not parse sinks.influxdb.host"),
            bin_width: value.lookup("influxdb.bin_width")
                .or(value.lookup("sinks.influxdb.bin_width"))
                .unwrap_or(&Value::Integer(1))
                .as_integer()
                .expect("could not parse sinks.influxdb.bin_width"),
            config_path: "sinks.influxdb".to_string(),
            tags: tags.clone(),
            flush_interval: value.lookup("influxdb.flush_interval")
                .or(value.lookup("sinks.influxdb.flush_interval"))
                .unwrap_or(&Value::Integer(global_flush_interval))
                .as_integer()
                .map(|i| i as u64)
                .unwrap(),
        })
    } else {
        None
    };

    let prometheus = if value.lookup("prometheus").or(value.lookup("sinks.prometheus")).is_some() {
        Some(PrometheusConfig {
            port: value.lookup("prometheus.port")
                .or(value.lookup("sinks.prometheus.port"))
                .unwrap_or(&Value::Integer(8086))
                .as_integer()
                .map(|i| i as u16)
                .expect("could not parse sinks.prometheus.port"),
            host: value.lookup("prometheus.host")
                .or(value.lookup("sinks.prometheus.host"))
                .unwrap_or(&Value::String("127.0.0.1".to_string()))
                .as_str()
                .map(|s| s.to_string())
                .expect("could not parse sinks.prometheus.host"),
            bin_width: value.lookup("prometheus.bin_width")
                .or(value.lookup("sinks.prometheus.bin_width"))
                .unwrap_or(&Value::Integer(1))
                .as_integer()
                .expect("could not parse sinks.prometheus.bin_width"),
            config_path: "sinks.prometheus".to_string(),
            flush_interval: value.lookup("prometheus.flush_interval")
                .or(value.lookup("sinks.prometheus.flush_interval"))
                .unwrap_or(&Value::Integer(global_flush_interval))
                .as_integer()
                .map(|i| i as u64)
                .unwrap(),
        })
    } else {
        None
    };

    let native_sink_config = if value.lookup("sinks.native").is_some() {
        Some(NativeConfig {
            port: value.lookup("sinks.native.port")
                .unwrap_or(&Value::Integer(1972))
                .as_integer()
                .map(|i| i as u16)
                .expect("could not parse sinks.native.port"),
            host: value.lookup("sinks.native.host")
                .unwrap_or(&Value::String("127.0.0.1".to_string()))
                .as_str()
                .map(|s| s.to_string())
                .expect("could not parse sinks.native.host"),
            config_path: "sinks.native".to_string(),
            flush_interval: value.lookup("sinks.native.flush_interval")
                .unwrap_or(&Value::Integer(global_flush_interval))
                .as_integer()
                .map(|i| i as u64)
                .unwrap(),
        })
    } else {
        None
    };

    let mut files = Vec::new();
    match value.lookup("file") {
        Some(array) => {
            for tbl in array.as_slice().unwrap() {
                match tbl.lookup("path") {
                    Some(pth) => {
                        let path = Path::new(pth.as_str().unwrap());
                        let fwds = match tbl.lookup("forwards") {
                            Some(fwds) => {
                                fwds.as_slice()
                                    .expect("forwards must be an array")
                                    .to_vec()
                                    .iter()
                                    .map(|s| s.as_str().unwrap().to_string())
                                    .collect()
                            }
                            None => Vec::new(),
                        };
                        let path_buf = path.to_path_buf();
                        let config = FileServerConfig {
                            path: path_buf.clone(),
                            tags: tags.clone(),
                            forwards: fwds,
                            config_path: format!("sources.files.{}", path_buf.to_str().unwrap()),
                        };
                        files.push(config)
                    }
                    None => continue,
                }
            }
        }
        None => {
            if let Some(tbls) = value.lookup("sources.files") {
                for tbl in tbls.as_table().unwrap().values() {
                    match tbl.lookup("path") {
                        Some(pth) => {
                            let path = Path::new(pth.as_str().unwrap());
                            let fwds = match tbl.lookup("forwards") {
                                Some(fwds) => {
                                    fwds.as_slice()
                                        .expect("forwards must be an array")
                                        .to_vec()
                                        .iter()
                                        .map(|s| s.as_str().unwrap().to_string())
                                        .collect()
                                }
                                None => Vec::new(),
                            };
                            let path_buf = path.to_path_buf();
                            let config = FileServerConfig {
                                path: path_buf.clone(),
                                tags: tags.clone(),
                                forwards: fwds,
                                config_path: format!("sources.files.{}",
                                                     path_buf.to_str().unwrap()),
                            };
                            files.push(config)
                        }
                        None => continue,
                    }
                }
            }
        }
    }

    let mut filters: HashMap<String, ProgrammableFilterConfig> = HashMap::new();
    if let Some(tbls) = value.lookup("filters") {
        for (name, tbl) in tbls.as_table().unwrap().iter() {
            match tbl.lookup("script") {
                Some(pth) => {
                    let path = Path::new(pth.as_str().unwrap());
                    let fwds = match tbl.lookup("forwards") {
                        Some(fwds) => {
                            fwds.as_slice()
                                .expect("forwards must be an array")
                                .to_vec()
                                .iter()
                                .map(|s| s.as_str().unwrap().to_string())
                                .collect()
                        }
                        None => Vec::new(),
                    };
                    let config_path = format!("filters.{}", name);
                    let config = ProgrammableFilterConfig {
                        script: scripts_dir.join(path),
                        forwards: fwds,
                        config_path: config_path.clone(),
                        tags: tags.clone(),
                    };
                    filters.insert(config_path, config);
                }
                None => continue,
            }
        }
    }


    let mut firehosen: Vec<FirehoseConfig> = Vec::new();
    match value.lookup("firehose") {
        Some(array) => {
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
                                    "us-west-2" | _ => Region::UsWest2,
                                });
                            let delivery_stream = ds.unwrap().to_string();
                            let flush_interval = val.lookup("flush_interval")
                                .unwrap_or(&Value::Integer(global_flush_interval))
                                .as_integer()
                                .map(|i| i as u64)
                                .unwrap();
                            firehosen.push(FirehoseConfig {
                                delivery_stream: delivery_stream.clone(),
                                batch_size: bs,
                                region: r.unwrap(),
                                config_path: format!("sinks.firehose.{}", delivery_stream),
                                flush_interval: flush_interval,
                            })
                        }
                        None => continue,
                    }
                }
            }
        }
        None => {
            if let Some(tbls) = value.lookup("sinks.firehose") {
                for (key, tbl) in tbls.as_table().unwrap().iter() {
                    match tbl.lookup("delivery_stream").map(|x| x.as_str()) {
                        Some(ds) => {
                            let bs = tbl.lookup("batch_size")
                                .unwrap_or(&Value::Integer(450))
                                .as_integer()
                                .map(|i| i as usize)
                                .unwrap();
                            let r = tbl.lookup("region")
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
                                    "us-west-2" | _ => Region::UsWest2,
                                });
                            let flush_interval = tbl.lookup("flush_interval")
                                .unwrap_or(&Value::Integer(global_flush_interval))
                                .as_integer()
                                .map(|i| i as u64)
                                .unwrap();
                            firehosen.push(FirehoseConfig {
                                delivery_stream: ds.unwrap().to_string(),
                                batch_size: bs,
                                region: r.unwrap(),
                                config_path: format!("sinks.firehose.{}", key),
                                flush_interval: flush_interval,
                            })
                        }
                        None => continue,
                    }
                }
            }
        }
    }

    let mut statsds = HashMap::new();
    match value.lookup("statsd-port") {
        Some(p) => {
            let mut sconfig = StatsdConfig::default();
            sconfig.port = p.as_integer().expect("statsd-port must be integer") as u16;
            sconfig.tags = tags.clone();
            statsds.insert("sources.statsd".to_string(), sconfig);
        }
        None => {
            if let Some(tbls) = value.lookup("sources.statsd") {
                for (name, tbl) in tbls.as_table().unwrap().iter() {
                    let is_enabled = tbl.lookup("enabled")
                        .unwrap_or(&Value::Boolean(true))
                        .as_bool()
                        .expect("must be a bool");
                    if is_enabled {
                        let mut sconfig = StatsdConfig::default();
                        if let Some(p) = tbl.lookup("port") {
                            sconfig.port = p.as_integer().expect("statsd-port must be integer") as
                                           u16;
                        }
                        if let Some(p) = tbl.lookup("host") {
                            sconfig.host = p.as_str().unwrap().to_string();
                        }
                        if let Some(p) = tbl.lookup("delete-gauges") {
                            sconfig.delete_gauges = p.as_bool()
                                .expect("statsd delete-gauges must be boolean") as
                                                    bool;
                        }
                        if let Some(fwds) = tbl.lookup("forwards") {
                            sconfig.forwards = fwds.as_slice()
                                .expect("forwards must be an array")
                                .to_vec()
                                .iter()
                                .map(|s| s.as_str().unwrap().to_string())
                                .collect();
                        }
                        sconfig.tags = tags.clone();
                        statsds.insert(format!("sources.statsd.{}", name), sconfig);
                    }
                }
            } else {
                let is_enabled = value.lookup("statsd.enabled")
                    .unwrap_or(&Value::Boolean(true))
                    .as_bool()
                    .expect("must be a bool");
                if is_enabled {
                    let mut sconfig = StatsdConfig::default();
                    if let Some(p) = value.lookup("statsd.port") {
                        sconfig.port = p.as_integer().expect("statsd-port must be integer") as u16;
                    }
                    if let Some(fwds) = value.lookup("statsd.forwards") {
                        sconfig.forwards = fwds.as_slice()
                            .expect("forwards must be an array")
                            .to_vec()
                            .iter()
                            .map(|s| s.as_str().unwrap().to_string())
                            .collect();
                    }
                    sconfig.tags = tags.clone();
                    statsds.insert("sources.statsd".to_string(), sconfig);
                }
            }
        }
    };

    let mut graphites = HashMap::new();
    match value.lookup("graphite-port") {
        Some(p) => {
            let mut gconfig = GraphiteConfig::default();
            gconfig.port = p.as_integer().expect("graphite-port must be integer") as u16;
            gconfig.tags = tags.clone();
            graphites.insert("sources.graphite".to_string(), gconfig);
        }
        None => {
            if let Some(tbls) = value.lookup("sources.graphite") {
                for (name, tbl) in tbls.as_table().unwrap().iter() {
                    let is_enabled = tbl.lookup("enabled")
                        .unwrap_or(&Value::Boolean(true))
                        .as_bool()
                        .expect("must be a bool");
                    if is_enabled {
                        let mut gconfig = GraphiteConfig::default();
                        if let Some(p) = tbl.lookup("port") {
                            gconfig.port = p.as_integer().expect("graphite-port must be integer") as
                                           u16;
                        }
                        if let Some(p) = tbl.lookup("host") {
                            gconfig.host = p.as_str().unwrap().to_string();
                        }
                        if let Some(fwds) = tbl.lookup("forwards") {
                            gconfig.forwards = fwds.as_slice()
                                .expect("forwards must be an array")
                                .to_vec()
                                .iter()
                                .map(|s| s.as_str().unwrap().to_string())
                                .collect();
                        }
                        gconfig.tags = tags.clone();
                        graphites.insert(format!("sources.graphite.{}", name), gconfig);
                    }
                }
            } else {
                let is_enabled = value.lookup("graphite.enabled")
                    .unwrap_or(&Value::Boolean(true))
                    .as_bool()
                    .expect("must be a bool");
                if is_enabled {
                    let mut gconfig = GraphiteConfig::default();
                    if let Some(p) = value.lookup("graphite.port") {
                        gconfig.port = p.as_integer().expect("graphite-port must be integer") as
                                       u16;
                    }
                    if let Some(fwds) = value.lookup("graphite.forwards") {
                        gconfig.forwards = fwds.as_slice()
                            .expect("forwards must be an array")
                            .to_vec()
                            .iter()
                            .map(|s| s.as_str().unwrap().to_string())
                            .collect();
                    }
                    gconfig.tags = tags.clone();
                    graphites.insert("sources.graphite".to_string(), gconfig);
                }
            }
        }
    };

    let native_server_config = if value.lookup("sources.native")
        .is_some() {
        let port = match value.lookup("sources.native.port") {
            Some(p) => p.as_integer().expect("fed_server.port must be integer") as u16,
            None => 1972,
        };
        let ip = match value.lookup("sources.native.ip") {
            Some(p) => p.as_str().unwrap(),
            None => "0.0.0.0",
        };
        let fwds = match value.lookup("sources.native.forwards") {
            Some(fwds) => {
                fwds.as_slice()
                    .expect("forwards must be an array")
                    .to_vec()
                    .iter()
                    .map(|s| s.as_str().unwrap().to_string())
                    .collect()
            }
            None => Vec::new(),
        };
        Some(NativeServerConfig {
            ip: ip.to_owned(),
            port: port,
            tags: tags.clone(),
            forwards: fwds,
            config_path: "sources.native".to_string(),
        })
    } else {
        None
    };

    Args {
        data_directory: value.lookup("data-directory")
            .unwrap_or(&Value::String("/tmp/cernan-data".to_string()))
            .as_str()
            .map(|s| Path::new(s).to_path_buf())
            .expect("could not parse data-directory"),
        scripts_directory: scripts_dir,
        statsds: statsds,
        graphites: graphites,
        native_sink_config: native_sink_config,
        native_server_config: native_server_config,
        global_flush_interval: value.lookup("global-flush-interval")
            .unwrap_or(&Value::Integer(60))
            .as_integer()
            .expect("global-flush-interval must be integer") as u64,
        console: console,
        null: null,
        wavefront: wavefront,
        influxdb: influxdb,
        prometheus: prometheus,
        firehosen: firehosen,
        files: files,
        filters: filters,
        verbose: verbosity,
        version: VERSION.unwrap().to_string(),
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use filter::ProgrammableFilterConfig;
    use metric::TagMap;
    use rusoto::Region;
    use std::path::{Path, PathBuf};

    #[test]
    fn config_file_data_directory() {
        let config = r#"
data-directory = "/foo/bar"
"#
            .to_string();
        let args = parse_config_file(config, 4);
        let dir = Path::new("/foo/bar").to_path_buf();

        assert_eq!(args.data_directory, dir);
    }

    #[test]
    fn config_file_data_directory_default() {
        let config = r#""#.to_string();
        let args = parse_config_file(config, 4);
        let dir = Path::new("/tmp/cernan-data").to_path_buf();

        assert_eq!(args.data_directory, dir);
    }

    #[test]
    fn config_file_scripts_directory() {
        let config = r#"
scripts-directory = "/foo/bar"
"#
            .to_string();
        let args = parse_config_file(config, 4);
        let dir = Path::new("/foo/bar").to_path_buf();

        assert_eq!(args.scripts_directory, dir);
    }

    #[test]
    fn config_file_scripts_directory_default() {
        let config = r#""#.to_string();
        let args = parse_config_file(config, 4);
        let dir = Path::new("/tmp/cernan-scripts").to_path_buf();

        assert_eq!(args.scripts_directory, dir);
    }

    #[test]
    fn config_file_default() {
        let config = "".to_string();

        let args = parse_config_file(config, 4);

        assert!(!args.statsds.is_empty());
        assert_eq!(args.statsds.get("sources.statsd").unwrap().port, 8125);
        assert!(!args.graphites.is_empty());
        assert_eq!(args.graphites.get("sources.graphite").unwrap().port, 2003);
        assert_eq!(args.global_flush_interval, 60);
        assert!(args.console.is_none());
        assert!(args.null.is_none());
        assert_eq!(true, args.firehosen.is_empty());
        assert!(args.wavefront.is_none());
        assert_eq!(args.verbose, 4);
    }

    #[test]
    fn config_fed_receiver_sources_style() {
        let config = r#"
[sources]
  [sources.native]
  ip = "127.0.0.1"
  port = 1972
"#
            .to_string();

        let args = parse_config_file(config, 4);

        assert!(args.native_server_config.is_some());
        let native_server_config = args.native_server_config.unwrap();
        assert_eq!(native_server_config.port, 1972);
        assert_eq!(native_server_config.ip, String::from("127.0.0.1"));
    }

    #[test]
    fn config_native_sink_config_distinct_host_sinks_style() {
        let config = r#"
[sinks]
  [sinks.native]
  host = "foo.example.com"
  port = 1972
  flush_interval = 120
"#
            .to_string();

        let args = parse_config_file(config, 4);

        assert!(args.native_sink_config.is_some());
        let native_sink_config = args.native_sink_config.unwrap();
        assert_eq!(native_sink_config.host, String::from("foo.example.com"));
        assert_eq!(native_sink_config.port, 1972);
        assert_eq!(native_sink_config.flush_interval, 120);
    }

    #[test]
    fn config_statsd_backward_compat() {
        let config = r#"
statsd-port = 1024
"#
            .to_string();

        let args = parse_config_file(config, 4);

        assert_eq!(args.statsds.len(), 1);

        let config0 = args.statsds.get("sources.statsd").unwrap();
        assert_eq!(config0.port, 1024);
    }

    #[test]
    fn config_statsd() {
        let config = r#"
[statsd]
port = 1024
"#
            .to_string();

        let args = parse_config_file(config, 4);

        assert!(!args.statsds.is_empty());
        assert_eq!(args.statsds.get("sources.statsd").unwrap().port, 1024);
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

        assert!(args.statsds.is_empty());
    }

    #[test]
    fn config_statsd_sources_style() {
        let config = r#"
[sources]
  [sources.statsd.primary]
  enabled = true
  host = "localhost"
  port = 1024
  delete-gauges = true
  forwards = ["sinks.console", "sinks.null"]
"#
            .to_string();

        let args = parse_config_file(config, 4);

        assert_eq!(args.statsds.len(), 1);

        let config0 = args.statsds.get("sources.statsd.primary").unwrap();
        assert_eq!(config0.delete_gauges, true);
        assert_eq!(config0.host, "localhost");
        assert_eq!(config0.port, 1024);
        assert_eq!(config0.forwards,
                   vec!["sinks.console".to_string(), "sinks.null".to_string()]);
    }

    #[test]
    fn config_statsd_sources_style_multiple() {
        let config = r#"
[sources]
  [sources.statsd.lower]
  enabled = true
  port = 1024
  delete-gauges = true
  forwards = ["sinks.console", "sinks.null"]

  [sources.statsd.higher]
  enabled = true
  port = 4048
  forwards = ["sinks.wavefront"]
"#
            .to_string();

        let args = parse_config_file(config, 4);

        assert_eq!(args.statsds.len(), 2);

        let config0 = args.statsds.get("sources.statsd.lower").unwrap();
        assert_eq!(config0.port, 1024);
        assert_eq!(config0.delete_gauges, true);
        assert_eq!(config0.forwards,
                   vec!["sinks.console".to_string(), "sinks.null".to_string()]);

        let config1 = args.statsds.get("sources.statsd.higher").unwrap();
        assert_eq!(config1.port, 4048);
        assert_eq!(config1.delete_gauges, false);
        assert_eq!(config1.forwards, vec!["sinks.wavefront".to_string()]);
    }

    #[test]
    fn config_graphite_backward_compat() {
        let config = r#"
graphite-port = 1024
"#
            .to_string();

        let args = parse_config_file(config, 4);

        assert_eq!(args.graphites.len(), 1);

        let config0 = args.graphites.get("sources.graphite").unwrap();
        assert_eq!(config0.port, 1024);
    }

    #[test]
    fn config_graphite() {
        let config = r#"
[graphite]
port = 1024
"#
            .to_string();

        let args = parse_config_file(config, 4);

        assert_eq!(args.graphites.len(), 1);

        let config0 = args.graphites.get("sources.graphite").unwrap();
        assert_eq!(config0.port, 1024);
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

        assert!(args.graphites.is_empty());
    }

    #[test]
    fn config_graphite_sources_style() {
        let config = r#"
[sources]
  [sources.graphite.primary]
  enabled = true
  host = "localhost"
  port = 2003
  forwards = ["filters.collectd_scrub"]
"#
            .to_string();

        let args = parse_config_file(config, 4);

        println!("{:?}", args.graphites);
        assert_eq!(args.graphites.len(), 1);

        let config0 = args.graphites.get("sources.graphite.primary").unwrap();
        assert_eq!(config0.port, 2003);
        assert_eq!(config0.host, "localhost");
        assert_eq!(config0.forwards, vec!["filters.collectd_scrub".to_string()]);
    }

    #[test]
    fn config_graphite_sources_style_multiple() {
        let config = r#"
[sources]
  [sources.graphite.lower]
  enabled = true
  port = 2003
  forwards = ["filters.collectd_scrub"]

  [sources.graphite.higher]
  enabled = true
  port = 2004
  forwards = ["sinks.wavefront"]
"#
            .to_string();

        let args = parse_config_file(config, 4);

        println!("{:?}", args.graphites);
        assert_eq!(args.graphites.len(), 2);

        let config0 = args.graphites.get("sources.graphite.lower").unwrap();
        assert_eq!(config0.port, 2003);
        assert_eq!(config0.forwards, vec!["filters.collectd_scrub".to_string()]);

        let config1 = args.graphites.get("sources.graphite.higher").unwrap();
        assert_eq!(config1.port, 2004);
        assert_eq!(config1.forwards, vec!["sinks.wavefront".to_string()]);
    }

    #[test]
    fn config_filters_sources_style() {
        let config = r#"
[filters]
  [filters.collectd_scrub]
  script = "cernan_bridge.lua"
  forwards = ["sinks.console"]
"#
            .to_string();

        let args = parse_config_file(config, 4);

        assert_eq!(args.filters.len(), 1);

        println!("{:?}", args.filters);
        let config0: &ProgrammableFilterConfig =
            args.filters.get("filters.collectd_scrub").unwrap();
        assert_eq!(config0.script.to_str().unwrap(),
                   "/tmp/cernan-scripts/cernan_bridge.lua");
        assert_eq!(config0.forwards, vec!["sinks.console"]);
    }

    #[test]
    fn config_filters_sources_style_non_default() {
        let config = r#"
scripts-directory = "data/"
[filters]
  [filters.collectd_scrub]
  script = "cernan_bridge.lua"
  forwards = ["sinks.console"]
"#
            .to_string();

        let args = parse_config_file(config, 4);

        assert_eq!(args.filters.len(), 1);

        println!("{:?}", args.filters);
        let config0: &ProgrammableFilterConfig =
            args.filters.get("filters.collectd_scrub").unwrap();
        assert_eq!(config0.script.to_str().unwrap(), "data/cernan_bridge.lua");
        assert_eq!(config0.forwards, vec!["sinks.console"]);
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
    fn config_file_wavefront_sinks_style() {
        let config = r#"
[sinks]
  [sinks.wavefront]
  port = 3131
  host = "example.com"
  bin_width = 9
  flush_interval = 15
"#
            .to_string();

        let args = parse_config_file(config, 4);

        assert!(args.wavefront.is_some());
        let wavefront = args.wavefront.unwrap();
        assert_eq!(wavefront.host, String::from("example.com"));
        assert_eq!(wavefront.port, 3131);
        assert_eq!(wavefront.bin_width, 9);
        assert_eq!(wavefront.flush_interval, 15);
    }

    #[test]
    fn config_file_wavefront_percentile_specification() {
        let config = r#"
[sinks]
  [sinks.wavefront]
  port = 3131
  host = "example.com"
  bin_width = 9
  percentiles = [ ["min", "0.0"], ["max", "1.0"], ["median", "0.5"] ]
"#
            .to_string();

        let args = parse_config_file(config, 4);

        assert!(args.wavefront.is_some());
        let wavefront = args.wavefront.unwrap();
        assert_eq!(wavefront.host, String::from("example.com"));
        assert_eq!(wavefront.port, 3131);
        assert_eq!(wavefront.bin_width, 9);

        assert_eq!(wavefront.percentiles.len(), 3);
        assert_eq!(wavefront.percentiles[0], ("min".to_string(), 0.0));
        assert_eq!(wavefront.percentiles[1], ("max".to_string(), 1.0));
        assert_eq!(wavefront.percentiles[2], ("median".to_string(), 0.5));
    }


    #[test]
    fn config_file_influxdb() {
        let config = r#"
[influxdb]
port = 3131
host = "example.com"
bin_width = 9
"#
            .to_string();

        let args = parse_config_file(config, 4);

        assert!(args.influxdb.is_some());
        let influxdb = args.influxdb.unwrap();
        assert_eq!(influxdb.host, String::from("example.com"));
        assert_eq!(influxdb.port, 3131);
        assert_eq!(influxdb.bin_width, 9);
    }

    #[test]
    fn config_file_influxdb_sinks_style() {
        let config = r#"
[sinks]
  [sinks.influxdb]
  port = 3131
  host = "example.com"
  bin_width = 9
  flush_interval = 70
"#
            .to_string();

        let args = parse_config_file(config, 4);

        assert!(args.influxdb.is_some());
        let influxdb = args.influxdb.unwrap();
        assert_eq!(influxdb.host, String::from("example.com"));
        assert_eq!(influxdb.port, 3131);
        assert_eq!(influxdb.bin_width, 9);
        assert_eq!(influxdb.flush_interval, 70);
    }

    #[test]
    fn config_file_prometheus() {
        let config = r#"
[prometheus]
port = 3131
host = "example.com"
bin_width = 9
"#
            .to_string();

        let args = parse_config_file(config, 4);

        assert!(args.prometheus.is_some());
        let prometheus = args.prometheus.unwrap();
        assert_eq!(prometheus.host, String::from("example.com"));
        assert_eq!(prometheus.port, 3131);
        assert_eq!(prometheus.bin_width, 9);
    }

    #[test]
    fn config_file_prometheus_sinks_style() {
        let config = r#"
[sinks]
  [sinks.prometheus]
  port = 3131
  host = "example.com"
  bin_width = 9
  flush_interval = 10
"#
            .to_string();

        let args = parse_config_file(config, 4);

        assert!(args.prometheus.is_some());
        let prometheus = args.prometheus.unwrap();
        assert_eq!(prometheus.host, String::from("example.com"));
        assert_eq!(prometheus.port, 3131);
        assert_eq!(prometheus.bin_width, 9);
        assert_eq!(prometheus.flush_interval, 10);
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
    fn config_file_console_explicit_bin_width_sinks_style() {
        let config = r#"
[sinks]
  [sinks.console]
  bin_width = 9
"#
            .to_string();

        let args = parse_config_file(config, 4);

        assert!(args.console.is_some());
        let console = args.console.unwrap();
        assert_eq!(console.bin_width, 9);
        assert_eq!(console.flush_interval, 60); // default
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
    fn config_file_null_sinks_style() {
        let config = r#"
[sinks]
  [sinks.null]
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
    fn config_file_firehose_complicated_sinks_style() {
        let config = r#"
[sinks]
  [sinks.firehose.stream_one]
  delivery_stream = "stream_one"
  batch_size = 20
  flush_interval = 15

  [sinks.firehose.stream_two]
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
        assert_eq!(args.firehosen[0].flush_interval, 15);

        assert_eq!(args.firehosen[1].delivery_stream, "stream_two");
        assert_eq!(args.firehosen[1].batch_size, 800);
        assert_eq!(args.firehosen[1].region, Region::UsEast1);
        assert_eq!(args.firehosen[1].flush_interval, 60); // default
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
    fn config_file_file_source_single_sources_style() {
        let config = r#"
[sources]
  [sources.files]
  [sources.files.foo_bar_txt]
  path = "/foo/bar.txt"
  forwards = ["sink.blech"]
"#
            .to_string();

        let args = parse_config_file(config, 4);

        assert!(!args.files.is_empty());
        assert_eq!(args.files[0].path, PathBuf::from("/foo/bar.txt"));
        assert_eq!(args.files[0].forwards, vec!["sink.blech"]);
    }

    #[test]
    fn config_file_file_source_multiple_sources_style() {
        let config = r#"
[sources]
  [sources.files]
  [sources.files.foo_bar_txt]
  path = "/foo/bar.txt"
  forwards = ["sink.blech"]

  [sources.files.bar_txt]
  path = "/bar.txt"
  forwards = ["sink.bar.blech"]
"#
            .to_string();

        let args = parse_config_file(config, 4);

        assert!(!args.files.is_empty());
        assert_eq!(args.files[1].path, PathBuf::from("/foo/bar.txt"));
        assert_eq!(args.files[1].forwards, vec!["sink.blech"]);
        assert_eq!(args.files[0].path, PathBuf::from("/bar.txt"));
        assert_eq!(args.files[0].forwards, vec!["sink.bar.blech"]);
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

        assert_eq!(args.graphites.get("sources.graphite").unwrap().tags, tags);
    }

    #[test]
    fn config_file_full() {
        let config = r#"
statsd-port = 1024
graphite-port = 1034

global-flush-interval = 128

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

        assert!(!args.statsds.is_empty());
        let statsd_config = args.statsds.get("sources.statsd").unwrap();
        assert_eq!(statsd_config.port, 1024);
        assert_eq!(statsd_config.tags, tags);
        assert!(!args.graphites.is_empty());
        let graphite_config = args.graphites.get("sources.graphite").unwrap();
        assert_eq!(graphite_config.port, 1034);
        assert_eq!(graphite_config.tags, tags);
        assert_eq!(args.global_flush_interval, 128);
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
