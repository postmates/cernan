//! Provides the CLI option parser
//!
//! Used to parse the argv/config file into a struct that
//! the server can consume and use as configuration data.

use clap::{App, Arg};
use metric::TagMap;
use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use toml;

const VERSION: Option<&'static str> = option_env!("CARGO_PKG_VERSION");

use filter::{
    DelayFilterConfig, FlushBoundaryFilterConfig, JSONEncodeFilterConfig,
    ProgrammableFilterConfig,
};
use sink::wavefront::PadControl;
use sink::{
    ConsoleConfig, ElasticsearchConfig, InfluxDBConfig, KafkaConfig, NativeConfig,
    NullConfig, PrometheusConfig, WavefrontConfig,
};
use source::{
    flushes_per_second, FileServerConfig, GraphiteConfig, InternalConfig,
    NativeServerConfig, StatsdConfig, StatsdParseConfig, TCPConfig,
};

// This stinks and is verbose. Once
// https://github.com/rust-lang/rust/issues/41681 lands we'll be able to do this
// much more nicely.

fn default_data_directory() -> PathBuf {
    Path::new("/tmp/cernan-data").to_path_buf()
}

fn default_scripts_directory() -> PathBuf {
    Path::new("/tmp/cernan-scripts").to_path_buf()
}

fn default_version() -> String {
    VERSION.unwrap().to_string()
}

/// Big configuration struct for cernan executable
///
/// This struct is what we construct from parsing the cernan configuration. It
/// is not intended to be created by external clients. Please see documentation
/// on `parse_args` in this module for more details.
#[derive(Debug)]
pub struct Args {
    /// The maximum size -- in bytes -- that a hopper queue may hold in memory
    /// before flushing to disk.
    pub max_hopper_in_memory_bytes: usize,
    /// The maximum number of queue files that hopper may hold on disk. The
    /// maximum disk consumption of a single hopper queue will be
    /// `max_hopper_queue_files * max_hopper_queue_bytes`.
    pub max_hopper_queue_files: usize,
    /// The maximum size -- in bytes -- that a hopper queue may grow to before
    /// being cycled.
    pub max_hopper_queue_bytes: usize,
    /// The location on-disk where cernan will store its private files. This
    /// directory MUST be solely owned by cernan.
    pub data_directory: PathBuf,
    /// The location on-disk where cernan will search for programmable filter
    /// scripts.
    pub scripts_directory: PathBuf,
    /// The global flush interval. This value is inherited by all sinks which do
    /// not specify their own.
    pub flush_interval: u64,
    /// Cernan version string. This is set automatically.
    pub version: String,
    /// The programmable filters to use in this cernan run. See
    /// `filters::ProgrammableFilter` for more.
    pub programmable_filters: Option<HashMap<String, ProgrammableFilterConfig>>,
    /// The delay filters to use in this cernan run. See `filters::DelayFilter`
    /// for more.
    pub delay_filters: Option<HashMap<String, DelayFilterConfig>>,
    /// The json_encode filters to use in this cernan run. See
    /// `filters::JSONEncodeFilter` for more.
    pub json_encode_filters: Option<HashMap<String, JSONEncodeFilterConfig>>,
    /// The flush boundaryfilters to use in this cernan run. See
    /// `filters::FlushBoundaryFilter` for more.
    pub flush_boundary_filters: Option<HashMap<String, FlushBoundaryFilterConfig>>,
    /// See `sinks::Console` for more.
    pub console: Option<ConsoleConfig>,
    /// See `sinks::Null` for more.
    pub null: Option<NullConfig>,
    /// See `sinks::Wavefront` for more.
    pub wavefront: Option<WavefrontConfig>,
    /// See `sinks::InfluxDB` for more.
    pub influxdb: Option<InfluxDBConfig>,
    /// See `sinks::Native` for more.
    pub native_sink_config: Option<NativeConfig>,
    /// See `sinks::Prometheus` for more.
    pub prometheus: Option<PrometheusConfig>,
    /// See `sinks::Elasticsearch` for more.
    pub elasticsearch: Option<ElasticsearchConfig>,
    /// See `sinks::Kafka` for more.
    pub kafkas: Option<Vec<KafkaConfig>>,
    /// See `sources::FileServer` for more.
    pub files: Option<Vec<FileServerConfig>>,
    /// See `sources::Internal` for more.
    pub internal: InternalConfig,
    /// See `sources::Graphite` for more.
    pub graphites: Option<HashMap<String, GraphiteConfig>>,
    /// See `sources::Avro` for more.
    pub avros: Option<HashMap<String, TCPConfig>>,
    /// See `sources::Native` for more.
    pub native_server_config: Option<HashMap<String, NativeServerConfig>>,
    /// See `sources::Statsd` for more.
    pub statsds: Option<HashMap<String, StatsdConfig>>,
}

impl Default for Args {
    fn default() -> Self {
        Args {
            max_hopper_in_memory_bytes: 0x10_0000,
            max_hopper_queue_bytes: 0x10_0000 * 100,
            max_hopper_queue_files: 100,
            data_directory: default_data_directory(),
            scripts_directory: default_scripts_directory(),
            flush_interval: 60 * flushes_per_second(),
            version: default_version(),
            // filters
            programmable_filters: None,
            delay_filters: None,
            json_encode_filters: None,
            flush_boundary_filters: None,
            // sinks
            console: None,
            null: None,
            wavefront: None,
            influxdb: None,
            prometheus: None,
            native_sink_config: None,
            elasticsearch: None,
            kafkas: None,
            // sources
            statsds: None,
            graphites: None,
            avros: None,
            native_server_config: None,
            files: None,
            internal: InternalConfig::default(),
        }
    }
}

/// Common utility function for parsing flush_interval and
/// returning the number of flushes per second represented.
fn parse_flush_interval(table: &toml::Value, key: &str) -> Option<u64> {
    match table.get(key) {
        Some(value) => match value {
            toml::Value::Float(f) => Some((f * flushes_per_second() as f64) as u64),
            toml::Value::Integer(i) => Some(*i as u64 * flushes_per_second()),
            _ => panic!("Expected number of seconds for flush_interval config parameter.")
        },
        _ => None
    }
}

/// Parse the cernan configuration arguments
///
/// This function will read the environment arguments and return a minimal
/// amount of information. This will be paired with a call to `parse_config`.
pub fn parse_args() -> (u64, String) {
    let args = App::new("cernan")
        .version(VERSION.unwrap_or("unknown"))
        .author("Brian L. Troutwine <blt@postmates.com>")
        .about("telemetry aggregation and shipping, last up the ladder")
        .arg(
            Arg::with_name("config-file")
                .long("config")
                .short("C")
                .value_name("config")
                .required(true)
                .help("The config file to feed in.")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("verbose")
                .short("v")
                .multiple(true)
                .help("Turn on verbose output."),
        )
        .get_matches();

    let verb = if args.is_present("verbose") {
        args.occurrences_of("verbose")
    } else {
        0
    };
    (verb, args.value_of("config-file").unwrap().to_string())
}

/// Parse the cernan configuration file.
///
/// Please see the cernan wiki for details on the configuration file
/// format. Examples are also available in this repository under
/// `examples/configs`.
pub fn parse_config(filename: &str) -> Args {
    let mut fp = match File::open(filename) {
        Err(e) => panic!("Could not open file {} with error {}", filename, e),
        Ok(fp) => fp,
    };

    let mut buffer = String::new();
    fp.read_to_string(&mut buffer).unwrap();
    parse_config_file(&buffer)
}

fn parse_config_file(buffer: &str) -> Args {
    let mut args = Args::default();
    let value: toml::Value =
        toml::from_str(buffer).expect("could not parse config file");

    args.max_hopper_queue_bytes = value
        .get("max-hopper-queue-bytes")
        .map(|s| {
            s.as_integer()
                .expect("could not parse max-hopper-queue-bytes") as usize
        })
        .unwrap_or(args.max_hopper_queue_bytes);

    args.max_hopper_queue_files = value
        .get("max-hopper-queue-files")
        .map(|s| {
            s.as_integer()
                .expect("could not parse max-hopper-queue-files") as usize
        })
        .unwrap_or(args.max_hopper_queue_files);

    args.max_hopper_in_memory_bytes = value
        .get("max-hopper-in-memory-bytes")
        .map(|s| {
            s.as_integer()
                .expect("could not parse max-hopper-in-memory-bytes")
                as usize
        })
        .unwrap_or(args.max_hopper_in_memory_bytes);

    args.data_directory = value
        .get("data-directory")
        .map(|s| {
            let s = s.as_str()
                .expect("data-directory value must be valid string");
            Path::new(s).to_path_buf()
        })
        .unwrap_or(args.data_directory);

    args.scripts_directory = value
        .get("scripts-directory")
        .map(|s| {
            let s = s.as_str()
                .expect("scripts-directory value must be valid string");
            Path::new(s).to_path_buf()
        })
        .unwrap_or(args.scripts_directory);

    args.flush_interval = parse_flush_interval(&value, "flush-interval")
        .unwrap_or(args.flush_interval);

    let global_tags: TagMap = match value.get("tags") {
        Some(tbl) => {
            let mut tags = TagMap::default();
            let ttbl = tbl.as_table().unwrap();
            for (k, v) in ttbl.iter() {
                let val = match v.as_str() {
                    Some(s) => s.to_string(),
                    None => {
                        let ktbl =
                            v.as_table().expect("tag must be a string or a table");
                        if ktbl.get("environment")
                            .map_or(false, |ev| ev.as_bool().unwrap_or(false))
                        {
                            let env_key = ktbl.get("value")
                                .expect("must have a value key")
                                .as_str()
                                .expect("value key must be string");
                            env::var_os(env_key)
                                .expect("value could not be read from the environment")
                                .into_string()
                                .expect(
                                    "value read from environment is not a rust string",
                                )
                        } else {
                            use std::process::exit;
                            println!(
                                "environment variable table must have environment / value keys"
                            );
                            exit(1);
                        }
                    }
                };
                tags.insert(k.clone(), val);
            }
            tags
        }
        None => TagMap::default(),
    };

    // filters
    //
    if let Some(filters) = value.get("filters") {
        args.delay_filters = filters.get("delay").map(|fltr| {
            let mut filters: HashMap<String, DelayFilterConfig> = HashMap::new();
            for (name, tbl) in fltr.as_table().unwrap().iter() {
                match tbl.get("tolerance") {
                    Some(tol) => {
                        let tolerance =
                            tol.as_integer().expect("tolerance must be an integer");
                        let fwds = match tbl.get("forwards") {
                            Some(fwds) => fwds.as_array()
                                .expect("forwards must be an array")
                                .to_vec()
                                .iter()
                                .map(|s| s.as_str().unwrap().to_string())
                                .collect(),
                            None => Vec::new(),
                        };
                        let config_path = format!("filters.delay.{}", name);
                        let config = DelayFilterConfig {
                            tolerance: tolerance,
                            forwards: fwds,
                            config_path: Some(config_path.clone()),
                        };
                        filters.insert(config_path, config);
                    }
                    None => continue,
                }
            }
            filters
        });

        args.json_encode_filters = filters.get("json_encode").map(|fltr| {
            let mut filters: HashMap<String, JSONEncodeFilterConfig> = HashMap::new();
            for (name, tbl) in fltr.as_table().unwrap().iter() {
                let parse_line = if let Some(parse_line) = tbl.get("parse_line") {
                    parse_line
                        .as_bool()
                        .expect("could not parse parse_line as boolean")
                } else {
                    false
                };
                let fwds = match tbl.get("forwards") {
                    Some(fwds) => fwds.as_array()
                        .expect("forwards must be an array")
                        .to_vec()
                        .iter()
                        .map(|s| s.as_str().unwrap().to_string())
                        .collect(),
                    None => Vec::new(),
                };
                let config_path = format!("filters.json_encode.{}", name);
                let config = JSONEncodeFilterConfig {
                    parse_line: parse_line,
                    forwards: fwds,
                    config_path: Some(config_path.clone()),
                    tags: global_tags.clone(),
                };
                filters.insert(config_path, config);
            }
            filters
        });

        args.flush_boundary_filters = filters.get("flush_boundary").map(|fltr| {
            let mut filters: HashMap<String, FlushBoundaryFilterConfig> =
                HashMap::new();
            for (name, tbl) in fltr.as_table().unwrap().iter() {
                match tbl.get("tolerance") {
                    Some(tol) => {
                        let tolerance =
                            tol.as_integer().expect("tolerance must be an integer");
                        let fwds = match tbl.get("forwards") {
                            Some(fwds) => fwds.as_array()
                                .expect("forwards must be an array")
                                .to_vec()
                                .iter()
                                .map(|s| s.as_str().unwrap().to_string())
                                .collect(),
                            None => Vec::new(),
                        };
                        let config_path = format!("filters.flush_boundary.{}", name);
                        let config = FlushBoundaryFilterConfig {
                            tolerance: tolerance as usize,
                            forwards: fwds,
                            config_path: Some(config_path.clone()),
                        };
                        filters.insert(config_path, config);
                    }
                    None => continue,
                }
            }
            filters
        });

        args.programmable_filters = filters.get("programmable").map(|fltr| {
            let mut filters: HashMap<String, ProgrammableFilterConfig> =
                HashMap::new();
            for (name, tbl) in fltr.as_table().unwrap().iter() {
                match tbl.get("script") {
                    Some(pth) => {
                        let path = Path::new(pth.as_str().unwrap());
                        let fwds = match tbl.get("forwards") {
                            Some(fwds) => fwds.as_array()
                                .expect("forwards must be an array")
                                .to_vec()
                                .iter()
                                .map(|s| s.as_str().unwrap().to_string())
                                .collect(),
                            None => Vec::new(),
                        };
                        let config_path = format!("filters.programmable.{}", name);
                        let config = ProgrammableFilterConfig {
                            scripts_directory: Some(args.scripts_directory.clone()),
                            script: Some(args.scripts_directory.join(path)),
                            forwards: fwds,
                            config_path: Some(config_path.clone()),
                            tags: global_tags.clone(),
                        };
                        filters.insert(config_path, config);
                    }
                    None => continue,
                }
            }
            filters
        })
    }

    // sinks
    //
    if let Some(sinks) = value.get("sinks") {
        let sinks = sinks.as_table().expect("sinks must be in table format");

        args.null = sinks.get("null").map(|_| NullConfig {
            config_path: "sinks.null".to_string(),
        });

        args.console = sinks.get("console").map(|snk| {
            let mut res = ConsoleConfig::default();
            res.config_path = Some("sinks.console".to_string());

            res.bin_width = snk.get("bin_width")
                .map(|bw| {
                    bw.as_integer()
                        .expect("could not parse sinks.console.bin_width")
                })
                .unwrap_or(res.bin_width);

            res.flush_interval = parse_flush_interval(snk, "flush_interval").unwrap_or(args.flush_interval);
            res.tags = global_tags.clone();

            res
        });

        args.wavefront = sinks.get("wavefront").map(|snk| {
            let mut res = WavefrontConfig::default();
            res.config_path = Some("sinks.wavefront".to_string());

            res.pad_control = snk.get("padding")
                .and_then(|t| t.as_table())
                .map(|tbl| PadControl {
                    set: tbl.get("set").map_or(false, |v| {
                        v.as_bool().expect("could not parse padding.set as boolean")
                    }),
                    sum: tbl.get("sum").map_or(false, |v| {
                        v.as_bool().expect("could not parse padding.sum as boolean")
                    }),
                    summarize: tbl.get("summarize").map_or(false, |v| {
                        v.as_bool()
                            .expect("could not parse padding.summarize as boolean")
                    }),
                    histogram: tbl.get("histogram").map_or(false, |v| {
                        v.as_bool()
                            .expect("could not parse padding.histogram as boolean")
                    }),
                })
                .unwrap_or(res.pad_control);

            res.percentiles = snk.get("percentiles")
                .and_then(|t| t.as_table())
                .map(|tbl| {
                    let mut prcnt = Vec::default();
                    for (k, v) in tbl.iter() {
                        let v: f64 =
                            v.as_float().expect("percentile value must be a float");
                        prcnt.push((k.clone(), v));
                    }
                    prcnt
                })
                .unwrap_or(res.percentiles);

            res.port = snk.get("port")
                .map(|p| {
                    p.as_integer()
                        .expect("could not parse sinks.wavefront.port")
                        as u16
                })
                .unwrap_or(res.port);

            res.age_threshold = snk.get("age_threshold")
                .map(|p| {
                    Some(p.as_integer()
                        .expect("could not parse sinks.wavefront.age_threshold")
                        as u64)
                })
                .unwrap_or(res.age_threshold);

            res.host = snk.get("host")
                .map(|p| {
                    p.as_str()
                        .expect("could not parse sinks.wavefront.host")
                        .to_string()
                })
                .unwrap_or(res.host);

            res.bin_width = snk.get("bin_width")
                .map(|bw| {
                    bw.as_integer()
                        .expect("could not parse sinks.wavefront.bin_width")
                })
                .unwrap_or(res.bin_width);

            res.flush_interval = parse_flush_interval(snk, "flush_interval").unwrap_or(args.flush_interval);
            if res.bin_width > (res.flush_interval as i64) {
                warn!("bin_width > flush_interval. bin_width will be effectively flush_interval due to flush behaviour.")
            }

            res.tags = global_tags.clone();

            res
        });

        args.influxdb = sinks.get("influxdb").map(|snk| {
            let mut res = InfluxDBConfig::default();
            res.config_path = Some("sinks.influxdb".to_string());

            res.port = snk.get("port")
                .map(|p| {
                    p.as_integer().expect("could not parse sinks.influxdb.port") as u16
                })
                .unwrap_or(res.port);

            res.secure = snk.get("secure")
                .map(|p| p.as_bool().expect("could not parse sinks.influxdb.secure"))
                .unwrap_or(res.secure);

            res.host = snk.get("host")
                .map(|p| {
                    p.as_str()
                        .expect("could not parse sinks.influxdb.host")
                        .to_string()
                })
                .unwrap_or(res.host);

            res.db = snk.get("db")
                .map(|p| {
                    p.as_str()
                        .expect("could not parse sinks.influxdb.db")
                        .to_string()
                })
                .unwrap_or(res.db);

            res.flush_interval = parse_flush_interval(snk, "flush_interval").unwrap_or(args.flush_interval);
            res.tags = global_tags.clone();

            res
        });

        args.prometheus = sinks.get("prometheus").map(|snk| {
            let mut res = PrometheusConfig::default();
            res.config_path = Some("sinks.prometheus".to_string());

            res.age_threshold = snk.get("age_threshold")
                .map(|p| {
                    Some(p.as_integer()
                        .expect("could not parse sinks.prometheus.age_threshold")
                        as u64)
                })
                .unwrap_or(res.age_threshold);

            res.port = snk.get("port")
                .map(|p| {
                    p.as_integer()
                        .expect("could not parse sinks.prometheus.port")
                        as u16
                })
                .unwrap_or(res.port);

            res.host = snk.get("host")
                .map(|p| {
                    p.as_str()
                        .expect("could not parse sinks.prometheus.host")
                        .to_string()
                })
                .unwrap_or(res.host);

            res.capacity_in_seconds = snk.get("capacity_in_seconds")
                .map(|p| {
                    p.as_integer()
                        .expect("could not parse sinks.prometheus.capacity_in_seconds")
                        as usize
                })
                .unwrap_or(res.capacity_in_seconds);

            res.tags = global_tags.clone();

            res
        });

        args.elasticsearch = sinks.get("elasticsearch").map(|snk| {
            let mut res = ElasticsearchConfig::default();
            res.config_path = Some("sinks.elasticsearch".to_string());

            res.delivery_attempt_limit = snk.get("delivery_attempt_limit")
                .map(|p| {
                    p.as_integer().expect(
                        "could not parse sinks.elasticsearch.delivery_attempt_limit",
                    ) as u8
                })
                .unwrap_or(res.delivery_attempt_limit);

            res.port = snk.get("port")
                .map(|p| {
                    p.as_integer()
                        .expect("could not parse sinks.elasticsearch.port")
                        as usize
                })
                .unwrap_or(res.port);

            res.host = snk.get("host")
                .map(|p| {
                    p.as_str()
                        .expect("could not parse sinks.elasticsearch.host")
                        .to_string()
                })
                .unwrap_or(res.host);

            res.index_prefix = snk.get("index-prefix")
                .map(|p| {
                    Some(
                        p.as_str()
                            .expect("could not parse sinks.elasticsearch.index-prefix")
                            .to_string(),
                    )
                })
                .unwrap_or(res.index_prefix);

            res.secure = snk.get("secure")
                .map(|bw| {
                    bw.as_bool()
                        .expect("could not parse sinks.elasticsearch.secure")
                })
                .unwrap_or(res.secure);

            res.index_type = snk.get("index_type")
                .map(|bw| {
                    bw.as_str()
                        .expect("could not parse sinks.elasticsearch.index_type")
                        .to_string()
                })
                .unwrap_or(res.index_type);

            res.flush_interval = parse_flush_interval(snk, "flush_interval").unwrap_or(args.flush_interval);
            res.tags = global_tags.clone();

            res
        });

        args.native_sink_config = sinks.get("native").map(|snk| {
            let mut res = NativeConfig::default();
            res.config_path = Some("sinks.native".to_string());

            res.port = snk.get("port")
                .map(|p| {
                    p.as_integer().expect("could not parse sinks.native.port") as u16
                })
                .unwrap_or(res.port);

            res.host = snk.get("host")
                .map(|p| {
                    p.as_str()
                        .expect("could not parse sinks.native.host")
                        .to_string()
                })
                .unwrap_or(res.host);

            res.flush_interval = parse_flush_interval(snk, "flush_interval").unwrap_or(args.flush_interval);
            res.tags = global_tags.clone();

            res
        });

        args.kafkas = sinks.get("kafka").map(|snk| {
            let mut kafkas = Vec::new();
            for (name, tbl) in snk.as_table().unwrap().iter() {
                let is_enabled = tbl.get("enabled")
                    .unwrap_or(&toml::Value::Boolean(true))
                    .as_bool()
                    .expect("must be a bool");
                if !is_enabled {
                    continue;
                }

                let mut res = KafkaConfig::default();
                let config_path = &format!("sinks.kafka.{}", name)[..];
                res.config_path = Some(config_path.to_string());

                let topic = tbl.get("topic")
                    .map(|x| x.as_str().expect("topic must be a string").to_string());
                if topic.is_none() {
                    warn!(
                        "kafka sink {} skipped as it does not provide a topic!",
                        config_path
                    );
                    continue;
                }
                res.topic_name = topic;

                let brokers = tbl.get("brokers").map(|x| {
                    x.as_str()
                        .expect("brokers must be a comma-separated list of host or host:port")
                        .to_string()
                });
                if brokers.is_none() {
                    warn!(
                        "kafka sink {} skipped as it does not provide brokers",
                        config_path
                    );
                    continue;
                }
                res.brokers = brokers;

                // Allow configuration of librdkafka producer with a sub-table.
                // Sooo many options:
                // https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
                res.rdkafka_config = tbl.get("librdkafka").map(|x| {
                    let tbl = x.as_table()
                        .expect("librdkafka configuration should be a table");
                    let mut map = HashMap::new();
                    for (key, value) in tbl.iter() {
                        match *value {
                            toml::Value::Integer(i) => {
                                map.insert(key.clone(), format!("{}", i))
                            }
                            toml::Value::String(ref s) => {
                                map.insert(key.clone(), s.clone())
                            }
                            toml::Value::Float(f) => {
                                map.insert(key.clone(), format!("{}", f))
                            }
                            toml::Value::Boolean(b) => {
                                map.insert(key.clone(), format!("{}", b))
                            }
                            _ => {
                                warn!(
                                    "ignoring {:?} in {}.librdkafka: unusable type {}",
                                    key,
                                    config_path,
                                    value.type_str()
                                );
                                continue;
                            }
                        };
                    }
                    map
                });

                res.flush_interval = parse_flush_interval(tbl, "flush_interval").unwrap_or(args.flush_interval);
                res.max_message_bytes = tbl.get("max_message_bytes")
                    .map(|fi| {
                        fi.as_integer()
                            .expect("could not parse sinks.kafka.max_message_bytes")
                            as usize
                    })
                    .unwrap_or(res.max_message_bytes);

                kafkas.push(res)
            }
            kafkas
        });
    }

    // sources
    //
    if let Some(sources) = value.get("sources") {
        let sources = sources.as_table().expect("sources must be in table format");

        args.files = sources.get("files").map(|src| {
            let mut files = Vec::new();
            for tbl in src.as_table().unwrap().values() {
                match tbl.get("path") {
                    Some(pth) => {
                        let mut fl = FileServerConfig::default();
                        fl.path = Some(Path::new(pth.as_str().unwrap()).to_path_buf());
                        fl.config_path = Some(format!("sources.files.{}", pth));

                        fl.forwards = tbl.get("forwards")
                            .map(|fwd| {
                                fwd.as_array()
                                    .expect("forwards must be an array")
                                    .to_vec()
                                    .iter()
                                    .map(|s| s.as_str().unwrap().to_string())
                                    .collect()
                            })
                            .unwrap_or(fl.forwards);

                        // NOTE The table lookup will return an i64 but we
                        // convert to usize. Strictly speaking this IS NOT a
                        // safe conversion but it makes no good sense for a user
                        // to be reading anywhere above 2**60 lines per file. We
                        // leave these people to their own wild works and hope
                        // for the best.
                        //
                        // Someday a static analysis system will flag this as
                        // unsafe. Welcome.
                        fl.max_read_bytes = tbl.get("max_read_bytes")
                            .map(|mrl| {
                                mrl.as_integer()
                                    .expect("could not parse max_read_bytes")
                                    as usize
                            })
                            .unwrap_or(fl.max_read_bytes);

                        files.push(fl)
                    }
                    None => continue,
                }
            }
            files
        });

        args.statsds = sources.get("statsd").map(|src| {
            let mut statsds = HashMap::default();
            for (name, tbl) in src.as_table().unwrap().iter() {
                let is_enabled = tbl.get("enabled")
                    .unwrap_or(&toml::Value::Boolean(true))
                    .as_bool()
                    .expect("must be a bool");
                if is_enabled {
                    let mut res = StatsdConfig::default();
                    res.config_path = Some(name.clone());

                    res.port = tbl.get("port")
                        .map(|p| {
                            p.as_integer().expect("could not parse statsd port") as u16
                        })
                        .unwrap_or(res.port);

                    res.host = tbl.get("host")
                        .map(|p| {
                            p.as_str()
                                .expect("could not parse statsd host")
                                .to_string()
                        })
                        .unwrap_or(res.host);

                    res.forwards = tbl.get("forwards")
                        .map(|fwd| {
                            fwd.as_array()
                                .expect("forwards must be an array")
                                .to_vec()
                                .iter()
                                .map(|s| s.as_str().unwrap().to_string())
                                .collect()
                        })
                        .unwrap_or(res.forwards);

                    res.parse_config = tbl.get("mapping")
                        .map(|cfg| {
                            let mut masks = Vec::new();
                            for (_, tbl) in
                                cfg.as_table().expect("mapping must be a table").iter()
                            {
                                if let Some(mask) = tbl.get("mask") {
                                    let re = ::regex::Regex::new(
                                        mask.as_str().expect("mask must be a string"),
                                    ).expect("mask is not a valid regex");
                                    if let Some(bnds) = tbl.get("bounds") {
                                        let bounds = bnds.as_array()
                                            .expect("bounds must be an array")
                                            .to_vec()
                                            .iter()
                                            .map(|v| v.as_float().unwrap())
                                            .collect();
                                        masks.insert(0, (re, bounds));
                                    } else {
                                        panic!("mapping must have bounds");
                                    }
                                } else {
                                    panic!("mapping must have a mask");
                                }
                            }
                            let mut parse_config = StatsdParseConfig::default();
                            parse_config.histogram_masks = masks;

                            parse_config
                        })
                        .unwrap_or(res.parse_config);

                    let error_bound = tbl.get("summarize_error_bound")
                        .map(|p| {
                            p.as_float()
                                .expect("summarize_error_bound must be a flaot")
                        })
                        .unwrap_or(0.01);

                    res.parse_config.summarize_error_bound = error_bound;

                    assert!(res.config_path.is_some());
                    assert!(!res.forwards.is_empty());

                    statsds.insert(format!("sources.statsd.{}", name), res);
                }
            }
            statsds
        });

        args.graphites = sources.get("graphite").map(|src| {
            let mut graphites = HashMap::default();
            for (name, tbl) in src.as_table().unwrap().iter() {
                let is_enabled = tbl.get("enabled")
                    .unwrap_or(&toml::Value::Boolean(true))
                    .as_bool()
                    .expect("must be a bool");
                if is_enabled {
                    let mut res = GraphiteConfig::default();
                    res.config_path = Some(name.clone());

                    res.port = tbl.get("port")
                        .map(|p| {
                            p.as_integer().expect("could not parse graphite port")
                                as u16
                        })
                        .unwrap_or(res.port);

                    res.host = tbl.get("host")
                        .map(|p| {
                            p.as_str()
                                .expect("could not parse graphite host")
                                .to_string()
                        })
                        .unwrap_or(res.host);

                    res.forwards = tbl.get("forwards")
                        .map(|fwd| {
                            fwd.as_array()
                                .expect("forwards must be an array")
                                .to_vec()
                                .iter()
                                .map(|s| s.as_str().unwrap().to_string())
                                .collect()
                        })
                        .unwrap_or(res.forwards);

                    assert!(res.config_path.is_some());
                    assert!(!res.forwards.is_empty());

                    graphites.insert(format!("sources.graphite.{}", name), res);
                }
            }
            graphites
        });

        args.avros = sources.get("avro").map(|src| {
            let mut avros = HashMap::default();
            for (name, tbl) in src.as_table().unwrap().iter() {
                let is_enabled = tbl.get("enabled")
                    .unwrap_or(&toml::Value::Boolean(true))
                    .as_bool()
                    .expect("must be a bool");
                if is_enabled {
                    let mut res = TCPConfig::default();
                    res.config_path = Some(name.clone());

                    // If the user doesn't provide a port, we assume 2002.
                    // This default value has been selected to commemorate the year
                    // Buzz Aldrin punched Bart Sibrel in the face.
                    res.port = tbl.get("port")
                        .map(|p| {
                            p.as_integer().expect("could not parse avro port") as u16
                        })
                        .unwrap_or(2002);

                    res.host = tbl.get("host")
                        .map(|p| {
                            p.as_str().expect("could not parse avro host").to_string()
                        })
                        .unwrap_or(res.host);

                    res.forwards = tbl.get("forwards")
                        .map(|fwd| {
                            fwd.as_array()
                                .expect("forwards must be an array")
                                .to_vec()
                                .iter()
                                .map(|s| s.as_str().unwrap().to_string())
                                .collect()
                        })
                        .unwrap_or(res.forwards);

                    assert!(res.config_path.is_some());
                    assert!(!res.forwards.is_empty());

                    avros.insert(format!("sources.avro.{}", name), res);
                }
            }
            avros
        });

        args.native_server_config = sources.get("native").map(|src| {
            let mut native_server_config = HashMap::default();
            for (name, tbl) in src.as_table().unwrap().iter() {
                let is_enabled = tbl.get("enabled")
                    .unwrap_or(&toml::Value::Boolean(true))
                    .as_bool()
                    .expect("must be a bool");
                if is_enabled {
                    let mut res = NativeServerConfig::default();
                    res.config_path = Some(format!("sources.native.{}", name));

                    res.port = tbl.get("port")
                        .map(|p| {
                            p.as_integer().expect("could not parse native port") as u16
                        })
                        .unwrap_or(res.port);

                    res.ip = tbl.get("ip")
                        .map(|p| {
                            p.as_str().expect("could not parse native ip").to_string()
                        })
                        .unwrap_or(res.ip);

                    res.forwards = tbl.get("forwards")
                        .map(|fwd| {
                            fwd.as_array()
                                .expect("forwards must be an array")
                                .to_vec()
                                .iter()
                                .map(|s| s.as_str().unwrap().to_string())
                                .collect()
                        })
                        .unwrap_or(res.forwards);

                    assert!(res.config_path.is_some());

                    native_server_config
                        .insert(format!("sources.native.{}", name), res);
                }
            }
            native_server_config
        });

        args.internal = sources
            .get("internal")
            .map(|src| {
                let mut res = InternalConfig::default();
                res.config_path = Some("sources.internal".to_string());

                res.forwards = src.get("forwards")
                    .map(|fwd| {
                        fwd.as_array()
                            .expect("forwards must be an array")
                            .to_vec()
                            .iter()
                            .map(|s| s.as_str().unwrap().to_string())
                            .collect()
                    })
                    .unwrap_or(res.forwards);

                res
            })
            .unwrap_or(args.internal);
    }

    args
}

#[cfg(test)]
mod test {
    use super::*;
    // use filter::ProgrammableFilterConfig;
    // use metric::TagMap;
    // use rusoto::Region;
    // use std::path::{Path, PathBuf};

    #[test]
    fn config_file_data_directory() {
        let config = r#"
data-directory = "/foo/bar"
"#;
        let args = parse_config_file(config);
        let dir = Path::new("/foo/bar").to_path_buf();

        assert_eq!(args.data_directory, dir);
    }

    #[test]
    fn config_max_hopper_queue_bytes() {
        let config = r#"
max-hopper-queue-bytes = 10
max-hopper-queue-files = 1024
max-hopper-in-memory-bytes = 4048
"#;
        let args = parse_config_file(config);
        assert_eq!(args.max_hopper_queue_bytes, 10);
        assert_eq!(args.max_hopper_queue_files, 1024);
        assert_eq!(args.max_hopper_in_memory_bytes, 4048);
    }

    #[test]
    fn config_file_data_directory_default() {
        let config = r#""#;
        let args = parse_config_file(config);
        let dir = Path::new("/tmp/cernan-data").to_path_buf();

        assert_eq!(args.data_directory, dir);
    }

    #[test]
    fn config_file_scripts_directory() {
        let config = r#"
scripts-directory = "/foo/bar"
"#;
        let args = parse_config_file(config);
        let dir = Path::new("/foo/bar").to_path_buf();

        assert_eq!(args.scripts_directory, dir);
    }

    #[test]
    fn config_file_scripts_directory_default() {
        let config = r#""#;
        let args = parse_config_file(config);
        let dir = Path::new("/tmp/cernan-scripts").to_path_buf();

        assert_eq!(args.scripts_directory, dir);
    }

    #[test]
    fn config_fed_receiver_sources_style() {
        let config = r#"
[sources]
  [sources.native.lower]
  ip = "127.0.0.1"
  port = 1972

  [sources.native.upper]
  enabled = false
  ip = "127.0.0.1"
  port = 1973
"#;

        let args = parse_config_file(config);

        assert!(args.native_server_config.is_some());
        let nsc = args.native_server_config.unwrap();

        let nsc_lower = nsc.get("sources.native.lower").unwrap();
        assert_eq!(nsc_lower.port, 1972);
        assert_eq!(nsc_lower.ip, String::from("127.0.0.1"));
        assert!(nsc_lower.forwards.is_empty());

        assert!(nsc.get("sources.native.upperr").is_none());
    }

    #[test]
    fn config_internal_source() {
        let config = r#"
[sources]
  [sources.internal]
  forwards = ["sinks.console", "sinks.null"]
"#;

        let args = parse_config_file(config);

        assert_eq!(
            args.internal.config_path,
            Some("sources.internal".to_string())
        );
        assert_eq!(
            args.internal.forwards,
            vec!["sinks.console".to_string(), "sinks.null".to_string()]
        );
    }

    #[test]
    fn config_elasticsearch_sink() {
        let config = r#"
[sinks]
  [sinks.elasticsearch]
  port = 1234
  host = "example.com"
  index-prefix = "prefix-"
  secure = true
  flush_interval = 2020
  delivery_attempt_limit = 33
"#;

        let args = parse_config_file(config);

        assert!(args.elasticsearch.is_some());
        let es = args.elasticsearch.unwrap();

        assert_eq!(es.port, 1234);
        assert_eq!(es.host, "example.com");
        assert_eq!(es.index_prefix, Some("prefix-".into()));
        assert_eq!(es.secure, true);
        assert_eq!(es.flush_interval, 2020 * flushes_per_second());
        assert_eq!(es.delivery_attempt_limit, 33);
    }

    #[test]
    fn config_kafka_sink() {
        let config = r#"
[sinks]
  [sinks.kafka.one]
  topic = "foobar"
  flush_interval = 100
  brokers = "127.0.0.1:9092"
  max_message_bytes = 65
    [sinks.kafka.one.librdkafka]
    "setting.one" = 1
    "setting.two" = 2
    "setting.three" = "three"
    "setting.four.five" = 4.5
    "setting.six" = true
    "setting.seven" = false
"#;
        let args = parse_config_file(config);

        assert!(args.kafkas.is_some());
        let kafkas = args.kafkas.unwrap();
        assert_eq!(kafkas.len(), 1);
        let expected_librdkafka_config: HashMap<String, String> = vec![
            (String::from("setting.one"), String::from("1")),
            (String::from("setting.two"), String::from("2")),
            (String::from("setting.three"), String::from("three")),
            (String::from("setting.four.five"), String::from("4.5")),
            (String::from("setting.six"), String::from("true")),
            (String::from("setting.seven"), String::from("false")),
        ].iter()
            .cloned()
            .collect();

        let k = &kafkas[0];
        assert_eq!(k.topic_name, Some(String::from("foobar")));
        assert_eq!(k.brokers, Some(String::from("127.0.0.1:9092")));
        assert_eq!(k.max_message_bytes, 65);
        assert_eq!(k.flush_interval, 100 * flushes_per_second());
        assert_eq!(k.rdkafka_config, Some(expected_librdkafka_config));
    }

    #[test]
    fn config_kafka_sink_skipped_if_no_topic() {
        let config = r#"
[sinks]
  [sinks.kafka.one]
  flush_interval = 100
  brokers = "127.0.0.1:9092"
  max_message_bytes = 65
"#;
        let args = parse_config_file(config);
        let kafkas = args.kafkas.unwrap();
        assert_eq!(kafkas.len(), 0);
    }

    #[test]
    fn config_kafka_sink_skipped_if_no_brokers() {
        let config = r#"
[sinks]
  [sinks.kafka.one]
  flush_interval = 100
  topic = "foobar"
  max_message_bytes = 65
"#;
        let args = parse_config_file(config);
        let kafkas = args.kafkas.unwrap();
        assert_eq!(kafkas.len(), 0);
    }

    #[test]
    fn config_kafka_sink_skipped_if_not_enabled() {
        let config = r#"
[sinks]
  [sinks.kafka.one]
  topic = "foobar"
  brokers = "broker1,broker2"
  enabled = false
"#;
        let args = parse_config_file(config);
        let kafkas = args.kafkas.unwrap();
        assert_eq!(kafkas.len(), 0);
    }

    #[test]
    fn config_kafka_sink_defaults() {
        let config = r#"
[sinks]
  [sinks.kafka.one]
  topic = "foobar"
  brokers = "broker,broker"
"#;
        let args = parse_config_file(config);
        let kafkas = args.kafkas.unwrap();
        assert_eq!(kafkas.len(), 1);
        let k = &kafkas[0];
        let defaults = KafkaConfig::default();

        assert_eq!(k.flush_interval, args.flush_interval);
        assert_eq!(k.max_message_bytes, defaults.max_message_bytes);
    }

    #[test]
    fn config_native_sink_config_distinct_host_sinks_style() {
        let config = r#"
    [sinks]
      [sinks.native]
      host = "foo.example.com"
      port = 1972
      flush_interval = 120
    "#;

        let args = parse_config_file(config);

        assert!(args.native_sink_config.is_some());
        let native_sink_config = args.native_sink_config.unwrap();
        assert_eq!(native_sink_config.host, String::from("foo.example.com"));
        assert_eq!(native_sink_config.port, 1972);
        assert_eq!(native_sink_config.flush_interval, 120 * flushes_per_second());
    }

    #[test]
    fn config_statsd_sources_style() {
        let config = r#"
[sources]
  [sources.statsd.primary]
  enabled = true
  host = "localhost"
  port = 1024
  forwards = ["sinks.console", "sinks.null"]
"#;

        let args = parse_config_file(config);

        assert!(args.statsds.is_some());
        let statsds = args.statsds.unwrap();

        let config0 = statsds.get("sources.statsd.primary").unwrap();
        assert_eq!(config0.host, "localhost");
        assert_eq!(config0.port, 1024);
        assert_eq!(
            config0.forwards,
            vec!["sinks.console".to_string(), "sinks.null".to_string()]
        );
    }

    #[test]
    fn config_statsd_sources_histogram_mappings() {
        let config = r#"
[sources]
  [sources.statsd.primary]
  enabled = true
  host = "localhost"
  port = 1024
  forwards = ["sinks.console", "sinks.null"]

  [sources.statsd.primary.mapping]
  [sources.statsd.primary.mapping.foo]
  mask = "foo.*"
  bounds = [0.0, 1.0, 10.0]

  [sources.statsd.primary.mapping.bar]
  mask = "bar.*"
  bounds = [0.0, 2.0, 20.0]
 "#;

        let args = parse_config_file(config);

        assert!(args.statsds.is_some());
        let statsds = args.statsds.unwrap();

        let config0 = statsds.get("sources.statsd.primary").unwrap();
        assert_eq!(config0.host, "localhost");
        assert_eq!(config0.port, 1024);
        assert_eq!(
            config0.forwards,
            vec!["sinks.console".to_string(), "sinks.null".to_string()]
        );
        assert_eq!(
            config0.parse_config.histogram_masks[0].1,
            vec![0.0, 1.0, 10.0]
        );
        assert_eq!(
            config0.parse_config.histogram_masks[1].1,
            vec![0.0, 2.0, 20.0]
        );
    }

    #[test]
    fn config_statsd_sources_error_bound() {
        let config = r#"
[sources]
  [sources.statsd.primary]
  enabled = true
  summarize_error_bound = 0.00001
  forwards = ["sinks.null"]
 "#;

        let args = parse_config_file(config);

        assert!(args.statsds.is_some());
        let statsds = args.statsds.unwrap();

        let config0 = statsds.get("sources.statsd.primary").unwrap();
        assert_eq!(config0.parse_config.summarize_error_bound, 0.00001);
    }

    #[test]
    fn config_statsd_sources_style_multiple() {
        let config = r#"
[sources]
  [sources.statsd.lower]
  enabled = true
  port = 1024
  forwards = ["sinks.console", "sinks.null"]

  [sources.statsd.higher]
  enabled = true
  port = 4048
  forwards = ["sinks.wavefront"]
"#;

        let args = parse_config_file(config);

        assert!(args.statsds.is_some());
        let statsds = args.statsds.unwrap();

        let config0 = statsds.get("sources.statsd.lower").unwrap();
        assert_eq!(config0.port, 1024);
        assert_eq!(
            config0.forwards,
            vec!["sinks.console".to_string(), "sinks.null".to_string()]
        );

        let config1 = statsds.get("sources.statsd.higher").unwrap();
        assert_eq!(config1.port, 4048);
        assert_eq!(config1.forwards, vec!["sinks.wavefront".to_string()]);
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
"#;

        let args = parse_config_file(config);

        assert!(args.graphites.is_some());
        let graphites = args.graphites.unwrap();
        assert_eq!(graphites.len(), 1);

        let config0 = graphites.get("sources.graphite.primary").unwrap();
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
"#;

        let args = parse_config_file(config);

        assert!(args.graphites.is_some());

        assert!(args.graphites.is_some());
        let graphites = args.graphites.unwrap();
        assert_eq!(graphites.len(), 2);

        let config0 = graphites.get("sources.graphite.lower").unwrap();
        assert_eq!(config0.port, 2003);
        assert_eq!(config0.forwards, vec!["filters.collectd_scrub".to_string()]);

        let config1 = graphites.get("sources.graphite.higher").unwrap();
        assert_eq!(config1.port, 2004);
        assert_eq!(config1.forwards, vec!["sinks.wavefront".to_string()]);
    }

    #[test]
    fn config_avro_sources_style() {
        let config = r#"
[sources]
  [sources.avro.primary]
  enabled = true
  host = "localhost"
  port = 2003
  forwards = ["filters.collectd_scrub"]
"#;

        let args = parse_config_file(config);

        assert!(args.avros.is_some());
        let avros = args.avros.unwrap();
        assert_eq!(avros.len(), 1);

        let config0 = avros.get("sources.avro.primary").unwrap();
        assert_eq!(config0.port, 2003);
        assert_eq!(config0.host, "localhost");
        assert_eq!(config0.forwards, vec!["filters.collectd_scrub".to_string()]);
    }

    #[test]
    fn config_avro_sources_style_multiple() {
        let config = r#"
[sources]
  [sources.avro.lower]
  enabled = true
  port = 2003
  forwards = ["filters.collectd_scrub"]

  [sources.avro.higher]
  enabled = true
  port = 2004
  forwards = ["sinks.kinesis"]
"#;

        let args = parse_config_file(config);

        assert!(args.avros.is_some());

        assert!(args.avros.is_some());
        let avros = args.avros.unwrap();
        assert_eq!(avros.len(), 2);

        let config0 = avros.get("sources.avro.lower").unwrap();
        assert_eq!(config0.port, 2003);
        assert_eq!(config0.forwards, vec!["filters.collectd_scrub".to_string()]);

        let config1 = avros.get("sources.avro.higher").unwrap();
        assert_eq!(config1.port, 2004);
        assert_eq!(config1.forwards, vec!["sinks.kinesis".to_string()]);
    }

    #[test]
    fn config_filters_flush_boundary() {
        let config = r#"
    [filters]
      [filters.flush_boundary.ten_seconds]
      tolerance = 10
      forwards = ["sinks.console"]
    "#;

        let args = parse_config_file(config);

        assert!(args.flush_boundary_filters.is_some());
        let filters = args.flush_boundary_filters.unwrap();

        let config0: &FlushBoundaryFilterConfig =
            filters.get("filters.flush_boundary.ten_seconds").unwrap();
        assert_eq!(config0.tolerance, 10);
        assert_eq!(config0.forwards, vec!["sinks.console"]);
    }

    #[test]
    fn config_filters_sources_style() {
        let config = r#"
    [filters]
      [filters.programmable.collectd_scrub]
      script = "cernan_bridge.lua"
      forwards = ["sinks.console"]
    "#;

        let args = parse_config_file(config);

        assert!(args.programmable_filters.is_some());
        let filters = args.programmable_filters.unwrap();

        let config0: &ProgrammableFilterConfig =
            filters.get("filters.programmable.collectd_scrub").unwrap();
        let script = config0.script.clone();
        assert_eq!(
            script.unwrap().to_str().unwrap(),
            "/tmp/cernan-scripts/cernan_bridge.lua"
        );
        assert_eq!(config0.forwards, vec!["sinks.console"]);
    }

    #[test]
    fn config_filters_sources_style_non_default() {
        let config = r#"
    scripts-directory = "data/"
    [filters]
      [filters.programmable.collectd_scrub]
      script = "cernan_bridge.lua"
      forwards = ["sinks.console"]
    "#;

        let args = parse_config_file(config);

        assert!(args.programmable_filters.is_some());
        let filters = args.programmable_filters.unwrap();

        let config0: &ProgrammableFilterConfig =
            filters.get("filters.programmable.collectd_scrub").unwrap();
        let script = config0.script.clone();
        assert_eq!(script.unwrap().to_str().unwrap(), "data/cernan_bridge.lua");
        assert_eq!(config0.forwards, vec!["sinks.console"]);
    }

    #[test]
    fn config_filters_delay() {
        let config = r#"
    [filters]
      [filters.delay.ten_second]
      tolerance = 10
      forwards = ["sinks.console"]
    "#;

        let args = parse_config_file(config);

        assert!(args.delay_filters.is_some());
        let filters = args.delay_filters.unwrap();

        let config0: &DelayFilterConfig =
            filters.get("filters.delay.ten_second").unwrap();
        assert_eq!(config0.tolerance, 10);
        assert_eq!(config0.forwards, vec!["sinks.console"]);
    }

    #[test]
    fn config_filters_json_encode() {
        let config = r#"
    [filters]
        [filters.json_encode.test]
        parse_line = true
        forwards = ["sinks.console"]
    "#;

        let args = parse_config_file(config);
        assert!(args.json_encode_filters.is_some());
        let filters = args.json_encode_filters.unwrap();

        let config0: &JSONEncodeFilterConfig =
            filters.get("filters.json_encode.test").unwrap();
        assert_eq!(config0.parse_line, true);
        assert_eq!(config0.forwards, vec!["sinks.console"]);
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
      age_threshold = 43
    "#;

        let args = parse_config_file(config);

        assert!(args.wavefront.is_some());
        let wavefront = args.wavefront.unwrap();
        assert_eq!(wavefront.host, String::from("example.com"));
        assert_eq!(wavefront.port, 3131);
        assert_eq!(wavefront.bin_width, 9);
        assert_eq!(wavefront.flush_interval, 15 * flushes_per_second());
        assert_eq!(wavefront.age_threshold, Some(43));
    }

    #[test]
    fn config_file_wavefront_percentile_specification() {
        let config = r#"
    [sinks]
      [sinks.wavefront]
      port = 3131
      host = "example.com"
      bin_width = 9

      [sinks.wavefront.percentiles]
      max = 1.0
      min = 0.0
      median = 0.5
    "#;

        let args = parse_config_file(config);

        assert!(args.wavefront.is_some());
        let wavefront = args.wavefront.unwrap();
        assert_eq!(wavefront.host, String::from("example.com"));
        assert_eq!(wavefront.port, 3131);
        assert_eq!(wavefront.bin_width, 9);
        assert_eq!(wavefront.age_threshold, None);

        assert_eq!(wavefront.percentiles.len(), 3);
        assert_eq!(wavefront.percentiles[0], ("max".to_string(), 1.0));
        assert_eq!(wavefront.percentiles[1], ("median".to_string(), 0.5));
        assert_eq!(wavefront.percentiles[2], ("min".to_string(), 0.0));
    }

    #[test]
    fn config_file_wavefront_padding_control() {
        let config = r#"
    [sinks]
      [sinks.wavefront]
      port = 3131
      host = "example.com"
      bin_width = 9

      [sinks.wavefront.padding]
      set = true
      sum = false
      summarize = false
      histogram = true
    "#;

        let args = parse_config_file(config);

        assert!(args.wavefront.is_some());
        let wavefront = args.wavefront.unwrap();
        assert_eq!(wavefront.host, String::from("example.com"));
        assert_eq!(wavefront.port, 3131);
        assert_eq!(wavefront.bin_width, 9);

        assert_eq!(wavefront.pad_control.set, true);
        assert_eq!(wavefront.pad_control.sum, false);
        assert_eq!(wavefront.pad_control.summarize, false);
        assert_eq!(wavefront.pad_control.histogram, true);
    }

    #[test]
    fn config_file_influxdb_sinks_style() {
        let config = r#"
    [sinks]
      [sinks.influxdb]
      port = 3131
      host = "example.com"
      db = "postmates"
      flush_interval = 70
      secure = true
    "#;

        let args = parse_config_file(config);

        assert!(args.influxdb.is_some());
        let influxdb = args.influxdb.unwrap();
        assert_eq!(influxdb.host, String::from("example.com"));
        assert_eq!(influxdb.db, String::from("postmates"));
        assert_eq!(influxdb.port, 3131);
        assert_eq!(influxdb.flush_interval, 70 * flushes_per_second());
        assert_eq!(influxdb.secure, true);
    }

    #[test]
    fn config_file_prometheus_sinks_style() {
        let config = r#"
    [sinks]
      [sinks.prometheus]
      port = 3131
      host = "example.com"
    "#;

        let args = parse_config_file(config);

        assert!(args.prometheus.is_some());
        let prometheus = args.prometheus.unwrap();
        assert_eq!(prometheus.host, String::from("example.com"));
        assert_eq!(prometheus.port, 3131);
        assert_eq!(prometheus.capacity_in_seconds, 600);
    }

    #[test]
    fn config_file_prometheus_explicit_summary_capacity() {
        let config = r#"
    [sinks]
      [sinks.prometheus]
      port = 3131
      host = "example.com"
      capacity_in_seconds = 50
    "#;

        let args = parse_config_file(config);

        assert!(args.prometheus.is_some());
        let prometheus = args.prometheus.unwrap();
        assert_eq!(prometheus.host, String::from("example.com"));
        assert_eq!(prometheus.port, 3131);
        assert_eq!(prometheus.capacity_in_seconds, 50);
    }

    #[test]
    fn config_file_console_explicit_bin_width_sinks_style() {
        let config = r#"
    [sinks]
      [sinks.console]
      bin_width = 9
    "#;

        let args = parse_config_file(config);

        assert!(args.console.is_some());
        let console = args.console.unwrap();
        assert_eq!(console.bin_width, 9);
        assert_eq!(console.flush_interval, args.flush_interval); // default
    }

    #[test]
    fn config_file_null_sinks_style() {
        let config = r#"
    [sinks]
      [sinks.null]
    "#;

        let args = parse_config_file(config);

        assert!(args.null.is_some());
    }

    #[test]
    fn config_file_file_source_single_sources_style() {
        let config = r#"
    [sources]
      [sources.files]
      [sources.files.foo_bar_txt]
      path = "/foo/bar.txt"
      forwards = ["sink.blech"]
    "#;

        let args = parse_config_file(config);

        assert!(args.files.is_some());
        let files = args.files.unwrap();

        assert_eq!(files[0].path, Some(PathBuf::from("/foo/bar.txt")));
        assert_eq!(files[0].forwards, vec!["sink.blech"]);
        assert_eq!(files[0].max_read_bytes, 2048);
    }

    #[test]
    fn config_file_file_source_multiple_sources_style() {
        let config = r#"
    [sources]
      [sources.files]
      [sources.files.foo_bar_txt]
      path = "/foo/bar.txt"
      max_read_bytes = 10
      forwards = ["sink.blech"]

      [sources.files.bar_txt]
      path = "/bar.txt"
      forwards = ["sink.bar.blech"]
    "#;

        let args = parse_config_file(config);

        assert!(args.files.is_some());
        let files = args.files.unwrap();

        assert_eq!(files[0].path, Some(PathBuf::from("/bar.txt")));
        assert_eq!(files[0].max_read_bytes, 2048);
        assert_eq!(files[0].forwards, vec!["sink.bar.blech"]);

        assert_eq!(files[1].path, Some(PathBuf::from("/foo/bar.txt")));
        assert_eq!(files[1].max_read_bytes, 10);
        assert_eq!(files[1].forwards, vec!["sink.blech"]);
    }

    #[test]
    fn flush_interval_converted_to_proper_discrete_values() {
        let config = r#"
        flush-interval = 10
        "#;

        let args = parse_config_file(config);
        assert_eq!(args.flush_interval, 10 * flushes_per_second());

        let config_f = r#"
        flush-interval = 1.5
        "#;

        let args_f = parse_config_file(config_f);
        assert_eq!(args_f.flush_interval, (1.5 * flushes_per_second() as f64) as u64);
    }
}
