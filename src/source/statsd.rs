use metric;
use protocols::statsd::parse_statsd;
use regex::Regex;
use source::Source;
use std::net::{ToSocketAddrs, UdpSocket};
use std::str;
use std::sync;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use util;
use util::send;

lazy_static! {
    pub static ref STATSD_GOOD_PACKET: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    pub static ref STATSD_BAD_PACKET: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
}

/// The statsd source
///
/// Statsd is a collection of protocols, originally spawned by the telemetering
/// work done out of Etsy. Cernan tries to support a cow-path subset of the
/// statsd protocol family.
pub struct Statsd {
    chans: util::Channel,
    host: String,
    port: u16,
    tags: sync::Arc<metric::TagMap>,
    parse_config: sync::Arc<StatsdParseConfig>,
}

/// The mask type for metrics in `StatsdParseConfig`.
pub type Mask = Regex;

/// The bound type for metrics in `StatsdParseConfig`.
pub type Bounds = Vec<f64>;

/// Configuration for the statsd parser
#[derive(Debug, Clone)]
pub struct StatsdParseConfig {
    /// Set specific bin masks for timeseries according to their name. The name
    /// may be a [regex](https://crates.io/crates/regex) match, such like
    /// 'foo.*'. In this case all metrics prefixed by 'foo.' which are timer or
    /// histogram will be interpreted as a histogram.
    pub histogram_masks: Vec<(Mask, Bounds)>,
}

impl Default for StatsdParseConfig {
    fn default() -> StatsdParseConfig {
        StatsdParseConfig {
            histogram_masks: vec![],
        }
    }
}

/// Configuration for the statsd source.
#[derive(Debug, Clone)]
pub struct StatsdConfig {
    /// The host for the statsd protocol to bind to.
    pub host: String,
    /// The port for the statsd source to listen on.
    pub port: u16,
    /// The tagmap that statsd will apply to all of its created Telemetry.
    pub tags: metric::TagMap,
    /// The forwards that statsd will send its telemetry on to.
    pub forwards: Vec<String>,
    /// The unique name for the source in the routing topology.
    pub config_path: Option<String>,
    /// Configuration for the parsing of statsd lines
    pub parse_config: StatsdParseConfig,
}

impl Default for StatsdConfig {
    fn default() -> StatsdConfig {
        StatsdConfig {
            host: "localhost".to_string(),
            port: 8125,
            tags: metric::TagMap::default(),
            forwards: Vec::new(),
            config_path: None,
            parse_config: StatsdParseConfig::default(),
        }
    }
}

impl Statsd {
    /// Create a new statsd
    pub fn new(chans: util::Channel, config: StatsdConfig) -> Statsd {
        Statsd {
            chans: chans,
            host: config.host,
            port: config.port,
            tags: sync::Arc::new(config.tags),
            parse_config: sync::Arc::new(config.parse_config),
        }
    }
}

fn handle_udp(
    mut chans: util::Channel,
    tags: &sync::Arc<metric::TagMap>,
    parse_config: &sync::Arc<StatsdParseConfig>,
    socket: &UdpSocket,
) {
    let mut buf = vec![0; 16_250];
    let mut metrics = Vec::new();
    let basic_metric = sync::Arc::new(Some(
        metric::Telemetry::default().overlay_tags_from_map(tags),
    ));
    loop {
        let (len, _) = match socket.recv_from(&mut buf) {
            Ok(r) => r,
            Err(e) => panic!(format!("Could not read UDP socket with error {:?}", e)),
        };
        match str::from_utf8(&buf[..len]) {
            Ok(val) => if parse_statsd(
                val,
                &mut metrics,
                &basic_metric,
                parse_config,
            ) {
                for m in metrics.drain(..) {
                    send(&mut chans, metric::Event::new_telemetry(m));
                }
                STATSD_GOOD_PACKET.fetch_add(1, Ordering::Relaxed);
            } else {
                STATSD_BAD_PACKET.fetch_add(1, Ordering::Relaxed);
                error!("BAD PACKET: {:?}", val);
            },
            Err(e) => {
                error!("Payload not valid UTF-8: {:?}", e);
            }
        }
    }
}

impl Source for Statsd {
    fn run(&mut self) {
        let mut joins = Vec::new();

        let addrs = (self.host.as_str(), self.port).to_socket_addrs();
        match addrs {
            Ok(ips) => {
                let ips: Vec<_> = ips.collect();
                for addr in ips {
                    let listener =
                        UdpSocket::bind(addr).expect("Unable to bind to TCP socket");
                    let chans = self.chans.clone();
                    let tags = sync::Arc::clone(&self.tags);
                    let parse_config = sync::Arc::clone(&self.parse_config);
                    info!("server started on {:?} {}", addr, self.port);
                    joins.push(thread::spawn(
                        move || handle_udp(chans, &tags, &parse_config, &listener),
                    ));
                }
            }
            Err(e) => {
                info!(
                    "Unable to perform DNS lookup on host {} with error {}",
                    self.host,
                    e
                );
            }
        }

        for jh in joins {
            // TODO Having sub-threads panic will not cause a bubble-up if that
            // thread is not the currently examined one. We're going to have to have
            // some manner of sub-thread communication going on.
            jh.join().expect("Uh oh, child thread panicked!");
        }
    }
}
