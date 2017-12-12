use constants;
use metric;
use mio;
use protocols::statsd::parse_statsd;
use regex::Regex;
use source::Source;
use std::net::ToSocketAddrs;
use std::str;
use std::sync;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
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
    /// Configure the error bound for a statsd timer or histogram. Cernan does
    /// not compute precise quantiles but approximations with a guaranteed upper
    /// bound on the error of approximation. This allows the end-user to set
    /// that.
    pub summarize_error_bound: f64,
}

impl Default for StatsdParseConfig {
    fn default() -> StatsdParseConfig {
        StatsdParseConfig {
            histogram_masks: vec![],
            summarize_error_bound: 0.01,
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
    conns: &util::TokenSlab<mio::net::UdpSocket>,
    poll: &mio::Poll,
) {
    let mut buf = vec![0; 16_250];
    let mut metrics = Vec::new();
    let basic_metric = sync::Arc::new(Some(
        metric::Telemetry::default().overlay_tags_from_map(tags),
    ));
    loop {
        let mut events = mio::Events::with_capacity(1024);
        match poll.poll(&mut events, None) {
            Ok(_num_events) => for event in events {
                match event.token() {
                    constants::SYSTEM => return,
                    token => {
                        // Get the socket to receive from:
                        let socket = &conns[token];

                        let (len, _) = match socket.recv_from(&mut buf) {
                            Ok(r) => r,
                            Err(e) => panic!(format!(
                                "Could not read UDP socket with error {:?}",
                                e
                            )),
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
            },
            Err(e) => panic!(format!("Failed during poll {:?}", e)),
        }
    } // loop
} // handle_udp

impl Source for Statsd {
    fn run(&mut self, poll: mio::Poll) {
        let addrs = (self.host.as_str(), self.port).to_socket_addrs();
        match addrs {
            Ok(ips) => {
                let mut conns = util::TokenSlab::<mio::net::UdpSocket>::new();
                for addr in ips {
                    let socket = mio::net::UdpSocket::bind(&addr)
                        .expect("Unable to bind to UDP socket");
                    let token = conns.insert(socket);
                    poll.register(
                        &conns[token],
                        token,
                        mio::Ready::readable(),
                        mio::PollOpt::edge(),
                    ).unwrap();
                }

                let chans = self.chans.clone();
                info!("server started on *:{}", self.port);
                handle_udp(chans, &self.tags, &self.parse_config, &conns, &poll);
            }
            Err(e) => {
                info!(
                    "Unable to perform DNS lookup on host {} with error {}",
                    self.host, e
                );
            }
        }
    }
}
