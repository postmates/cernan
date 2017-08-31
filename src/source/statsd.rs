use metric;
use protocols::statsd::parse_statsd;
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
}

/// Configuration for the statsd source.
#[derive(Debug, Deserialize, Clone)]
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
}

impl Default for StatsdConfig {
    fn default() -> StatsdConfig {
        StatsdConfig {
            host: "localhost".to_string(),
            port: 8125,
            tags: metric::TagMap::default(),
            forwards: Vec::new(),
            config_path: None,
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
        }
    }
}

fn handle_udp(
    mut chans: util::Channel,
    tags: sync::Arc<metric::TagMap>,
    socket: &UdpSocket,
) {
    let mut buf = [0; 8192];
    let mut metrics = Vec::new();
    let basic_metric = sync::Arc::new(Some(
        metric::Telemetry::default().overlay_tags_from_map(&tags),
    ));
    loop {
        let (len, _) = match socket.recv_from(&mut buf) {
            Ok(r) => r,
            Err(e) => panic!(format!("Could not read UDP socket with error {:?}", e)),
        };
        match str::from_utf8(&buf[..len]) {
            Ok(val) => if parse_statsd(val, &mut metrics, basic_metric.clone()) {
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
                    let tags = self.tags.clone();
                    info!("server started on {:?} {}", addr, self.port);
                    joins.push(
                        thread::spawn(move || handle_udp(chans, tags, &listener)),
                    );
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
