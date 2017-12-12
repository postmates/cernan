use super::Source;
use metric;
use protocols::graphite::parse_graphite;
use std::io::BufReader;
use std::io::prelude::*;
use std::net::{TcpListener, TcpStream};
use std::net::ToSocketAddrs;
use std::str;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use util;
use util::send;

lazy_static! {
    pub static ref GRAPHITE_NEW_PEER: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    pub static ref GRAPHITE_GOOD_PACKET: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    pub static ref GRAPHITE_TELEM: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    pub static ref GRAPHITE_BAD_PACKET: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
}

/// Graphite protocol source
///
/// This source produces `metric::Telemetry` from the graphite protocol.
pub struct Graphite {
    chans: util::Channel,
    host: String,
    port: u16,
    tags: Arc<metric::TagMap>,
}

/// Configured for the `metric::Telemetry` source.
#[derive(Debug, Deserialize, Clone)]
pub struct GraphiteConfig {
    /// The host that the source will listen on. May be an IP address or a DNS
    /// hostname.
    pub host: String,
    /// The port that the source will listen on.
    pub port: u16,
    /// The tags that the source will apply to all Telemetry it creates.
    pub tags: metric::TagMap,
    /// The forwards that the source will send all its Telemetry.
    pub forwards: Vec<String>,
    /// The unique name of the source in the routing topology.
    pub config_path: Option<String>,
}

impl Default for GraphiteConfig {
    fn default() -> GraphiteConfig {
        GraphiteConfig {
            host: "localhost".to_string(),
            port: 2003,
            tags: metric::TagMap::default(),
            forwards: Vec::new(),
            config_path: Some("sources.graphite".to_string()),
        }
    }
}

impl Graphite {
    /// Create a new Graphite
    pub fn new(chans: util::Channel, config: GraphiteConfig) -> Graphite {
        Graphite {
            chans: chans,
            host: config.host,
            port: config.port,
            tags: Arc::new(config.tags),
        }
    }
}

fn handle_tcp(
    chans: util::Channel,
    tags: Arc<metric::TagMap>,
    listner: TcpListener,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        for stream in listner.incoming() {
            if let Ok(stream) = stream {
                GRAPHITE_NEW_PEER.fetch_add(1, Ordering::Relaxed);
                debug!(
                    "new peer at {:?} | local addr for peer {:?}",
                    stream.peer_addr(),
                    stream.local_addr()
                );
                let tags = Arc::clone(&tags);
                let chans = chans.clone();
                thread::spawn(move || {
                    handle_stream(chans, tags, stream);
                });
            }
        }
    })
}

fn handle_stream(
    mut chans: util::Channel,
    tags: Arc<metric::TagMap>,
    stream: TcpStream,
) {
    thread::spawn(move || {
        let mut line = String::new();
        let mut res = Vec::new();
        let mut line_reader = BufReader::new(stream);
        let basic_metric = Arc::new(Some(
            metric::Telemetry::default().overlay_tags_from_map(&tags),
        ));
        while let Some(len) = line_reader.read_line(&mut line).ok() {
            if len > 0 {
                if parse_graphite(&line, &mut res, &basic_metric) {
                    assert!(!res.is_empty());
                    GRAPHITE_GOOD_PACKET.fetch_add(1, Ordering::Relaxed);
                    GRAPHITE_TELEM.fetch_add(1, Ordering::Relaxed);
                    for m in res.drain(..) {
                        send(&mut chans, metric::Event::Telemetry(Arc::new(Some(m))));
                    }
                    line.clear();
                } else {
                    GRAPHITE_BAD_PACKET.fetch_add(1, Ordering::Relaxed);
                    error!("bad packet: {:?}", line);
                    line.clear();
                }
            } else {
                break;
            }
        }
    });
}

impl Source for Graphite {
    fn run(&mut self) {
        let mut joins = Vec::new();

        let addrs = (self.host.as_str(), self.port).to_socket_addrs();
        match addrs {
            Ok(ips) => {
                let ips: Vec<_> = ips.collect();
                for addr in ips {
                    let listener =
                        TcpListener::bind(addr).expect("Unable to bind to TCP socket");
                    let chans = self.chans.clone();
                    let tags = Arc::clone(&self.tags);
                    info!("server started on {:?} {}", addr, self.port);
                    joins.push(thread::spawn(move || {
                        handle_tcp(chans, tags, listener)
                    }));
                }
            }
            Err(e) => {
                info!(
                    "Unable to perform DNS lookup on host {} with error {}",
                    self.host, e
                );
            }
        }

        // TODO thread spawn trick, join on results
        for jh in joins {
            // TODO Having sub-threads panic will not cause a bubble-up if that
            // thread is not the currently examined one. We're going to have to have
            // some manner of sub-thread communication going on.
            jh.join().expect("Uh oh, child thread panicked!");
        }
    }
}
