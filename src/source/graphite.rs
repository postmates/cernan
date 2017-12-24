use constants;
use metric;
use mio;
use protocols::graphite::parse_graphite;
use source;
use std::io::BufReader;
use std::io::prelude::*;
use std::str;
use std::sync;
use std::sync::atomic::{AtomicUsize, Ordering};
use thread;
use util;
use util::send;

lazy_static! {
    pub static ref GRAPHITE_NEW_PEER: sync::Arc<AtomicUsize> = sync::Arc::new(AtomicUsize::new(0));
    pub static ref GRAPHITE_GOOD_PACKET: sync::Arc<AtomicUsize> = sync::Arc::new(AtomicUsize::new(0));
    pub static ref GRAPHITE_TELEM: sync::Arc<AtomicUsize> = sync::Arc::new(AtomicUsize::new(0));
    pub static ref GRAPHITE_BAD_PACKET: sync::Arc<AtomicUsize> = sync::Arc::new(AtomicUsize::new(0));
}

/// Graphite protocol source
///
/// This source produces `metric::Telemetry` from the graphite protocol.
pub struct Graphite {
    server: source::TCP,
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

impl From<GraphiteConfig> for source::tcp::TCPConfig {
    fn from(item: GraphiteConfig) -> Self {
        source::tcp::TCPConfig {
            host: item.host,
            port: item.port,
            tags: item.tags,
            forwards: item.forwards,
            config_path: item.config_path,
        }
    }
}

fn handle_stream(
    mut chans: util::Channel,
    tags: &sync::Arc<metric::TagMap>,
    poller: &mio::Poll,
    stream: mio::net::TcpStream,
) {
    let mut line = String::new();
    let mut res = Vec::new();
    let mut line_reader = BufReader::new(stream);
    let basic_metric = sync::Arc::new(Some(
        metric::Telemetry::default().overlay_tags_from_map(tags),
    ));

    loop {
        let mut events = mio::Events::with_capacity(1024);
        match poller.poll(&mut events, None) {
            Err(e) => panic!(format!("Failed during poll {:?}", e)),
            Ok(_num_events) => for event in events {
                match event.token() {
                    constants::SYSTEM => return,
                    _stream_token => if let Ok(len) = line_reader.read_line(&mut line)
                    {
                        if len > 0 {
                            if parse_graphite(&line, &mut res, &basic_metric) {
                                assert!(!res.is_empty());
                                GRAPHITE_GOOD_PACKET.fetch_add(1, Ordering::Relaxed);
                                GRAPHITE_TELEM.fetch_add(1, Ordering::Relaxed);
                                for m in res.drain(..) {
                                    send(
                                        &mut chans,
                                        metric::Event::Telemetry(sync::Arc::new(
                                            Some(m),
                                        )),
                                    );
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
                    },
                }
            },
        }
    }
}

impl source::Source<Graphite, GraphiteConfig> for Graphite {
    /// Create a new Graphite
    fn new(chans: util::Channel, config: GraphiteConfig) -> Graphite {
        Graphite {
            server: source::TCP::new(chans, config.into()),
        }
    }

    fn run(self) -> thread::ThreadHandle {
        self.server.run(handle_stream)
    }
}
