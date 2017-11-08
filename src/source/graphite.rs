use super::Source;
extern crate mio;
use metric;
use constants;
use std;
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
use std::collections::HashMap;

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

fn spawn_stream_handlers(
    chans: util::Channel,
    tags: std::sync::Arc<metric::TagMap>,
    listener : & mio::net::TcpListener,
    stream_handlers : &mut Vec<util::ChildThread>,
) -> () {
    loop {
        match listener.accept() {
            Ok((stream, _addr)) => {
                let chans = chans.clone();
                let tags_clone = std::sync::Arc::clone(&tags);
                let new_stream = util::ChildThread::new(move |poller| {
                    poller.register(
                        &stream,
                        mio::Token(0),
                        mio::Ready::readable(),
                        mio::PollOpt::edge()).unwrap();

                    handle_stream(
                       chans.clone(),
                       tags_clone,
                       poller,
                       stream);
                });

                stream_handlers.push(new_stream);
            }

            Err(e) => if e.kind() == std::io::ErrorKind::WouldBlock {
                break;
            }

            Err(e) => {
                panic!("Failed while accepting new connection");
            }
        };

    }
}

fn handle_tcp(
    chans: util::Channel,
    tags: std::sync::Arc<metric::TagMap>,
    socket_map: HashMap<mio::Token, mio::net::TcpListener>,
    poll: mio::Poll,
) {
    let mut stream_handlers = Vec::new();
    loop {
        let mut events = mio::Events::with_capacity(1024);
        match poll.poll(& mut events, None) {
            Err(e) =>
                panic!(format!("Failed during poll {:?}", e)),
            Ok(_num_events) => {
                for event in events {
                    match event.token() {
                        constants::SYSTEM => return, // TODO - Shutdown stream handlers.
                        listener_token => {
                            let listener = socket_map.get(&listener_token).unwrap();
                            spawn_stream_handlers(chans, tags, &listener, &mut stream_handlers);
                        }
                    }
                }
            }
        }
    }
}

fn handle_stream(
    mut chans: util::Channel,
    tags: Arc<metric::TagMap>,
    poller: mio::Poll,
    stream: mio::net::TcpStream,
) {
    let mut line = String::new();
    let mut res = Vec::new();
    let mut line_reader = BufReader::new(stream);
    let basic_metric = Arc::new(Some(
        metric::Telemetry::default().overlay_tags_from_map(&tags),
    ));
    while let Some(len) = line_reader.read_line(&mut line).ok() {
        if len > 0 {
            if parse_graphite(&line, &mut res, Arc::clone(&basic_metric)) {
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
}

impl Source for Graphite {
    fn run(&mut self, poll: mio::Poll) {
        let addrs = (self.host.as_str(), self.port).to_socket_addrs();
        match addrs {
            Ok(ips) => {
                let ips: Vec<_> = ips.collect();
                let mut socket_map : HashMap<mio::Token, mio::net::TcpListener> = HashMap::new();
                for i in 0..ips.len() {
                    let token = mio::Token(i);
                    let addr = ips[i];
                    let listener =
                        mio::net::TcpListener::bind(&addr).expect("Unable to bind to TCP socket");
                    info!("registered listener for {:?} {}", addr, self.port);
                    poll.register(
                        &listener,
                        token,
                        mio::Ready::readable(),
                        mio::PollOpt::edge()).unwrap();

                    socket_map.insert(token, listener);
                }

                handle_tcp(self.chans.clone(), std::sync::Arc::clone(&self.tags), socket_map, poll);
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
