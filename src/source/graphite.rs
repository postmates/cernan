use metric;
use protocols::graphite::parse_graphite;
use std::io::BufReader;
use std::io::prelude::*;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddrV4, SocketAddrV6};
use std::net::{TcpListener, TcpStream};
use std::str;
use std::sync::Arc;
use std::thread;
use super::Source;
use util;
use util::send;

pub struct Graphite {
    chans: util::Channel,
    port: u16,
    tags: Arc<metric::TagMap>,
}

#[derive(Debug,Clone)]
pub struct GraphiteConfig {
    pub ip: String,
    pub port: u16,
    pub tags: metric::TagMap,
    pub forwards: Vec<String>,
    pub config_path: String,
}

impl Default for GraphiteConfig {
    fn default() -> GraphiteConfig {
        GraphiteConfig {
            ip: String::from(""),
            port: 2003,
            tags: metric::TagMap::default(),
            forwards: Vec::new(),
            config_path: "sources.graphite".to_string(),
        }
    }
}

impl Graphite {
    pub fn new(chans: util::Channel, config: GraphiteConfig) -> Graphite {
        Graphite {
            chans: chans,
            port: config.port,
            tags: Arc::new(config.tags),
        }
    }
}

fn handle_tcp(chans: util::Channel,
              tags: Arc<metric::TagMap>,
              listner: TcpListener)
              -> thread::JoinHandle<()> {
    thread::spawn(move || {
        for stream in listner.incoming() {
            if let Ok(stream) = stream {
                debug!("new peer at {:?} | local addr for peer {:?}",
                       stream.peer_addr(),
                       stream.local_addr());
                let tags = tags.clone();
                let chans = chans.clone();
                thread::spawn(move || {
                    handle_stream(chans, tags, stream);
                });
            }
        }
    })
}


fn handle_stream(mut chans: util::Channel, tags: Arc<metric::TagMap>, stream: TcpStream) {
    thread::spawn(move || {
        let mut line = String::new();
        let mut res = Vec::new();
        let mut line_reader = BufReader::new(stream);
        let basic_metric = Arc::new(Some(metric::Metric::default().overlay_tags_from_map(&tags)));
        while let Some(len) = line_reader.read_line(&mut line).ok() {
            if len > 0 {
                if parse_graphite(&line, &mut res, basic_metric.clone()) {
                    let metric = metric::Metric::new("cernan.graphite.packet", 1.0)
                        .counter()
                        .overlay_tags_from_map(&tags);
                    send("graphite",
                         &mut chans,
                         metric::Event::Telemetry(Arc::new(Some(metric))));
                    for m in res.drain(..) {
                        send("graphite",
                             &mut chans,
                             metric::Event::Telemetry(Arc::new(Some(m))));
                    }
                } else {
                    let metric = metric::Metric::new("cernan.graphite.bad_packet", 1.0)
                        .counter()
                        .overlay_tags_from_map(&tags);
                    send("graphite",
                         &mut chans,
                         metric::Event::Telemetry(Arc::new(Some(metric))));
                    error!("bad packet: {:?}", line);
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

        let addr_v6 = SocketAddrV6::new(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1), self.port, 0, 0);
        let listener_v6 = TcpListener::bind(addr_v6).expect("Unable to bind to TCP V6 socket");
        let chans_v6 = self.chans.clone();
        let tags_v6 = self.tags.clone();
        info!("server started on ::1 {}", self.port);
        joins.push(thread::spawn(move || handle_tcp(chans_v6, tags_v6, listener_v6)));

        let addr_v4 = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), self.port);
        let listener_v4 = TcpListener::bind(addr_v4).expect("Unable to bind to TCP V4 socket");
        let chans_v4 = self.chans.clone();
        let tags_v4 = self.tags.clone();
        info!("server started on 127.0.0.1:{}", self.port);
        joins.push(thread::spawn(move || handle_tcp(chans_v4, tags_v4, listener_v4)));

        // TODO thread spawn trick, join on results
        for jh in joins {
            // TODO Having sub-threads panic will not cause a bubble-up if that
            // thread is not the currently examined one. We're going to have to have
            // some manner of sub-thread communication going on.
            jh.join().expect("Uh oh, child thread paniced!");
        }
    }
}
