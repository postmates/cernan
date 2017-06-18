use super::Source;
use metric;
use protocols::graphite::parse_graphite;
use source::internal::report_telemetry;
use std::io::BufReader;
use std::io::prelude::*;
use std::net::{TcpListener, TcpStream};
use std::net::ToSocketAddrs;
use std::str;
use std::sync::Arc;
use std::thread;
use util;
use util::send;

pub struct Graphite {
    chans: util::Channel,
    host: String,
    port: u16,
    tags: Arc<metric::TagMap>,
    telemetry_error: f64,
}

#[derive(Debug, Deserialize, Clone)]
pub struct GraphiteConfig {
    pub host: String,
    pub port: u16,
    pub tags: metric::TagMap,
    pub forwards: Vec<String>,
    pub config_path: Option<String>,
    pub telemetry_error: f64,
}

impl Default for GraphiteConfig {
    fn default() -> GraphiteConfig {
        GraphiteConfig {
            host: "localhost".to_string(),
            port: 2003,
            tags: metric::TagMap::default(),
            forwards: Vec::new(),
            config_path: Some("sources.graphite".to_string()),
            telemetry_error: 0.001,
        }
    }
}

impl Graphite {
    pub fn new(chans: util::Channel, config: GraphiteConfig) -> Graphite {
        Graphite {
            chans: chans,
            host: config.host,
            port: config.port,
            tags: Arc::new(config.tags),
            telemetry_error: config.telemetry_error,
        }
    }
}

fn handle_tcp(chans: util::Channel,
              tags: Arc<metric::TagMap>,
              listner: TcpListener,
              telemetry_error: f64)
              -> thread::JoinHandle<()> {
    thread::spawn(move || for stream in listner.incoming() {
                      if let Ok(stream) = stream {
                          report_telemetry("cernan.graphite.new_peer", 1.0, telemetry_error);
                          debug!("new peer at {:?} | local addr for peer {:?}",
                                 stream.peer_addr(),
                                 stream.local_addr());
                          let tags = tags.clone();
                          let chans = chans.clone();
                          thread::spawn(move || {
                                            handle_stream(chans, tags, stream, telemetry_error);
                                        });
                      }
                  })
}


fn handle_stream(mut chans: util::Channel,
                 tags: Arc<metric::TagMap>,
                 stream: TcpStream,
                 telemetry_error: f64) {
    thread::spawn(move || {
        let mut line = String::new();
        let mut res = Vec::new();
        let mut line_reader = BufReader::new(stream);
        let basic_metric = Arc::new(Some(metric::Telemetry::default()
                                             .overlay_tags_from_map(&tags)));
        while let Some(len) = line_reader.read_line(&mut line).ok() {
            if len > 0 {
                if parse_graphite(&line, &mut res, basic_metric.clone()) {
                    report_telemetry("cernan.graphite.packet", 1.0, telemetry_error);
                    for m in res.drain(..) {
                        send(&mut chans, metric::Event::Telemetry(Arc::new(Some(m))));
                    }
                    line.clear();
                } else {
                    report_telemetry("cernan.graphite.bad_packet", 1.0, telemetry_error);
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
                    let tags = self.tags.clone();
                    info!("server started on {:?} {}", addr, self.port);
                    let error = self.telemetry_error;
                    joins.push(thread::spawn(move || {
                                                 handle_tcp(chans, tags, listener, error)
                                             }));
                }
            }
            Err(e) => {
                info!("Unable to perform DNS lookup on host {} with error {}",
                      self.host,
                      e);
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
