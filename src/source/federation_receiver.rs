use bincode::SizeLimit;
use bincode::serde::deserialize_from;
use flate2::read::ZlibDecoder;
use metric;
use mpsc;
use std::io::prelude::*;
use std::io::{Take, BufReader};
use std::net::{TcpListener, TcpStream, ToSocketAddrs};
use std::str;
use std::thread;
use std::time::Instant;

use time;
use super::{send, Source};

pub struct FederationReceiver {
    chans: Vec<mpsc::Sender<metric::Event>>,
    ip: String,
    port: u16,
    tags: metric::TagMap,
}

#[derive(Debug)]
pub struct FederationReceiverConfig {
    pub ip: String,
    pub port: u16,
    pub tags: metric::TagMap,
    pub forwards: Vec<String>,
    pub config_path: String,
}

impl FederationReceiver {
    pub fn new(chans: Vec<mpsc::Sender<metric::Event>>,
               config: FederationReceiverConfig)
               -> FederationReceiver {
        FederationReceiver {
            chans: chans,
            ip: config.ip,
            port: config.port,
            tags: config.tags,
        }
    }
}

fn handle_tcp(chans: Vec<mpsc::Sender<metric::Event>>,
              tags: metric::TagMap,
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

fn handle_stream(mut chans: Vec<mpsc::Sender<metric::Event>>,
                 tags: metric::TagMap,
                 stream: TcpStream) {
    thread::spawn(move || {
        let mut sz_buf = [0; 4];
        let mut reader = BufReader::new(stream);
        match reader.read_exact(&mut sz_buf) {
            Ok(()) => {
                let payload_size_in_bytes = u8tou32abe(&sz_buf);
                trace!("payload_size_in_bytes: {}", payload_size_in_bytes);
                let recv_time = Instant::now();
                let hndl = (&mut reader).take(payload_size_in_bytes as u64);
                debug!("recv time elapsed (ns): {}", time::elapsed_ns(recv_time));
                let mut e = ZlibDecoder::new(hndl);
                match deserialize_from::<ZlibDecoder<Take<&mut BufReader<TcpStream>>>,
                                         Vec<metric::Event>>(&mut e, SizeLimit::Infinite) {
                    Ok(events) => {
                        trace!("total events in payload: {}", events.len());
                        for mut ev in events {
                            trace!("event: {:?}", ev);
                            ev = match ev {
                                metric::Event::Statsd(m) => {
                                    metric::Event::Statsd(m.merge_tags_from_map(&tags))
                                }
                                metric::Event::Graphite(m) => {
                                    metric::Event::Graphite(m.merge_tags_from_map(&tags))
                                }
                                // we refuse to accept any non-telemetry forward
                                // for now
                                _ => continue,
                            };
                            send("receiver", &mut chans, &ev);
                        }
                        let metric = metric::Metric::new("cernan.federation.receiver.packet", 1.0)
                            .counter()
                            .overlay_tags_from_map(&tags);
                        send("receiver", &mut chans, &metric::Event::Statsd(metric));
                    }
                    Err(e) => {
                        trace!("failed to decode payload with error: {:?}", e);
                        panic!("Failed decoding. Skipping {:?}", e);
                    }
                }
            }
            Err(e) => trace!("Unable to read payload: {:?}", e),
        }
    });
}


impl Source for FederationReceiver {
    fn run(&mut self) {
        let srv: Vec<_> = (self.ip.as_str(), self.port)
            .to_socket_addrs()
            .expect("unable to make socket addr")
            .collect();
        let listener = TcpListener::bind(srv.first().unwrap())
            .expect("Unable to bind to TCP socket");
        let chans = self.chans.clone();
        let tags = self.tags.clone();
        let jh = thread::spawn(move || handle_tcp(chans, tags, listener));

        jh.join().expect("Uh oh, child thread paniced!");
    }
}

#[inline]
fn u8tou32abe(v: &[u8]) -> u32 {
    (v[3] as u32) + ((v[2] as u32) << 8) + ((v[1] as u32) << 24) + ((v[0] as u32) << 16)
}
