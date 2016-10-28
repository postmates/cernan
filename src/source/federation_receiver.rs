use bincode::SizeLimit;
use bincode::serde::deserialize_from;
use flate2::read::ZlibDecoder;
use fnv::FnvHasher;
use metric;
use mpsc;
use notify::op::{REMOVE, RENAME, WRITE};
use notify::{RecommendedWatcher, Error, Watcher};
use std::collections::HashMap;
use std::fs::File;
use std::hash::BuildHasherDefault;
use std::io::prelude::*;
use std::io::{Take, SeekFrom, BufReader};
use std::net::{Ipv6Addr, UdpSocket, SocketAddrV6, SocketAddrV4, Ipv4Addr};
use std::net::{TcpListener, TcpStream, ToSocketAddrs};
use std::path::PathBuf;
use std::str;
use std::sync::mpsc::channel;
use std::thread::sleep;
use std::thread;
use std::time::{Duration, Instant};

use super::{send, Source};

pub struct FederationReceiver {
    chans: Vec<mpsc::Sender<metric::Event>>,
    listener: TcpListener,
    tags: metric::TagMap,
}

impl FederationReceiver {
    pub fn new(chans: Vec<mpsc::Sender<metric::Event>>,
               ip: &str,
               port: u16,
               tags: metric::TagMap)
               -> FederationReceiver {
        let srv: Vec<_> =
            (ip, port).to_socket_addrs().expect("unable to make socket addr").collect();
        let listener = TcpListener::bind(srv.first().unwrap())
            .expect("Unable to bind to TCP socket");
        FederationReceiver {
            chans: chans,
            listener: listener,
            tags: tags,
        }
    }

    fn handle_receiver_client(&mut self, stream: TcpStream) {
        let tags = self.tags.clone();
        let chans = self.chans.clone();
        thread::spawn(move || {
            let mut sz_buf = [0; 4];
            let mut reader = BufReader::new(stream);
            match reader.read_exact(&mut sz_buf) {
                Ok(()) => {
                let payload_size_in_bytes = u8tou32abe(&sz_buf);
                trace!("[receiver] payload_size_in_bytes: {}", payload_size_in_bytes);
                let recv_time = Instant::now();
                let hndl = (&mut reader).take(payload_size_in_bytes as u64);
                // NOTE this elasped time is wrong! See NOTEs throughout this module.
                debug!("[receiver] recv time elapsed (ns): {}", recv_time.elapsed().subsec_nanos());
                let mut e = ZlibDecoder::new(hndl);
                match deserialize_from::<ZlibDecoder<Take<&mut BufReader<TcpStream>>>,
                                         Vec<metric::Event>>(&mut e, SizeLimit::Infinite) {
                    Ok(events) => {
                        trace!("[receiver] total events in payload: {}", events.len());
                        for mut ev in events {
                            trace!("[receiver] event: {:?}", ev);
                            ev = match ev {
                                metric::Event::Statsd(m) => {
                                    metric::Event::Statsd(m.merge_tags_from_map(&tags))
                                }
                                metric::Event::Graphite(m) => {
                                    metric::Event::Graphite(m.merge_tags_from_map(&tags))
                                }
                                _ => continue, // we refuse to accept any non-telemetry forward for now
                            };
                            send("receiver", &mut chans, &ev);
                        }
                        let metric = metric::Metric::new("cernan.federation.receiver.packet", 1.0)
                            .counter()
                            .overlay_tags_from_map(&tags);
                        send("receiver", &mut chans, &metric::Event::Statsd(metric));
                    }
                    Err(e) => {
                        trace!("[receiver] failed to decode payload with error: {:?}", e);
                        panic!("Failed decoding. Skipping {:?}", e);
                    }
                }
            }
                Err(e) => trace!("[receiver] Unable to read payload: {:?}", e),
            }
        });
    }
}

impl Source for FederationReceiver {
    fn run(&mut self) {
        for stream in self.listener.incoming() {
            if let Ok(stream) = stream {
                stream.set_nonblocking(false).expect("could not set TcpStream to block");
                let srv_chans = self.chans.clone();
                // NOTE cloning these tags for every TCP connection is _crummy_!
                // This will increase memory fragmentation / use of memory and will
                // add latency to connection setup.
                self.handle_receiver_client(stream);
            }
        }
    }
}

#[inline]
fn u8tou32abe(v: &[u8]) -> u32 {
    (v[3] as u32) + ((v[2] as u32) << 8) + ((v[1] as u32) << 24) + ((v[0] as u32) << 16)
}
