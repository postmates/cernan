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

pub struct Graphite {
    chans: Vec<mpsc::Sender<metric::Event>>,
    listener_v6: TcpListener,
    listener_v4: TcpListener,
    tags: metric::TagMap,
}

impl Graphite {
    pub fn new(chans: Vec<mpsc::Sender<metric::Event>>, port: u16) {
        let addr_v6 = SocketAddrV6::new(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1), port, 0, 0);
        let listener_v6 = TcpListener::bind(addr_v6).expect("Unable to bind to TCP V6 socket");
        let addr_v4 = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), port);
        let listener_v4 = TcpListener::bind(addr_v4).expect("Unable to bind to TCP V4 socket");
        Graphite {
            chans: chans,
            listener_v4: listener_v4,
            listener_v6: listener_v6,
        }
    }

    fn handle_client(mut chans: Vec<mpsc::Sender<metric::Event>>, stream: TcpStream) {
        let line_reader = BufReader::new(stream);
        for line in line_reader.lines() {
            match line {
                Ok(line) => {
                    let buf = line.into_bytes();
                    str::from_utf8(&buf)
                        .map(|val| {
                            trace!("[graphite] {}", val);
                            let pyld_hndl_time = Instant::now();
                            match metric::Metric::parse_graphite(val) {
                                Some(metrics) => {
                                    let metric = metric::Metric::new("cernan.graphite.packet", 1.0)
                                        .counter()
                                        .overlay_tags_from_map(&tags);;
                                    send("graphite", &mut chans, &metric::Event::Statsd(metric));
                                    for mut m in metrics {
                                        m = m.overlay_tags_from_map(&tags);
                                        send("graphite", &mut chans, &metric::Event::Graphite(m));
                                    }
                                    // NOTE this is wrong! See above NOTE.
                                    debug!("[graphite] payload handle effective, elapsed (ns): {}", pyld_hndl_time.elapsed().subsec_nanos());
                                }
                                None => {
                                    let metric = metric::Metric::new("cernan.graphite.bad_packet", 1.0)
                                        .counter()
                                        .overlay_tags_from_map(&tags);
                                    send("graphite", &mut chans, &metric::Event::Statsd(metric));
                                    error!("BAD PACKET: {:?}", val);
                                    // NOTE this is wrong! See above NOTE.
                                    debug!("[graphite] payload handle failure, elapsed (ns): {}", pyld_hndl_time.elapsed().subsec_nanos());
                                }
                            }
                        })
                        .ok();
                }
                Err(_) => break,
            }
        }
    }
}

impl Source for Graphite {
    fn run(&mut self) {
        let mut joins = Vec::new();

        // TODO thread spawn trick, join on results
        joins.push(thread::spawn(move || {
            for stream in self.listener_v4.incoming() {
                if let Ok(stream) = stream {
                    debug!("[graphite] new peer at {:?} | local addr for peer {:?}", stream.peer_addr(), stream.local_addr());
                    let srv_chans = chans.clone();
                    let tags = tags.clone();
                    thread::spawn(move || handle_client(srv_chans, stream, tags));
                }
            }
        }));
        joins.push(thread::spawn(move || {
            for stream in self.listener_v6.incoming() {
                if let Ok(stream) = stream {
                    debug!("[graphite] new peer at {:?} | local addr for peer {:?}", stream.peer_addr(), stream.local_addr());
                    let srv_chans = chans.clone();
                    let tags = tags.clone();
                    thread::spawn(move || handle_client(srv_chans, stream, tags));
                }
            }
        }));
        for jh in joins {
            // TODO Having sub-threads panic will not cause a bubble-up if that
            // thread is not the currently examined one. We're going to have to have
            // some manner of sub-thread communication going on.
            jh.join().expect("Uh oh, child thread paniced!");
        }
    }
}
