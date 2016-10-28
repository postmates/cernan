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

use super::send;
use source::Source;

pub struct Statsd {
    chans: Vec<mpsc::Sender<metric::Event>>,
    socket_v6: UdpSocket,
    socket_v4: UdpSocket,
    tags: metric::TagMap,
}

impl Statsd {
    pub fn new(chans: Vec<mpsc::Sender<metric::Event>>, port: u16, tags: metric::TagMap) -> Statsd {
        let addr_v6 = SocketAddrV6::new(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1), port, 0, 0);
        let socket_v6 = UdpSocket::bind(addr_v6).expect("Unable to bind to UDP socket");
        let addr_v4 = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), port);
        let socket_v4 = UdpSocket::bind(addr_v4).expect("Unable to bind to UDP socket");
        Statsd {
            chans: chans,
            socket_v4: socket_v4,
            socket_v6: socket_v6,
            tags: tags,
        }
    }

    fn handle_udp(&mut self, socket: UdpSocket) -> thread::JoinHandle<u64> {
        // TODO put these fields in an ARC to avoid having to clone them,
        // similar story for all other sources
        let tags = self.tags.clone();
        let chans = self.chans.clone();
        thread::spawn(move || {
            let mut buf = [0; 8192];
            loop {
                let recv_time = Instant::now();
                let (len, _) = match socket.recv_from(&mut buf) {
                    Ok(r) => r,
                    Err(_) => panic!("Could not read UDP socket."),
                };
                // NOTE this elapsed time is wrong! We must correctly include whole
                // seconds. STD makes this goofy, I believe chrono makes this simpler.
                debug!("[statsd] recv time elapsed (ns): {}",
                       recv_time.elapsed().subsec_nanos());
                str::from_utf8(&buf[..len])
                    .map(|val| {
                        trace!("[statsd] {}", val);
                        let pyld_hndl_time = Instant::now();
                        match metric::Metric::parse_statsd(val) {
                            Some(metrics) => {
                                for mut m in metrics {
                                    m = m.overlay_tags_from_map(&tags);
                                    send("statsd", &mut chans, &metric::Event::Statsd(m));
                                }
                                let mut metric = metric::Metric::new("cernan.statsd.packet", 1.0)
                                    .counter();
                                metric = metric.overlay_tags_from_map(&tags);
                                send("statsd", &mut chans, &metric::Event::Statsd(metric));
                                // NOTE this is wrong! See above NOTE.
                                debug!("[statsd] payload handle effective, elapsed (ns): {}",
                                       pyld_hndl_time.elapsed().subsec_nanos());
                            }
                            None => {
                                let mut metric =
                                    metric::Metric::new("cernan.statsd.bad_packet", 1.0).counter();
                                metric = metric.overlay_tags_from_map(&tags);
                                send("statsd", &mut chans, &metric::Event::Statsd(metric));
                                error!("BAD PACKET: {:?}", val);
                                // NOTE this is wrong! See above NOTE.
                                debug!("[statsd] payload handle failure, elapsed (ns): {}",
                                       pyld_hndl_time.elapsed().subsec_nanos());
                            }
                        }
                    })
                    .ok();
            }
        })
    }
}

impl Source for Statsd {
    fn run(&mut self) {
        let mut joins = Vec::new();

        joins.push(self.handle_udp(self.socket_v4));
        joins.push(self.handle_udp(self.socket_v6));

        for jh in joins {
            // TODO Having sub-threads panic will not cause a bubble-up if that
            // thread is not the currently examined one. We're going to have to have
            // some manner of sub-thread communication going on.
            jh.join().expect("Uh oh, child thread paniced!");
        }
    }
}
