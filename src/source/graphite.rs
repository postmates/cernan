use metric;
use mpsc;
use std::io::prelude::*;
use std::io::BufReader;
use std::net::{Ipv6Addr, SocketAddrV6, SocketAddrV4, Ipv4Addr};
use std::net::{TcpListener, TcpStream};
use std::str;
use std::thread;
use std::time::Instant;

use super::{send, Source};

pub struct Graphite {
    chans: Vec<mpsc::Sender<metric::Event>>,
    port: u16,
    tags: metric::TagMap,
}

impl Graphite {
    pub fn new(chans: Vec<mpsc::Sender<metric::Event>>,
               port: u16,
               tags: metric::TagMap)
               -> Graphite {
        Graphite {
            chans: chans,
            port: port,
            tags: tags,
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
                debug!("[graphite] new peer at {:?} | local addr for peer {:?}",
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
                                        .overlay_tags_from_map(&tags);
                                    send("graphite", &mut chans, &metric::Event::Statsd(metric));
                                    for mut m in metrics {
                                        m = m.overlay_tags_from_map(&tags);
                                        send("graphite", &mut chans, &metric::Event::Graphite(m));
                                    }
                                    // NOTE this is wrong! See above NOTE.
                                    debug!("[graphite] payload handle effective, elapsed (ns): {}",
                                           pyld_hndl_time.elapsed().subsec_nanos());
                                }
                                None => {
                                    let metric = metric::Metric::new("cernan.graphite.bad_packet",
                                                                     1.0)
                                        .counter()
                                        .overlay_tags_from_map(&tags);
                                    send("graphite", &mut chans, &metric::Event::Statsd(metric));
                                    error!("[graphite] bad packet: {:?}", val);
                                    // NOTE this is wrong! See above NOTE.
                                    debug!("[graphite] payload handle failure, elapsed (ns): {}",
                                           pyld_hndl_time.elapsed().subsec_nanos());
                                }
                            }
                        })
                        .ok();
                }
                Err(_) => break,
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
        joins.push(thread::spawn(move || handle_tcp(chans_v6, tags_v6, listener_v6)));

        let addr_v4 = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), self.port);
        let listener_v4 = TcpListener::bind(addr_v4).expect("Unable to bind to TCP V4 socket");
        let chans_v4 = self.chans.clone();
        let tags_v4 = self.tags.clone();
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
