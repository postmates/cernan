use metric;
use mpsc;
use std::net::{Ipv6Addr, UdpSocket, SocketAddrV6, SocketAddrV4, Ipv4Addr};
use std::str;
use std::thread;
use std::time::Instant;

use super::send;
use source::Source;

pub struct Statsd {
    chans: Vec<mpsc::Sender<metric::Event>>,
    port: u16,
    tags: metric::TagMap,
}

impl Statsd {
    pub fn new(chans: Vec<mpsc::Sender<metric::Event>>, port: u16, tags: metric::TagMap) -> Statsd {
        Statsd {
            chans: chans,
            port: port,
            tags: tags,
        }
    }
}

fn handle_udp(mut chans: Vec<mpsc::Sender<metric::Event>>,
              tags: metric::TagMap,
              socket: UdpSocket) {
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
                        let mut metric = metric::Metric::new("cernan.statsd.packet", 1.0).counter();
                        metric = metric.overlay_tags_from_map(&tags);
                        send("statsd", &mut chans, &metric::Event::Statsd(metric));
                        // NOTE this is wrong! See above NOTE.
                        debug!("[statsd] payload handle effective, elapsed (ns): {}",
                               pyld_hndl_time.elapsed().subsec_nanos());
                    }
                    None => {
                        let mut metric = metric::Metric::new("cernan.statsd.bad_packet", 1.0)
                            .counter();
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
}

impl Source for Statsd {
    fn run(&mut self) {
        let mut joins = Vec::new();

        let addr_v6 = SocketAddrV6::new(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1), self.port, 0, 0);
        let socket_v6 = UdpSocket::bind(addr_v6).expect("Unable to bind to UDP V6 socket");
        let chans_v6 = self.chans.clone();
        let tags_v6 = self.tags.clone();
        joins.push(thread::spawn(move || handle_udp(chans_v6, tags_v6, socket_v6)));

        // let addr_v4 = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), self.port);
        // let socket_v4 = UdpSocket::bind(addr_v4).expect("Unable to bind to UDP socket");
        // let chans_v4 = self.chans.clone();
        // let tags_v4 = self.tags.clone();
        // joins.push(thread::spawn(move || handle_udp(chans_v4, tags_v4, socket_v4)));

        for jh in joins {
            // TODO Having sub-threads panic will not cause a bubble-up if that
            // thread is not the currently examined one. We're going to have to have
            // some manner of sub-thread communication going on.
            jh.join().expect("Uh oh, child thread paniced!");
        }
    }
}
