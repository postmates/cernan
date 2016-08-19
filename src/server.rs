use std::sync::mpsc::Sender;
use std::net::{Ipv6Addr, UdpSocket, SocketAddrV6, SocketAddrV4, Ipv4Addr};
use std::net::{TcpListener, TcpStream};
use std::thread::sleep;
use std::time::Duration;
use std::thread;
use std::io::prelude::*;
use std::io::BufReader;
use std::sync::Arc;
use std::str;
use metric;

pub enum Event {
    Statsd(Vec<Arc<metric::Metric>>),
    Graphite(Vec<Arc<metric::Metric>>),
    TimerFlush,
    Snapshot,
}

/// statsd
pub fn udp_server_v6(chan: Sender<Event>, port: u16) {
    let addr = SocketAddrV6::new(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1), port, 0, 0);
    let socket = UdpSocket::bind(addr).ok().expect("Unable to bind to UDP socket");
    info!("statsd server started on ::1 {}", port);
    let mut buf = [0; 8192];
    loop {
        let (len, _) = match socket.recv_from(&mut buf) {
            Ok(r) => r,
            Err(_) => panic!("Could not read UDP socket."),
        };
        str::from_utf8(&buf[..len])
            .map(|val| {
                trace!("statsd - {}", val);
                match metric::Metric::parse_statsd(val) {
                    Some(metrics) => {
                        chan.send(Event::Statsd(metrics)).unwrap();
                    }
                    None => {
                        error!("BAD PACKET: {:?}", val);
                    }
                }
            })
            .ok();
    }
}

pub fn udp_server_v4(chan: Sender<Event>, port: u16) {
    let addr = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), port);
    let socket = UdpSocket::bind(addr).ok().expect("Unable to bind to UDP socket");
    info!("statsd server started on 127.0.0.1:{}", port);
    let mut buf = [0; 8192];
    loop {
        let (len, _) = match socket.recv_from(&mut buf) {
            Ok(r) => r,
            Err(_) => panic!("Could not read UDP socket."),
        };
        str::from_utf8(&buf[..len])
            .map(|val| {
                trace!("statsd - {}", val);
                match metric::Metric::parse_statsd(val) {
                    Some(metrics) => {
                        chan.send(Event::Statsd(metrics)).unwrap();
                    }
                    None => {
                        error!("BAD PACKET: {:?}", val);
                    }
                }
            })
            .ok();
    }
}

pub fn tcp_server_ipv6(chan: Sender<Event>, port: u16) {
    let addr = SocketAddrV6::new(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1), port, 0, 0);
    let listener = TcpListener::bind(addr).expect("Unable to bind to TCP socket");
    info!("graphite server started on ::1 {}", port);
    for stream in listener.incoming() {
        let newchan = chan.clone();
        if let Ok(stream) = stream {
            thread::spawn(move || {
                // connection succeeded
                handle_client(newchan, stream)
            });
        }
    }
}

pub fn tcp_server_ipv4(chan: Sender<Event>, port: u16) {
    let addr = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), port);
    let listener = TcpListener::bind(addr).expect("Unable to bind to TCP socket");
    info!("graphite server started on 127.0.0.1:{}", port);
    for stream in listener.incoming() {
        let newchan = chan.clone();
        if let Ok(stream) = stream {
            thread::spawn(move || {
                // connection succeeded
                handle_client(newchan, stream)
            });
        }
    }
}

fn handle_client(chan: Sender<Event>, stream: TcpStream) {
    let line_reader = BufReader::new(stream);
    for line in line_reader.lines() {
        match line {
            Ok(line) => {
                let buf = line.into_bytes();
                str::from_utf8(&buf)
                    .map(|val| {
                        debug!("graphite - {}", val);
                        match metric::Metric::parse_graphite(val) {
                            Some(metrics) => {
                                chan.send(Event::Graphite(metrics)).unwrap();
                            }
                            None => {
                                error!("BAD PACKET: {:?}", val);
                            }
                        }
                    })
                    .ok();
            }
            Err(_) => break,
        }
    }
}

// emit flush event into channel on a regular interval
pub fn flush_timer_loop(chan: Sender<Event>, interval: u64) {
    let duration = Duration::new(interval, 0);
    loop {
        sleep(duration);
        chan.send(Event::TimerFlush).expect("[FLUSH] Unable to write to chan!");
    }
}

// emit snapshot event into channel on a regular interval
//
// A snapshot indicates to supporting backends that it is time to generate a
// payload and store this in preparation for a future flush event.
pub fn snapshot_loop(chan: Sender<Event>) {
    let duration = Duration::new(1, 0);
    loop {
        sleep(duration);
        chan.send(Event::Snapshot).expect("[SNAPSHOT] Unable to write to chan!");
    }
}
