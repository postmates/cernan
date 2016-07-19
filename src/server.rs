use std::sync::mpsc::Sender;
use std::net::{Ipv4Addr, UdpSocket, SocketAddrV4};
use std::net::{TcpListener, TcpStream};
use std::thread::sleep;
use std::time::Duration;
use std::thread;
use std::io::prelude::*;
use std::io::BufReader;
use std::str;
use std::sync::Arc;

use metric;

pub enum Event {
    Graphite(Vec<Arc<metric::Metric>>),
    Statsd(Vec<Arc<metric::Metric>>),
    TimerFlush,
}

// flush timer
pub fn flush_timer_loop(chan: Sender<Event>, interval: u64) {
    let duration = Duration::new(interval, 0);
    loop {
        sleep(duration);
        chan.send(Event::TimerFlush).unwrap();
    }
}

// statsd
pub fn udp_server(chan: Sender<Event>, port: u16) {
    let addr = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), port);
    let socket = UdpSocket::bind(addr).ok().unwrap();
    info!("statsd server started on 127.0.0.1:{}", port);
    let mut buf = [0; 8192];
    loop {
        match socket.recv_from(&mut buf) {
            Ok(r) => r,
            Err(_) => panic!("Could not read UDP socket."),
        };
        str::from_utf8(&buf)
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
            }).ok();
    }
}

// graphite
pub fn tcp_server(chan: Sender<Event>, port: u16) {
    let addr = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), port);
    let listener = TcpListener::bind(addr).unwrap();
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
                    }).ok();
            }
            Err(_) => break,
        }
    }
}
