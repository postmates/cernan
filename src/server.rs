use std::net::{Ipv6Addr, UdpSocket, SocketAddrV6, SocketAddrV4, Ipv4Addr};
use std::net::{TcpListener, TcpStream};
use std::thread::sleep;
use std::time::Duration;
use std::thread;
use std::io::prelude::*;
use std::io::BufReader;
use std::str;
use metric;
use std::fs::File;
use std::io::SeekFrom;
use std::path::PathBuf;

use std::sync::mpsc::channel;
use notify::{RecommendedWatcher, Error, Watcher};
use notify::op::*;
use mpmc;

/// statsd
pub fn udp_server_v6(chan: mpmc::Sender, port: u16) {
    let addr = SocketAddrV6::new(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1), port, 0, 0);
    let socket = UdpSocket::bind(addr).ok().expect("Unable to bind to UDP socket");
    info!("statsd server started on ::1 {}", port);
    handle_udp(chan, socket);
}

pub fn udp_server_v4(chan: mpmc::Sender, port: u16) {
    let addr = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), port);
    let socket = UdpSocket::bind(addr).ok().expect("Unable to bind to UDP socket");
    info!("statsd server started on 127.0.0.1:{}", port);
    handle_udp(chan, socket);
}

pub fn handle_udp(mut chan: mpmc::Sender, socket: UdpSocket) {
    let mut buf = [0; 8192];
    loop {
        let (len, _) = match socket.recv_from(&mut buf) {
            Ok(r) => r,
            Err(_) => panic!("Could not read UDP socket."),
        };
        str::from_utf8(&buf[..len])
            .map(|val| {
                match metric::Metric::parse_statsd(val) {
                    Some(metrics) => {
                        for m in metrics {
                            chan.send(&metric::Event::Statsd(m)).expect("oops");
                        }
                        let metric = metric::Metric::counter("cernan.statsd.packet");
                        chan.send(&metric::Event::Statsd(metric)).unwrap();
                    }
                    None => {
                        let metric = metric::Metric::counter("cernan.statsd.bad_packet");
                        chan.send(&metric::Event::Statsd(metric)).unwrap();
                        error!("BAD PACKET: {:?}", val);
                    }
                }
            })
            .ok();
    }
}

pub fn file_server(mut chan: mpmc::Sender, path: PathBuf) {
    let (tx, rx) = channel();
    // NOTE on OSX fsevent will _not_ let us watch a file we don't own
    // effectively. See
    // https://developer.apple.com/library/mac/documentation/Darwin/Conceptual/FSEvents_ProgGuide/FileSystemEventSecurity/FileSystemEventSecurity.html
    // for more details. If we must properly support _all_ files on OSX we will
    // probably need to fall back to Pollwatcher for that operating system.
    let w: Result<RecommendedWatcher, Error> = Watcher::new(tx);

    let mut fp = File::open(&path).unwrap();
    fp.seek(SeekFrom::End(0)).expect("could not seek to end of file");
    let mut reader = BufReader::new(fp);

    match w {
        Ok(mut watcher) => {
            watcher.watch(&path).expect("could not set up watch for path");

            while let Ok(event) = rx.recv() {
                match event.op {
                    Ok(op) => {
                        if op.contains(CREATE) {
                            let fp = File::open(&path).unwrap();
                            reader = BufReader::new(fp);
                        }
                        if op.contains(WRITE) {
                            loop {
                                let mut line = String::new();
                                match reader.read_line(&mut line) {
                                    Ok(0) => break,
                                    Ok(_) => {
                                        let name = format!("{}.lines", path.to_str().unwrap());
                                        let metric = metric::Metric::counter(&name);
                                        chan.send(&metric::Event::Statsd(metric)).unwrap();
                                    },
                                    Err(err) => panic!(err)
                                }
                            }
                        }
                    }
                    Err(e) => panic!("Unknown file event error: {}", e)
                }
            }
        }
        Err(e) => panic!("Could not create file watcher: {}", e)
    }
}

pub fn tcp_server_ipv6(chan: mpmc::Sender, port: u16) {
    let addr = SocketAddrV6::new(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1), port, 0, 0);
    let listener = TcpListener::bind(addr).expect("Unable to bind to TCP socket");
    info!("graphite server started on ::1 {}", port);
    for stream in listener.incoming() {
        if let Ok(stream) = stream {
            let cchan = chan.clone();
            thread::spawn(move || {
                // connection succeeded
                handle_client(cchan, stream)
            });
        }
    }
}

pub fn tcp_server_ipv4(chan: mpmc::Sender, port: u16) {
    let addr = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), port);
    let listener = TcpListener::bind(addr).expect("Unable to bind to TCP socket");
    info!("graphite server started on 127.0.0.1:{}", port);
    for stream in listener.incoming() {
        if let Ok(stream) = stream {
            let cchan = chan.clone();
            thread::spawn(move || {
                // connection succeeded
                handle_client(cchan, stream)
            });
        }
    }
}

fn handle_client(mut chan: mpmc::Sender, stream: TcpStream) {
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
                                let metric = metric::Metric::counter("cernan.graphite.packet");
                                chan.send(&metric::Event::Statsd(metric)).unwrap();
                                for m in metrics {
                                    chan.send(&metric::Event::Graphite(m)).unwrap();
                                }
                            }
                            None => {
                                let metric = metric::Metric::counter("cernan.graphite.bad_packet");
                                chan.send(&metric::Event::Statsd(metric)).unwrap();
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
pub fn flush_timer_loop(mut chan: mpmc::Sender, interval: u64) {
    let duration = Duration::new(interval, 0);
    loop {
        sleep(duration);
        chan.send(&metric::Event::TimerFlush).expect("[FLUSH] Unable to write to chan!");
    }
}

// emit snapshot event into channel on a regular interval
//
// A snapshot indicates to supporting backends that it is time to generate a
// payload and store this in preparation for a future flush event.
pub fn snapshot_loop(mut chan: mpmc::Sender) {
    let duration = Duration::new(1, 0);
    loop {
        sleep(duration);
        chan.send(&metric::Event::Snapshot).expect("[SNAPSHOT] Unable to write to chan!");
    }
}
