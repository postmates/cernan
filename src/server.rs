use bincode::SizeLimit;
use std::net::{Ipv6Addr, UdpSocket, SocketAddrV6, SocketAddrV4, Ipv4Addr};
use std::net::{TcpListener, TcpStream, ToSocketAddrs};
use std::thread::sleep;
use std::time::Duration;
use std::thread;
use std::io::prelude::*;
use std::io::Take;
use std::str;
use metric;
use std::fs::File;
use std::io::{SeekFrom,BufReader};
use std::path::PathBuf;
use bincode::serde::{deserialize_from};

use std::sync::mpsc::channel;
use notify::{RecommendedWatcher, Error, Watcher};
use notify::op::*;
use mpsc;

use flate2::read::ZlibDecoder;

#[inline]
fn send(chans: &mut Vec<mpsc::Sender<metric::Event>>, event: &metric::Event) {
    for mut chan in chans {
        chan.send(event);
    }
}

/// statsd
pub fn udp_server_v6(chans: Vec<mpsc::Sender<metric::Event>>, port: u16) {
    let addr = SocketAddrV6::new(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1), port, 0, 0);
    let socket = UdpSocket::bind(addr).expect("Unable to bind to UDP socket");
    info!("statsd server started on ::1 {}", port);
    handle_udp(chans, socket);
}

pub fn udp_server_v4(chans: Vec<mpsc::Sender<metric::Event>>, port: u16) {
    let addr = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), port);
    let socket = UdpSocket::bind(addr).expect("Unable to bind to UDP socket");
    info!("statsd server started on 127.0.0.1:{}", port);
    handle_udp(chans, socket);
}

pub fn handle_udp(mut chans: Vec<mpsc::Sender<metric::Event>>, socket: UdpSocket) {
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
                        for m in metrics {
                            send(&mut chans, &metric::Event::Statsd(m));
                        }
                        let metric = metric::Metric::counter("cernan.statsd.packet");
                        send(&mut chans, &metric::Event::Statsd(metric));
                    }
                    None => {
                        let metric = metric::Metric::counter("cernan.statsd.bad_packet");
                        send(&mut chans, &metric::Event::Statsd(metric));
                        error!("BAD PACKET: {:?}", val);
                    }
                }
            })
            .ok();
    }
}

pub fn file_server(mut chans: Vec<mpsc::Sender<metric::Event>>, path: PathBuf) {
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
                            let mut lines = Vec::new();
                            loop {
                                let mut line = String::new();
                                match reader.read_line(&mut line) {
                                    Ok(0) => break,
                                    Ok(_) => {
                                        let name = format!("{}.lines", path.to_str().unwrap());
                                        let metric = metric::Metric::counter(&name);
                                        send(&mut chans, &metric::Event::Statsd(metric));
                                        lines.push(metric::LogLine::new(
                                            String::from(path.to_str().unwrap()),
                                            line,
                                        ));
                                    },
                                    Err(err) => panic!(err)
                                }
                            }
                            send(&mut chans, &metric::Event::Log(lines));
                        }
                    }
                    Err(e) => panic!("Unknown file event error: {}", e),
                }
            }
        }
        Err(e) => panic!("Could not create file watcher: {}", e),
    }
}

//
// cernan crd_receiver sink
//
pub fn receiver_sink_server(chans: Vec<mpsc::Sender<metric::Event>>, ip: &String, port: u16) {
    let srv: Vec<_> = (ip.as_str(), port).to_socket_addrs().expect("unable to make socket addr").collect();
    let listener = TcpListener::bind(srv.first().unwrap()).expect("Unable to bind to TCP socket");
    for stream in listener.incoming() {
        if let Ok(stream) = stream {
            stream.set_nonblocking(false).expect("could not set TcpStream to block");
            let srv_chans = chans.clone();
            thread::spawn(move || handle_receiver_client(srv_chans, stream));
        }
    }
}

#[inline]
fn u8tou32abe(v: &[u8]) -> u32 {
    (v[3] as u32) + ((v[2] as u32) << 8) + ((v[1] as u32) << 24) + ((v[0] as u32) << 16)
}

fn handle_receiver_client(mut chans: Vec<mpsc::Sender<metric::Event>>, stream: TcpStream) {
    let mut sz_buf = [0; 4];
    let mut reader = BufReader::new(stream);
    match reader.read_exact(&mut sz_buf) {
        Ok(()) => {
            let payload_size_in_bytes = u8tou32abe(&sz_buf);
            let hndl = (&mut reader).take(payload_size_in_bytes as u64);
            let mut e = ZlibDecoder::new(hndl);
            match deserialize_from::<ZlibDecoder<Take<&mut BufReader<TcpStream>>>, Vec<metric::Event>>(&mut e, SizeLimit::Infinite) {
                Ok(events) => {
                    for ev in events {
                        send(&mut chans, &ev);
                    }
                }
                Err(e) => panic!("Failed decoding. Skipping {:?}", e),
            }
        }
        Err(e) => panic!("Unable to read payload: {:?}", e),
    }
}

//
// graphite
//
pub fn tcp_server_ipv6(chans: Vec<mpsc::Sender<metric::Event>>, port: u16) {
    let addr = SocketAddrV6::new(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1), port, 0, 0);
    let listener = TcpListener::bind(addr).expect("Unable to bind to TCP socket");
    info!("graphite server started on ::1 {}", port);
    for stream in listener.incoming() {
        if let Ok(stream) = stream {
            let srv_chans = chans.clone();
            thread::spawn(move || handle_client(srv_chans, stream));
        }
    }
}

pub fn tcp_server_ipv4(chans: Vec<mpsc::Sender<metric::Event>>, port: u16) {
    let addr = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), port);
    let listener = TcpListener::bind(addr).expect("Unable to bind to TCP socket");
    info!("graphite server started on 127.0.0.1:{}", port);
    for stream in listener.incoming() {
        if let Ok(stream) = stream {
            let srv_chans = chans.clone();
            thread::spawn(move || handle_client(srv_chans, stream));
        }
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
                        trace!("graphite - {}", val);
                        match metric::Metric::parse_graphite(val) {
                            Some(metrics) => {
                                let metric = metric::Metric::counter("cernan.graphite.packet");
                                send(&mut chans, &metric::Event::Statsd(metric));
                                for m in metrics {
                                    send(&mut chans, &metric::Event::Graphite(m));
                                }
                            }
                            None => {
                                let metric = metric::Metric::counter("cernan.graphite.bad_packet");
                                send(&mut chans, &metric::Event::Statsd(metric));
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
pub fn flush_timer_loop(mut chans: Vec<mpsc::Sender<metric::Event>>, interval: u64) {
    let duration = Duration::new(interval, 0);
    loop {
        sleep(duration);
        send(&mut chans, &metric::Event::TimerFlush);
    }
}
