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
use std::time::Duration;

type HashMapFnv<K, V> = HashMap<K, V, BuildHasherDefault<FnvHasher>>;

#[inline]
fn send(chans: &mut Vec<mpsc::Sender<metric::Event>>, event: &metric::Event) {
    for mut chan in chans {
        chan.send(event);
    }
}

// STATSD
//

pub fn udp_server_v6(chans: Vec<mpsc::Sender<metric::Event>>, port: u16, tags: metric::TagMap) {
    let addr = SocketAddrV6::new(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1), port, 0, 0);
    let socket = UdpSocket::bind(addr).expect("Unable to bind to UDP socket");
    info!("statsd server started on ::1 {}", port);
    handle_udp(chans, socket, tags);
}

pub fn udp_server_v4(chans: Vec<mpsc::Sender<metric::Event>>, port: u16, tags: metric::TagMap) {
    let addr = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), port);
    let socket = UdpSocket::bind(addr).expect("Unable to bind to UDP socket");
    info!("statsd server started on 127.0.0.1:{}", port);
    handle_udp(chans, socket, tags);
}

pub fn handle_udp(mut chans: Vec<mpsc::Sender<metric::Event>>,
                  socket: UdpSocket,
                  tags: metric::TagMap) {
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
                        for mut m in metrics {
                            m = m.overlay_tags_from_map(&tags);
                            send(&mut chans, &metric::Event::Statsd(m));
                        }
                        let mut metric = metric::Metric::new("cernan.statsd.packet", 1.0).counter();
                        metric = metric.overlay_tags_from_map(&tags);
                        send(&mut chans, &metric::Event::Statsd(metric));
                    }
                    None => {
                        let mut metric = metric::Metric::new("cernan.statsd.bad_packet", 1.0)
                            .counter();
                        metric = metric.overlay_tags_from_map(&tags);
                        send(&mut chans, &metric::Event::Statsd(metric));
                        error!("BAD PACKET: {:?}", val);
                    }
                }
            })
            .ok();
    }
}

// FILE
//

pub fn file_server(mut chans: Vec<mpsc::Sender<metric::Event>>,
                   path: PathBuf,
                   tags: metric::TagMap) {
    let (tx, rx) = channel();
    // NOTE on OSX fsevent will _not_ let us watch a file we don't own
    // effectively. See
    // https://developer.apple.com/library/mac/documentation/Darwin/Conceptual/FSEvents_ProgGuide/FileSystemEventSecurity/FileSystemEventSecurity.html
    // for more details. If we must properly support _all_ files on OSX we will
    // probably need to fall back to Pollwatcher for that operating system.
    let w: Result<RecommendedWatcher, Error> = Watcher::new(tx);

    let mut fp_map: HashMapFnv<PathBuf, BufReader<File>> = HashMapFnv::default();

    match w {
        Ok(mut watcher) => {
            watcher.watch(&path).expect("could not set up watch for path");

            while let Ok(event) = rx.recv() {
                match event.op {
                    Ok(op) => {
                        if let Some(path) = event.path {
                            trace!("OP: {:?} | PATH: {:?} | FP_MAP: {:?}", op, path, fp_map);
                            let mut lines = Vec::new();
                            if op.contains(REMOVE) || op.contains(RENAME) {
                                fp_map.remove(&path);
                            }
                            if op.contains(WRITE) && !fp_map.contains_key(&path) {
                                let _ = File::open(&path).map(|fp| {
                                    let mut reader = BufReader::new(fp);
                                    reader.seek(SeekFrom::End(0))
                                        .expect("could not seek to end of file");
                                    fp_map.insert(path.clone(), reader);
                                });
                            }
                            if op.contains(WRITE) {
                                if let Some(rdr) = fp_map.get_mut(&path) {
                                    loop {
                                        for line in rdr.lines() {
                                            trace!("PATH: {:?} | LINE: {:?}", path, line);
                                            lines.push(metric::LogLine::new(path.to_str()
                                                                                .unwrap(),
                                                                            line.unwrap(),
                                                                            tags.clone()));
                                        }
                                        if lines.is_empty() {
                                            trace!("EMPTY LINES, SEEK TO START: {:?}", path);
                                            rdr.seek(SeekFrom::Start(0))
                                                .expect("could not seek to start of file");
                                            continue;
                                        } else {
                                            break;
                                        }
                                    }
                                }
                            }
                            if !lines.is_empty() {
                                send(&mut chans, &metric::Event::Log(lines));
                            }
                        }
                    }
                    Err(e) => panic!("Unknown file event error: {}", e),
                }
            }
        }
        Err(e) => panic!("Could not create file watcher: {}", e),
    }
}

// FEDERATION_RECEIVER
//

pub fn receiver_sink_server(chans: Vec<mpsc::Sender<metric::Event>>,
                            ip: &str,
                            port: u16,
                            tags: metric::TagMap) {
    let srv: Vec<_> = (ip, port).to_socket_addrs().expect("unable to make socket addr").collect();
    let listener = TcpListener::bind(srv.first().unwrap()).expect("Unable to bind to TCP socket");
    for stream in listener.incoming() {
        if let Ok(stream) = stream {
            stream.set_nonblocking(false).expect("could not set TcpStream to block");
            let srv_chans = chans.clone();
            let tags = tags.clone();
            thread::spawn(move || handle_receiver_client(srv_chans, stream, tags));
        }
    }
}

#[inline]
fn u8tou32abe(v: &[u8]) -> u32 {
    (v[3] as u32) + ((v[2] as u32) << 8) + ((v[1] as u32) << 24) + ((v[0] as u32) << 16)
}

fn handle_receiver_client(mut chans: Vec<mpsc::Sender<metric::Event>>,
                          stream: TcpStream,
                          tags: metric::TagMap) {
    let mut sz_buf = [0; 4];
    let mut reader = BufReader::new(stream);
    match reader.read_exact(&mut sz_buf) {
        Ok(()) => {
            let payload_size_in_bytes = u8tou32abe(&sz_buf);
            let hndl = (&mut reader).take(payload_size_in_bytes as u64);
            let mut e = ZlibDecoder::new(hndl);
            match deserialize_from::<ZlibDecoder<Take<&mut BufReader<TcpStream>>>,
                                     Vec<metric::Event>>(&mut e, SizeLimit::Infinite) {
                Ok(events) => {
                    for mut ev in events {
                        trace!("FED RECV PRE-EVENT: {:?}", ev);
                        ev = match ev {
                            metric::Event::Statsd(m) => {
                                metric::Event::Statsd(m.merge_tags_from_map(&tags))
                            }
                            metric::Event::Graphite(m) => {
                                metric::Event::Graphite(m.merge_tags_from_map(&tags))
                            }
                            _ => continue, // we refuse to accept any non-telemetry forward for now
                        };
                        trace!("FED RECV POST-EVENT: {:?}", ev);
                        send(&mut chans, &ev);
                    }
                    let metric = metric::Metric::new("cernan.federation.receiver.packet", 1.0)
                        .counter()
                        .overlay_tags_from_map(&tags);;
                    send(&mut chans, &metric::Event::Statsd(metric));
                }
                Err(e) => panic!("Failed decoding. Skipping {:?}", e),
            }
        }
        Err(e) => trace!("Unable to read payload: {:?}", e),
    }
}

// GRAPHITE
//

pub fn tcp_server_ipv6(chans: Vec<mpsc::Sender<metric::Event>>, port: u16, tags: metric::TagMap) {
    let addr = SocketAddrV6::new(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1), port, 0, 0);
    let listener = TcpListener::bind(addr).expect("Unable to bind to TCP socket");
    info!("graphite server started on ::1 {}", port);
    for stream in listener.incoming() {
        if let Ok(stream) = stream {
            let srv_chans = chans.clone();
            let tags = tags.clone();
            thread::spawn(move || handle_client(srv_chans, stream, tags));
        }
    }
}

pub fn tcp_server_ipv4(chans: Vec<mpsc::Sender<metric::Event>>, port: u16, tags: metric::TagMap) {
    let addr = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), port);
    let listener = TcpListener::bind(addr).expect("Unable to bind to TCP socket");
    info!("graphite server started on 127.0.0.1:{}", port);
    for stream in listener.incoming() {
        if let Ok(stream) = stream {
            let srv_chans = chans.clone();
            let tags = tags.clone();
            thread::spawn(move || handle_client(srv_chans, stream, tags));
        }
    }
}

fn handle_client(mut chans: Vec<mpsc::Sender<metric::Event>>,
                 stream: TcpStream,
                 tags: metric::TagMap) {
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
                                let metric = metric::Metric::new("cernan.graphite.packet", 1.0)
                                    .counter()
                                    .overlay_tags_from_map(&tags);;
                                send(&mut chans, &metric::Event::Statsd(metric));
                                for mut m in metrics {
                                    m = m.overlay_tags_from_map(&tags);
                                    send(&mut chans, &metric::Event::Graphite(m));
                                }
                            }
                            None => {
                                let metric = metric::Metric::new("cernan.graphite.bad_packet", 1.0)
                                    .counter()
                                    .overlay_tags_from_map(&tags);
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

// FLUSH
//

pub fn flush_timer_loop(mut chans: Vec<mpsc::Sender<metric::Event>>, interval: u64) {
    let duration = Duration::new(interval, 0);
    loop {
        sleep(duration);
        send(&mut chans, &metric::Event::TimerFlush);
    }
}
