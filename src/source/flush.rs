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

pub struct FlushTimer {
    chans: Vec<mpsc::Sender<metric::Event>>,
    interval: u64,
}

impl FlushTimer {
    pub fn new(chans: Vec<mpsc::Sender<metric::Event>>, interval: u64) {
        FlushTimer {
            chans: chans,
            interval: interval,
        }
    }
}

impl Source for FlushTimer {
    fn run(&mut self) {
        let duration = Duration::new(interval, 0);
        loop {
            sleep(duration);
            send("flush", &mut chans, &metric::Event::TimerFlush);
        }
    }
}
