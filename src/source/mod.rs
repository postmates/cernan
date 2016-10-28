// TODO break server up into multiple modules which will allow the use of
// std::module_path! in our logging output

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

#[inline]
pub fn send<S>(ctx: S, chans: &mut Vec<mpsc::Sender<metric::Event>>, event: &metric::Event) where S: Into<String> {
    for mut chan in chans {
        let snd_time = Instant::now();
        chan.send(event);
        // NOTE this elapsed time is wrong! We must correctly include whole
        // seconds. STD makes this goofy, I believe chrono makes this simpler.
        trace!("[{}] channel send {:?} to {} elapsed (ns): {}", ctx, event, chan.name(), snd_time.elapsed().subsec_nanos());
    }
}

pub trait Source {
    fn run(&mut self) -> ();
}
