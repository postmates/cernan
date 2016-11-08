use bincode::SizeLimit;
use bincode::serde::serialize_into;
use metric;
use mpsc;
use sink::Sink;
use std::io::Write;
use std::net::{TcpStream, ToSocketAddrs};
use time;

use flate2::Compression;
use flate2::write::ZlibEncoder;

pub struct FederationTransmitter {
    port: u16,
    host: String,
    buffer: Vec<metric::Event>,
}

#[derive(Debug)]
pub struct FederationTransmitterConfig {
    pub port: u16,
    pub host: String,
}

impl FederationTransmitter {
    pub fn new(config: FederationTransmitterConfig) -> FederationTransmitter {
        FederationTransmitter {
            port: config.port,
            host: config.host,
            buffer: Vec::new(),
        }
    }
}

impl Default for FederationTransmitter {
    fn default() -> Self {
        FederationTransmitter {
            port: 1972,
            host: String::from("127.0.0.1"),
            buffer: Vec::new(),
        }
    }
}

impl Sink for FederationTransmitter {
    fn deliver(&mut self, _: metric::Metric) {
        // intentionally nothing
    }

    fn deliver_lines(&mut self, _: Vec<metric::LogLine>) {
        // intentionally nothing
    }

    fn run(&mut self, mut recv: mpsc::Receiver<metric::Event>) {
        let mut attempts = 0;
        loop {
            time::delay(attempts);
            match recv.next() {
                None => attempts += 1,
                Some(event) => {
                    attempts = 0;
                    match event {
                        metric::Event::TimerFlush => self.flush(),
                        _ => self.buffer.push(event),
                    }
                }
            }
        }
    }

    fn flush(&mut self) {
        let mut e = ZlibEncoder::new(Vec::new(), Compression::Default);
        serialize_into(&mut e, &self.buffer, SizeLimit::Infinite).expect("could not serialize");
        let mut t = e.finish().expect("unable to finish compression write");
        let pyld_sz_bytes: [u8; 4] = u32tou8abe(t.len() as u32);
        t.insert(0, pyld_sz_bytes[0]);
        t.insert(0, pyld_sz_bytes[1]);
        t.insert(0, pyld_sz_bytes[2]);
        t.insert(0, pyld_sz_bytes[3]);

        let srv: Vec<_> = (self.host.as_str(), self.port)
            .to_socket_addrs()
            .expect("Unable to resolve domain")
            .collect();
        match TcpStream::connect(srv.first().unwrap()) {
            Ok(mut stream) => {
                let res = stream.write(&t[..]);
                if res.is_ok() {
                    self.buffer.clear();
                }
            }
            Err(e) => debug!("Unable to connect: {}", e),
        }
    }
}

#[inline]
fn u32tou8abe(v: u32) -> [u8; 4] {
    [v as u8, (v >> 8) as u8, (v >> 24) as u8, (v >> 16) as u8]
}
