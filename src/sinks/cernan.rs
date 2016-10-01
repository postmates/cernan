use bincode::SizeLimit;
use bincode::serde::serialize;
use metric;
use mpsc;
use sink::Sink;
use std::io::Write;
use std::net::{TcpStream,ToSocketAddrs};

pub struct Cernan {
    buffer: Vec<metric::Event>,
}

impl Cernan {
    pub fn new() -> Cernan {
        Cernan { buffer: Vec::new() }
    }
}

impl Default for Cernan {
    fn default() -> Self {
        Self::new()
    }
}

impl Sink for Cernan {
    fn deliver(&mut self, _: metric::Metric) {
        // intentionally nothing
    }

    fn deliver_lines(&mut self, _: Vec<metric::LogLine>) {
        // intentionally nothing
    }

    fn run(&mut self, recv: mpsc::Receiver<metric::Event>) {
        for event in recv {
            match event {
                metric::Event::TimerFlush => self.flush(),
                _ => self.buffer.push(event),
            }
        }
    }

    fn flush(&mut self) {
        let mut t = serialize(&self.buffer, SizeLimit::Infinite).expect("could not serialize");
        let pyld_sz_bytes: [u8; 4] = u32tou8abe(t.len() as u32);
        t.insert(0, pyld_sz_bytes[0]);
        t.insert(0, pyld_sz_bytes[1]);
        t.insert(0, pyld_sz_bytes[2]);
        t.insert(0, pyld_sz_bytes[3]);
        let srv: Vec<_> = "127.0.0.1:1972".to_socket_addrs()
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
