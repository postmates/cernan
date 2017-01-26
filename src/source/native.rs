use super::Source;
use byteorder::{BigEndian, ReadBytesExt};
use hopper;
use metric;
use protobuf;
use protocols::native::{AggregationMethod, Payload};
use std::io;
use std::io::Read;
use std::net::{TcpListener, TcpStream, ToSocketAddrs};
use std::str;
use std::thread;
use util;

pub struct NativeServer {
    chans: util::Channel,
    ip: String,
    port: u16,
    tags: metric::TagMap,
}

#[derive(Debug)]
pub struct NativeServerConfig {
    pub ip: String,
    pub port: u16,
    pub tags: metric::TagMap,
    pub forwards: Vec<String>,
    pub config_path: String,
}

impl NativeServer {
    pub fn new(chans: Vec<hopper::Sender<metric::Event>>,
               config: NativeServerConfig)
               -> NativeServer {
        NativeServer {
            chans: chans,
            ip: config.ip,
            port: config.port,
            tags: config.tags,
        }
    }
}

fn handle_tcp(chans: util::Channel,
              tags: metric::TagMap,
              listner: TcpListener)
              -> thread::JoinHandle<()> {
    thread::spawn(move || for stream in listner.incoming() {
        if let Ok(stream) = stream {
            debug!("new peer at {:?} | local addr for peer {:?}",
                   stream.peer_addr(),
                   stream.local_addr());
            let tags = tags.clone();
            let chans = chans.clone();
            thread::spawn(move || { handle_stream(chans, tags, stream); });
        }
    })
}

fn handle_stream(mut chans: util::Channel, tags: metric::TagMap, stream: TcpStream) {
    thread::spawn(move || {
        let mut reader = io::BufReader::new(stream);
        let mut buf = Vec::with_capacity(4000);
        loop {
            let payload_size_in_bytes = match reader.read_u32::<BigEndian>() {
                Ok(i) => i as usize,
                Err(_) => return,
            };
            buf.resize(payload_size_in_bytes, 0);
            if reader.read_exact(&mut buf).is_err() {
                return;
            }
            match protobuf::parse_from_bytes::<Payload>(&buf) {
                Ok(pyld) => {
                    for point in pyld.get_points() {
                        let name: &str = point.get_name();
                        let smpls: &[f64] = point.get_samples();
                        let aggr_type: AggregationMethod = point.get_method();
                        let meta = point.get_metadata();
                        // FIXME #166
                        let ts: i64 = (point.get_timestamp_ms() as f64 * 0.001) as i64;

                        if smpls.is_empty() {
                            continue;
                        }
                        let mut metric = metric::Telemetry::new(name, smpls[0]);
                        for smpl in &smpls[1..] {
                            metric = metric.insert_value(*smpl);
                        }
                        metric = match aggr_type {
                            AggregationMethod::SET => metric.aggr_set(),
                            AggregationMethod::SUM => metric.aggr_sum(),
                            AggregationMethod::SUMMARIZE => metric.aggr_summarize(),
                        };
                        metric = if point.get_persisted() {
                            metric.persist()
                        } else {
                            metric.ephemeral()
                        };
                        metric = metric.timestamp(ts);
                        metric = metric.overlay_tags_from_map(&tags);
                        for mt in meta {
                            metric = metric.overlay_tag(mt.get_key(), mt.get_value());
                        }
                        util::send("native", &mut chans, metric::Event::new_telemetry(metric));
                    }
                    for line in pyld.get_lines() {
                        let path: &str = line.get_path();
                        let value: &str = line.get_value();
                        let meta = line.get_metadata();
                        // FIXME #166
                        let ts: i64 = (line.get_timestamp_ms() as f64 * 0.001) as i64;

                        let mut logline = metric::LogLine::new(path, value);
                        logline = logline.time(ts);
                        logline = logline.overlay_tags_from_map(&tags);
                        for mt in meta {
                            logline = logline.overlay_tag(mt.get_key(), mt.get_value());
                        }
                        util::send("native", &mut chans, metric::Event::new_log(logline));

                    }
                }
                Err(err) => {
                    trace!("Unable to read payload: {:?}", err);
                    return;
                }
            }
        }
    });
}


impl Source for NativeServer {
    fn run(&mut self) {
        let srv: Vec<_> = (self.ip.as_str(), self.port)
            .to_socket_addrs()
            .expect("unable to make socket addr")
            .collect();
        let listener = TcpListener::bind(srv.first().unwrap())
            .expect("Unable to bind to TCP socket");
        let chans = self.chans.clone();
        let tags = self.tags.clone();
        info!("server started on {}:{}", self.ip, self.port);
        let jh = thread::spawn(move || handle_tcp(chans, tags, listener));

        jh.join().expect("Uh oh, child thread paniced!");
    }
}
