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
    telemetry_error: f64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct NativeServerConfig {
    pub ip: String,
    pub port: u16,
    pub tags: metric::TagMap,
    pub forwards: Vec<String>,
    pub config_path: Option<String>,
    pub telemetry_error: f64,
}

impl Default for NativeServerConfig {
    fn default() -> Self {
        NativeServerConfig {
            ip: "0.0.0.0".to_string(),
            port: 1972,
            tags: metric::TagMap::default(),
            forwards: Vec::default(),
            config_path: None,
            telemetry_error: 0.001,
        }
    }
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
            telemetry_error: config.telemetry_error,
        }
    }
}

fn handle_tcp(chans: util::Channel,
              tags: metric::TagMap,
              listner: TcpListener,
              telemetry_error: f64)
              -> thread::JoinHandle<()> {
    thread::spawn(move || for stream in listner.incoming() {
                      if let Ok(stream) = stream {
                          debug!("new peer at {:?} | local addr for peer {:?}",
                                 stream.peer_addr(),
                                 stream.local_addr());
                          let tags = tags.clone();
                          let chans = chans.clone();
                          thread::spawn(move || {
                                            handle_stream(chans, tags, stream, telemetry_error);
                                        });
                      }
                  })
}

fn handle_stream(mut chans: util::Channel,
                 tags: metric::TagMap,
                 stream: TcpStream,
                 telemetry_error: f64) {
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
                Ok(mut pyld) => {
                    for mut point in pyld.take_points().into_iter() {
                        let name: String = point.take_name();
                        let smpls: Vec<f64> = point.take_samples();
                        let aggr_type: AggregationMethod = point.get_method();
                        let mut meta = point.take_metadata();
                        // FIXME #166
                        let ts: i64 = (point.get_timestamp_ms() as f64 * 0.001) as i64;

                        if smpls.is_empty() {
                            continue;
                        }
                        let mut metric = metric::Telemetry::new(name, smpls[0], telemetry_error);
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
                        for (key, value) in meta.drain() {
                            metric = metric.overlay_tag(key, value);
                        }
                        util::send(&mut chans, metric::Event::new_telemetry(metric));
                    }
                    for mut line in pyld.take_lines().into_iter() {
                        let path: String = line.take_path();
                        let value: String = line.take_value();
                        let mut meta = line.take_metadata();
                        // FIXME #166
                        let ts: i64 = (line.get_timestamp_ms() as f64 * 0.001) as i64;

                        let mut logline = metric::LogLine::new(path, value);
                        logline = logline.time(ts);
                        logline = logline.overlay_tags_from_map(&tags);
                        for (key, value) in meta.drain() {
                            logline = logline.overlay_tag(key, value);
                        }
                        util::send(&mut chans, metric::Event::new_log(logline));

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
        let listener = TcpListener::bind(srv.first().unwrap()).expect("Unable to bind to TCP socket");
        let chans = self.chans.clone();
        let tags = self.tags.clone();
        info!("server started on {}:{}", self.ip, self.port);
        let error = self.telemetry_error;
        let jh = thread::spawn(move || handle_tcp(chans, tags, listener, error));

        jh.join().expect("Uh oh, child thread panicked!");
    }
}
