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

/// The native source
///
/// This source is the pair to the native sink. The native source/sink use or
/// consume cernan's native protocol, defined
/// `resources/protobufs/native.proto`. Clients may use the native protocol
/// without having to obey the translation required in other sources or
/// operators may set up cernan to cernan communication.
pub struct NativeServer {
    chans: util::Channel,
    ip: String,
    port: u16,
    tags: metric::TagMap,
}

/// Configuration for the native source
#[derive(Debug, Clone, Deserialize)]
pub struct NativeServerConfig {
    /// The IP address the native source will bind to.
    pub ip: String,
    /// The port the source will listen on.
    pub port: u16,
    /// The tags the Native source will associate with every Telemetry it
    /// creates.
    pub tags: metric::TagMap,
    /// The forwards for the native source to send its Telemetry along.
    pub forwards: Vec<String>,
    /// The unique name for the source in the routing topology.
    pub config_path: Option<String>,
}

impl Default for NativeServerConfig {
    fn default() -> Self {
        NativeServerConfig {
            ip: "0.0.0.0".to_string(),
            port: 1972,
            tags: metric::TagMap::default(),
            forwards: Vec::default(),
            config_path: None,
        }
    }
}

impl NativeServer {
    /// Create a new NativeServer
    pub fn new(
        chans: Vec<hopper::Sender<metric::Event>>,
        config: NativeServerConfig,
    ) -> NativeServer {
        NativeServer {
            chans: chans,
            ip: config.ip,
            port: config.port,
            tags: config.tags,
        }
    }
}

fn handle_tcp(
    chans: util::Channel,
    tags: metric::TagMap,
    listner: TcpListener,
) -> thread::JoinHandle<()> {
    thread::spawn(move || for stream in listner.incoming() {
        if let Ok(stream) = stream {
            debug!(
                "new peer at {:?} | local addr for peer {:?}",
                stream.peer_addr(),
                stream.local_addr()
            );
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
                // TODO we have to handle bin_bounds. We'll use samples to get
                // the values of each bounds' counter.
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
                        let mut metric = metric::Telemetry::new().name(name);
                        metric = metric.value(smpls[0]);
                        metric = match aggr_type {
                            AggregationMethod::SET => {
                                metric.kind(metric::AggregationMethod::Set)
                            }
                            AggregationMethod::SUM => {
                                metric.kind(metric::AggregationMethod::Sum)
                            }
                            AggregationMethod::SUMMARIZE => {
                                metric.kind(metric::AggregationMethod::Summarize)
                            }
                            AggregationMethod::BIN => {
                                metric.kind(metric::AggregationMethod::Histogram)
                            }
                        };
                        metric = metric.persist(point.get_persisted());
                        metric = metric.timestamp(ts);
                        let mut metric = metric.harden().unwrap(); // todo don't unwrap
                        metric = metric.overlay_tags_from_map(&tags);
                        for (key, value) in meta.drain() {
                            metric = metric.overlay_tag(key, value);
                        }
                        for smpl in &smpls[1..] {
                            metric = metric.insert(*smpl);
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
        let listener = TcpListener::bind(srv.first().unwrap())
            .expect("Unable to bind to TCP socket");
        let chans = self.chans.clone();
        let tags = self.tags.clone();
        info!("server started on {}:{}", self.ip, self.port);
        let jh = thread::spawn(move || handle_tcp(chans, tags, listener));

        jh.join().expect("Uh oh, child thread panicked!");
    }
}
