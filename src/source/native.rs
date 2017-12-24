use byteorder::{BigEndian, ReadBytesExt};
use constants;
use hopper;
use metric;
use mio;
use protobuf;
use protocols::native::{AggregationMethod, Payload};
use source;
use std::io;
use std::io::Read;
use std::str;
use std::sync;
use thread;
use util;

/// The native source
///
/// This source is the pair to the native sink. The native source/sink use or
/// consume cernan's native protocol, defined
/// `resources/protobufs/native.proto`. Clients may use the native protocol
/// without having to obey the translation required in other sources or
/// operators may set up cernan to cernan communication.
pub struct NativeServer {
    server: source::tcp::TCP,
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

impl From<NativeServerConfig> for source::tcp::TCPConfig {
    fn from(item: NativeServerConfig) -> Self {
        source::tcp::TCPConfig {
            host: item.ip,
            port: item.port,
            tags: item.tags,
            forwards: item.forwards,
            config_path: item.config_path,
        }
    }
}

fn handle_stream(
    chans: util::Channel,
    tags: &sync::Arc<metric::TagMap>,
    poller: &mio::Poll,
    stream: mio::net::TcpStream,
) {
    let mut reader = io::BufReader::new(stream);
    loop {
        let mut events = mio::Events::with_capacity(1024);
        match poller.poll(&mut events, None) {
            Err(e) => panic!(format!("Failed during poll {:?}", e)),
            Ok(_num_events) => for event in events {
                match event.token() {
                    constants::SYSTEM => return,
                    _stream_token => {
                        let rchans = chans.clone();
                        handle_stream_payload(rchans, tags, &mut reader);
                    }
                }
            },
        }
    }
}

fn handle_stream_payload(
    mut chans: util::Channel,
    tags: &sync::Arc<metric::TagMap>,
    reader: &mut io::BufReader<mio::net::TcpStream>,
) {
    let mut buf = Vec::with_capacity(4000);
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
                metric = metric.overlay_tags_from_map(tags);
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
                logline = logline.overlay_tags_from_map(tags);
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

impl source::Source<NativeServer, NativeServerConfig> for NativeServer {
    /// Create a new NativeServer
    fn new(
        chans: Vec<hopper::Sender<metric::Event>>,
        config: NativeServerConfig,
    ) -> NativeServer {
        NativeServer {
            server: source::TCP::new(chans, config.into()),
        }
    }

    fn run(self) -> thread::ThreadHandle {
        self.server.run(handle_stream)
    }
}
