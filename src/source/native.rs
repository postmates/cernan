use constants;
use metric;
use mio;
use protobuf;
use protocols::native::{AggregationMethod, Payload};
use source::{BufferedPayload, PayloadErr, TCPConfig, TCPStreamHandler, TCP};
use std::net;
use std::str;
use std::sync;
use std::sync::atomic;
use util;

lazy_static! {
    /// Total payloads processed.
    pub static ref NATIVE_PAYLOAD_SUCCESS_SUM: sync::Arc<atomic::AtomicUsize> = sync::Arc::new(atomic::AtomicUsize::new(0));
    /// Total fatal parse failures.
    pub static ref NATIVE_PAYLOAD_PARSE_FAILURE_SUM: sync::Arc<atomic::AtomicUsize> = sync::Arc::new(atomic::AtomicUsize::new(0));
}

/// The native source
///
/// This source is the pair to the native sink. The native source/sink use or
/// consume cernan's native protocol, defined
/// `resources/protobufs/native.proto`. Clients may use the native protocol
/// without having to obey the translation required in other sources or
/// operators may set up cernan to cernan communication.

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

impl From<NativeServerConfig> for TCPConfig {
    fn from(item: NativeServerConfig) -> Self {
        TCPConfig {
            host: item.ip,
            port: item.port,
            tags: item.tags,
            forwards: item.forwards,
            config_path: item.config_path,
        }
    }
}

#[derive(Default, Debug, Clone, Deserialize)]
pub struct NativeStreamHandler;

impl TCPStreamHandler for NativeStreamHandler {
    fn handle_stream(
        &mut self,
        chans: util::Channel,
        tags: &sync::Arc<metric::TagMap>,
        poller: &mio::Poll,
        stream: mio::net::TcpStream,
    ) -> () {
        let mut streaming = true;
        let mut reader = BufferedPayload::new(stream.try_clone().unwrap());
        while streaming {
            let mut events = mio::Events::with_capacity(1024);
            match poller.poll(&mut events, None) {
                Err(e) => panic!("Failed during poll {:?}", e),
                Ok(_num_events) => {
                    for event in events {
                        match event.token() {
                            constants::SYSTEM => {
                                streaming = false;
                                break;
                            }
                            _stream_token => {
                                while streaming {
                                    match reader.read() {
                                        Ok(mut raw) => {
                                            let handle_res =
                                                self.handle_stream_payload(
                                                    chans.clone(),
                                                    tags,
                                                    &mut raw,
                                                );
                                            if handle_res.is_err() {
                                                NATIVE_PAYLOAD_PARSE_FAILURE_SUM
                                                    .fetch_add(
                                                        1,
                                                        atomic::Ordering::Relaxed,
                                                    );
                                                streaming = false;
                                                break;
                                            }
                                            NATIVE_PAYLOAD_SUCCESS_SUM.fetch_add(
                                                1,
                                                atomic::Ordering::Relaxed,
                                            );
                                        }
                                        Err(PayloadErr::WouldBlock) => {
                                            // Not enough data yet.  Try again.
                                            break;
                                        }
                                        Err(PayloadErr::EOF) => {
                                            // Client went away.  Shut it down
                                            // (gracefully).
                                            trace!("TCP stream closed.");
                                            streaming = false;
                                            break;
                                        }
                                        Err(e) => {
                                            error!("Failed to process native payload! {:?}", e);
                                            streaming = false;
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                    }
                } // events processing
            } // poll
        } // while connected

        // On some systems shutting down an already closed connection (client or
        // otherwise) results in an Err.  See -
        // https://doc.rust-lang.org/beta/std/net/struct.TcpStream.html#platform-specific-behavior
        let _shutdown_result = stream.shutdown(net::Shutdown::Both);
    } // handle_stream
}

impl NativeStreamHandler {
    fn handle_stream_payload(
        &mut self,
        mut chans: util::Channel,
        tags: &sync::Arc<metric::TagMap>,
        buf: &mut Vec<u8>,
    ) -> Result<(), protobuf::ProtobufError> {
        match protobuf::parse_from_bytes::<Payload>(buf) {
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
                Ok(())
            }
            Err(err) => {
                trace!("Unable to read payload: {:?}", err);
                return Err(err);
            }
        }
    }
}

/// Source for Cernan's native protocol.
pub type NativeServer = TCP<NativeStreamHandler>;
