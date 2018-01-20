//! Kinesis sink for Raw events.
use base64;
use hyper;
use metric;
use metric::{LogLine, Telemetry};
use rusoto_core;
use rusoto_core::DefaultCredentialsProvider;
use rusoto_core::default_tls_client;
use rusoto_kinesis::{KinesisClient, PutRecordsError, PutRecordsInput,
                     PutRecordsOutput, PutRecordsRequestEntry};
use rusoto_kinesis::Kinesis as RusotoKinesis;
use sink::Sink;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use util::Valve;

lazy_static! {
    /// Total records published.
    pub static ref KINESIS_PUBLISH_SUCCESS_SUM: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    /// Total records discarded due to constraint violations.
    pub static ref KINESIS_PUBLISH_DISCARD_SUM: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    /// Total retryable publication errors.
    pub static ref KINESIS_PUBLISH_FAILURE_SUM: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    /// Total fatal publication errors.
    pub static ref KINESIS_PUBLISH_FATAL_SUM: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
}

/// Config options for Kinesis config.
#[derive(Clone, Debug, Deserialize)]
pub struct KinesisConfig {
    /// Canonical name for the given Kinesis sink.
    pub config_path: Option<String>,
    /// Region `stream_name` exists in.
    pub region: rusoto_core::Region,
    /// Kinesis stream identifier to publish to.
    pub stream_name: Option<String>,
    /// How often (seconds) the local buffer is published.  Default = 1 second.
    pub flush_interval: u64,
}

impl Default for KinesisConfig {
    fn default() -> KinesisConfig {
        KinesisConfig {
            config_path: None,
            region: rusoto_core::region::default_region(),
            stream_name: None,
            flush_interval: 1,
        }
    }
}

fn connect(
    region: rusoto_core::Region,
) -> Box<KinesisClient<DefaultCredentialsProvider, hyper::client::Client>> {
    let tls = default_tls_client().unwrap();
    let provider = DefaultCredentialsProvider::new().unwrap();
    return Box::new(KinesisClient::new(tls, provider, region));
}

/// Kinesis sink internal state.
pub struct Kinesis {
    region: rusoto_core::Region,
    client: Box<KinesisClient<DefaultCredentialsProvider, hyper::client::Client>>,

    flush_interval: u64,
    max_records_per_batch: usize,
    max_bytes_per_batch: usize,

    /// Name of the stream we are publishing to.
    stream_name: String,
    /// Number of bytes the current buffer represents.
    buffer_size: usize,
    /// Kinesis prepped event blobs.
    buffer: Vec<PutRecordsRequestEntry>,
}

impl Sink<KinesisConfig> for Kinesis {
    fn init(config: KinesisConfig) -> Self {
        if config.stream_name.is_none() {
            panic!("No Kinesis stream provided!");
        };

        let flush_interval = config.flush_interval;
        let max_records = 1000;
        let max_bytes = 1 << 20; // 1MB
        Kinesis {
            client: connect(config.region.clone()),
            region: config.region,
            stream_name: config.stream_name.unwrap(),

            /// Publication limits.  See - https://docs.aws.amazon.com/streams/latest/dev/service-sizes-and-limits.html
            flush_interval: flush_interval,
            buffer: Vec::with_capacity(max_records),
            buffer_size: 0,
            max_records_per_batch: max_records,
            max_bytes_per_batch: max_bytes, // 1 MB
        }
    }

    fn valve_state(&self) -> Valve {
        // We never close up shop.
        Valve::Open
    }

    fn deliver(&mut self, _: Arc<Option<Telemetry>>) -> () {
        // Discard point
    }

    fn deliver_line(&mut self, _: Arc<Option<LogLine>>) -> () {
        // Discard line
    }

    /// Encodes and records the given event into the internal buffer.
    ///
    /// If the given record would put the buffer at capacity, then the contents
    /// are first flushed before the given record is added.
    fn deliver_raw(
        &mut self,
        order_by: u64,
        _encoding: metric::Encoding,
        bytes: Vec<u8>,
    ) {
        let encoded_bytes = base64::encode(&bytes).into_bytes();
        let encoded_bytes_len = encoded_bytes.len();
        let record_too_big = encoded_bytes_len > self.max_bytes_per_batch;
        if record_too_big {
            KINESIS_PUBLISH_DISCARD_SUM.fetch_add(1, Ordering::Relaxed);
            warn!("Discarding encoded record with size {:?} as it is too large to publish!", encoded_bytes_len);
            return;
        }

        let buffer_too_big =
            (self.buffer_size + encoded_bytes_len) > self.max_bytes_per_batch;
        let buffer_too_long = self.buffer.len() >= self.max_records_per_batch;
        if buffer_too_big || buffer_too_long {
            self.flush();
        }

        let partition_key = format!("{:X}", order_by);
        let entry = PutRecordsRequestEntry {
            data: encoded_bytes,
            explicit_hash_key: None,
            partition_key: partition_key,
        };
        self.buffer.push(entry);
        self.buffer_size += encoded_bytes_len;
    }

    fn flush(&mut self) {
        self.publish_buffer();
    }

    fn flush_interval(&self) -> Option<u64> {
        Some(self.flush_interval)
    }

    fn shutdown(mut self) -> () {
        self.flush();
    }
}

impl Kinesis {
    /// Syn. publishes the entire contents of the buffer.
    ///
    /// Records which fail to publish are reattempted indefinitely.
    pub fn publish_buffer(&mut self) {
        self.buffer_size = 0;
        let mut buffer: Vec<PutRecordsRequestEntry> = self.buffer.drain(..).collect();

        while !buffer.is_empty() {
            let put_records_input = PutRecordsInput {
                records: buffer.clone(),
                stream_name: self.stream_name.clone(),
            };

            match self.client.put_records(&put_records_input) {
                Ok(put_records_output) => {
                    self.filter_successful(&mut buffer, &put_records_output);
                    break;
                }

                Err(PutRecordsError::ProvisionedThroughputExceeded(_)) => {
                    info!(
                        "Provisioned throughput exceeded on {:?}.  Retrying...",
                        self.stream_name
                    );
                }

                Err(err) => {
                    KINESIS_PUBLISH_FATAL_SUM.fetch_add(1, Ordering::Relaxed);
                    self.client = connect(self.region.clone());
                    error!(
                        "Reconnecting due to fatal exception during put_records: {:?}",
                        err
                    );
                    continue;
                }
            }
        }
    }

    /// Filters record request entries from the source buffer if they have been
    /// successfully published.
    pub fn filter_successful(
        &self,
        buffer: &mut Vec<PutRecordsRequestEntry>,
        put_records_output: &PutRecordsOutput,
    ) {
        if put_records_output.failed_record_count.is_none()
            || put_records_output.failed_record_count == Some(0)
        {
            buffer.clear();
            return;
        }

        for (idx, record_result) in put_records_output.records.iter().enumerate().rev()
        {
            if record_result.sequence_number.is_some() {
                buffer.remove(idx);
                KINESIS_PUBLISH_SUCCESS_SUM.fetch_add(1, Ordering::Relaxed);
            } else {
                // Something went wrong
                trace!(
                    "Record failed to publish: {:?} - {:?}",
                    record_result.error_code.clone().unwrap(),
                    record_result.error_message.clone().unwrap()
                );
                KINESIS_PUBLISH_FAILURE_SUM.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}
