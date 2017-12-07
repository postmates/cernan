//! `ElasticSearch` is a documentation indexing engine.

use chrono::DateTime;
use chrono::naive::NaiveDateTime;
use chrono::offset::Utc;
use elastic::client::responses::bulk;
use elastic::error::Result;
use elastic::error;
use elastic::prelude::*;
use metric::{LogLine, Telemetry};
use sink::{Sink, Valve};
use std::cmp;
use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync;
use uuid;

lazy_static! {
    /// Total deliveries made
    pub static ref ELASTIC_RECORDS_DELIVERY: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    /// Total internal buffer entries
    pub static ref ELASTIC_INTERNAL_BUFFER_LEN: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    /// Total records delivered in the last delivery
    pub static ref ELASTIC_RECORDS_TOTAL_DELIVERED: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    /// Total records that failed to be delivered due to error
    pub static ref ELASTIC_RECORDS_TOTAL_FAILED: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    /// Unknown error occurred during attempted flush
    pub static ref ELASTIC_ERROR_UNKNOWN: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    /// Total number of index bulk action errors
    pub static ref ELASTIC_BULK_ACTION_INDEX_ERR: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    /// Total number of create bulk action errors
    pub static ref ELASTIC_BULK_ACTION_CREATE_ERR: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    /// Total number of update bulk action errors
    pub static ref ELASTIC_BULK_ACTION_UPDATE_ERR: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    /// Total number of delete bulk action errors
    pub static ref ELASTIC_BULK_ACTION_DELETE_ERR: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));

    /// Total number of api errors due to index not found
    pub static ref ELASTIC_ERROR_API_INDEX_NOT_FOUND: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    /// Total number of api errors due to parsing
    pub static ref ELASTIC_ERROR_API_PARSING: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    /// Total number of api errors due to mapper parsing
    pub static ref ELASTIC_ERROR_API_MAPPER_PARSING: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    /// Total number of api errors due to action request validation
    pub static ref ELASTIC_ERROR_API_ACTION_REQUEST_VALIDATION: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    /// Total number of api errors due to missing document
    pub static ref ELASTIC_ERROR_API_DOCUMENT_MISSING: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    /// Total number of api errors due to index already existing
    pub static ref ELASTIC_ERROR_API_INDEX_ALREADY_EXISTS: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    /// Total number of api errors due to unknown reasons
    pub static ref ELASTIC_ERROR_API_UNKNOWN: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    /// Total number of client errors, no specific reasons
    pub static ref ELASTIC_ERROR_CLIENT: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
}

/// Configuration for the Elasticsearch sink
///
/// Elasticsearch is an open-source document indexing engine. It can be used for
/// performing searches over corpus, which for cernan's use is
/// `metric::LogLine`.
#[derive(Debug, Clone)]
pub struct ElasticsearchConfig {
    /// The unique name of the sink in the routing topology
    pub config_path: Option<String>,
    /// The Elasticsearch index prefix. This prefix will be added to the
    /// automatically created date-based index of this sink.
    pub index_prefix: Option<String>,
    /// The _type of the Elasticsearch index
    pub index_type: String,
    /// Determines whether to use HTTP or HTTPS when publishing to
    /// Elasticsearch.
    pub secure: bool,
    /// Determine how many times to attempt the delivery of a log line before
    /// dropping it from the buffer. Failures of a global bulk request does not
    /// count against this limit.
    pub delivery_attempt_limit: u8,
    /// The Elasticsearch host. May be an IP address or DNS hostname.
    pub host: String,
    /// The Elasticsearch port.
    pub port: usize,
    /// The sink's specific flush interval.
    pub flush_interval: u64,
}

impl Default for ElasticsearchConfig {
    fn default() -> Self {
        ElasticsearchConfig {
            config_path: Some("sinks.elasticsearch".to_string()),
            secure: false,
            host: "127.0.0.1".to_string(),
            index_prefix: None,
            index_type: "payload".to_string(),
            delivery_attempt_limit: 10,
            port: 9200,
            flush_interval: 1,
        }
    }
}

struct Line {
    attempts: u8,
    uuid: uuid::Uuid,
    line: LogLine,
}

/// The elasticsearch sink struct.
///
/// Refer to the documentation on `ElasticsearchConfig` for more details.
pub struct Elasticsearch {
    buffer: Vec<Line>,
    secure: bool,
    host: String,
    port: usize,
    delivery_attempt_limit: u8,
    index_prefix: Option<String>,
    index_type: String,
    flush_interval: u64,
}

impl Elasticsearch {
    /// Construct a new Elasticsearch.
    ///
    /// Refer to the documentation on Elasticsearch for more details.
    pub fn new(config: ElasticsearchConfig) -> Elasticsearch {
        Elasticsearch {
            buffer: Vec::new(),
            secure: config.secure,
            host: config.host,
            port: config.port,
            index_prefix: config.index_prefix,
            index_type: config.index_type,
            delivery_attempt_limit: config.delivery_attempt_limit,
            flush_interval: config.flush_interval,
        }
    }

    fn bulk_body(&self, buffer: &mut String) -> () {
        assert!(!self.buffer.is_empty());
        use serde_json::{to_string, Value};
        for m in &self.buffer {
            let uuid = m.uuid.hyphenated().to_string();
            let line = &m.line;
            let header: Value = json!({
                "index": {
                    "_index" : idx(&self.index_prefix, line.time),
                    "_type" : self.index_type.clone(),
                    "_id" : uuid.clone(),
                }
            });
            buffer.push_str(&to_string(&header).unwrap());
            buffer.push('\n');
            let mut payload: Value = json!({
                "uuid": uuid,
                "path": line.path.clone(),
                "payload": line.value.clone(),
                "timestamp": format_time(line.time),
            });
            let obj = payload.as_object_mut().unwrap();
            for &(ref k, ref v) in line.tags.iter() {
                obj.insert(k.clone(), Value::String(v.clone()));
            }
            for &(ref k, ref v) in line.fields.iter() {
                obj.insert(k.clone(), Value::String(v.clone()));
            }
            buffer.push_str(&to_string(&obj).unwrap());
            buffer.push('\n');
        }
    }
}

impl Sink for Elasticsearch {
    fn flush_interval(&self) -> Option<u64> {
        Some(self.flush_interval)
    }

    fn flush(&mut self) {
        if self.buffer.is_empty() {
            return;
        }

        let proto = if self.secure { "https" } else { "http" };
        let params =
            RequestParams::new(format!("{}://{}:{}", proto, self.host, self.port));
        let client = SyncClientBuilder::from_params(params).build().unwrap();

        let mut buffer = String::with_capacity(4048);
        self.bulk_body(&mut buffer);
        if let Ok(snd) = client.request(BulkRequest::new(buffer)).send() {
            let bulk_resp: Result<BulkResponse> = snd.into_response::<BulkResponse>();
            ELASTIC_INTERNAL_BUFFER_LEN.store(self.buffer.len(), Ordering::Relaxed);
            match bulk_resp {
                Ok(bulk) => {
                    ELASTIC_RECORDS_DELIVERY.fetch_add(1, Ordering::Relaxed);
                    for item in bulk.iter() {
                        match item {
                            Ok(item) => {
                                let uuid = uuid::Uuid::parse_str(item.id())
                                    .expect("catastrophic error, TID not a UUID");
                                let mut idx = 0;
                                for i in 0..self.buffer.len() {
                                    match self.buffer[i].uuid.cmp(&uuid) {
                                        cmp::Ordering::Equal => {
                                            break;
                                        },
                                        _ => { idx += 1 },
                                    }
                                }
                                self.buffer.remove(idx);
                                ELASTIC_RECORDS_TOTAL_DELIVERED
                                    .fetch_add(1, Ordering::Relaxed);
                            }
                            Err(item) => {
                                let uuid = uuid::Uuid::parse_str(item.id())
                                    .expect("catastrophic error, TID not a UUID");
                                let mut idx = 0;
                                for i in 0..self.buffer.len() {
                                    match self.buffer[i].uuid.cmp(&uuid) {
                                        cmp::Ordering::Equal => {
                                            break;
                                        },
                                        _ => { idx += 1 },
                                    }
                                }
                                self.buffer[idx].attempts += 1;
                                if self.buffer[idx].attempts > self.delivery_attempt_limit {
                                    self.buffer.remove(idx);
                                }
                                ELASTIC_RECORDS_TOTAL_FAILED
                                    .fetch_add(1, Ordering::Relaxed);
                                if let Some(cause) = item.cause() {
                                    error!(
                                        "Failed to write item with error {}, cause {}",
                                        item.description(),
                                        cause
                                    );
                                } else {
                                    error!(
                                        "Failed to write item with error {}",
                                        item.description()
                                    );
                                }
                                match item.action() {
                                    bulk::Action::Index => {
                                        ELASTIC_BULK_ACTION_INDEX_ERR
                                            .fetch_add(1, Ordering::Relaxed)
                                    }
                                    bulk::Action::Create => {
                                        ELASTIC_BULK_ACTION_CREATE_ERR
                                            .fetch_add(1, Ordering::Relaxed)
                                    }
                                    bulk::Action::Update => {
                                        ELASTIC_BULK_ACTION_UPDATE_ERR
                                            .fetch_add(1, Ordering::Relaxed)
                                    }
                                    bulk::Action::Delete => {
                                        ELASTIC_BULK_ACTION_DELETE_ERR
                                            .fetch_add(1, Ordering::Relaxed)
                                    }
                                };
                            }
                        }
                    }
                }
                Err(err) => match err {
                    error::Error::Api(ref api_error) => {
                        use elastic::error::ApiError;
                        match *api_error {
                            ApiError::IndexNotFound { ref index } => {
                                ELASTIC_ERROR_API_INDEX_NOT_FOUND
                                    .fetch_add(1, Ordering::Relaxed);
                                error!(
                                    "Unable to write, API Error (Index Not Found): {}",
                                    index
                                );
                            }
                            ApiError::Parsing { ref reason, .. } => {
                                ELASTIC_ERROR_API_PARSING
                                    .fetch_add(1, Ordering::Relaxed);
                                error!(
                                    "Unable to write, API Error (Parsing): {}",
                                    reason
                                );
                            }
                            ApiError::MapperParsing { ref reason, .. } => {
                                ELASTIC_ERROR_API_MAPPER_PARSING
                                    .fetch_add(1, Ordering::Relaxed);
                                error!(
                                    "Unable to write, API Error (Mapper Parsing): {}",
                                    reason
                                );
                            }
                            ApiError::ActionRequestValidation {
                                ref reason, ..
                            } => {
                                ELASTIC_ERROR_API_ACTION_REQUEST_VALIDATION
                                    .fetch_add(1, Ordering::Relaxed);
                                error!(
                                    "Unable to write, API Error (Action Request Validation): {}",
                                    reason
                                );
                            }
                            ApiError::DocumentMissing { ref index, .. } => {
                                ELASTIC_ERROR_API_DOCUMENT_MISSING
                                    .fetch_add(1, Ordering::Relaxed);
                                error!(
                                "Unable to write, API Error (Document Missing): {}",
                                index
                            );
                            }
                            ApiError::IndexAlreadyExists { ref index, .. } => {
                                ELASTIC_ERROR_API_INDEX_ALREADY_EXISTS
                                    .fetch_add(1, Ordering::Relaxed);
                                error!(
                                    "Unable to write, API Error (Index Already Exists): {}",
                                    index
                                );
                            }
                            _ => {
                                ELASTIC_ERROR_API_UNKNOWN
                                    .fetch_add(1, Ordering::Relaxed);
                                error!("Unable to write, API Error (Unknown)");
                            }
                        }
                    }
                    error::Error::Client(ref client_error) => {
                        ELASTIC_ERROR_CLIENT.fetch_add(1, Ordering::Relaxed);
                        error!(
                            "Unable to write, client error: {}",
                            client_error.description()
                        );
                    }
                },
            }
        }
    }

    fn deliver(&mut self, _: sync::Arc<Option<Telemetry>>) -> () {
        // nothing, intentionally
    }

    fn deliver_line(&mut self, mut lines: sync::Arc<Option<LogLine>>) -> () {
        let line: LogLine = sync::Arc::make_mut(&mut lines).take().unwrap();
        let uuid = uuid::Uuid::new_v4();
        self.buffer.push(Line {
            uuid: uuid,
            line: line,
            attempts: 0,
        });
    }

    fn valve_state(&self) -> Valve {
        if self.buffer.len() > 10_000 {
            Valve::Closed
        } else {
            Valve::Open
        }
    }
}

#[inline]
fn format_time(time: i64) -> String {
    let naive_time = NaiveDateTime::from_timestamp(time, 0);
    let utc_time: DateTime<Utc> = DateTime::from_utc(naive_time, Utc);
    format!("{}", utc_time.format("%+"))
}

#[inline]
fn idx(prefix: &Option<String>, time: i64) -> String {
    let naive_time = NaiveDateTime::from_timestamp(time, 0);
    let utc_time: DateTime<Utc> = DateTime::from_utc(naive_time, Utc);
    match *prefix {
        Some(ref p) => format!("{}-{}", p, utc_time.format("%Y-%m-%d")),
        None => format!("{}", utc_time.format("%Y-%m-%d")),
    }
}
