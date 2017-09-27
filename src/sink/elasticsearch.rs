//! `ElasticSearch` is a documentation indexing engine.

use chrono::DateTime;
use chrono::naive::NaiveDateTime;
use chrono::offset::Utc;
use elastic::client::responses::BulkAction;
use elastic::error::Result;
use elastic::prelude::*;
use metric::{LogLine, Telemetry};
use sink::{Sink, Valve};
use std::error::Error;
use std::sync;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use uuid::Uuid;

lazy_static! {
    /// Total deliveries made
    pub static ref ELASTIC_RECORDS_DELIVERY: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
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
    /// Total number of api errors due to unknown reasons 
    pub static ref ELASTIC_ERROR_API_UNKNOWN: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    /// Total number of api errors due to parse respose json errors
    pub static ref ELASTIC_ERROR_RESPONSE_JSON: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    /// Total number of api errors due to parse respose io errors
    pub static ref ELASTIC_ERROR_RESPONSE_IO: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    /// Total number of api errors due to json parsing 
    pub static ref ELASTIC_ERROR_JSON: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    /// Total number of api errors due to reqwest client
    pub static ref ELASTIC_ERROR_REQUEST_FAILURE: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
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
    /// Determines whether to use HTTP or HTTPS when publishing to
    /// Elasticsearch.
    pub secure: bool, // whether http or https
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
            port: 9200,
            flush_interval: 1,
        }
    }
}

/// The elasticsearch sink struct.
///
/// Refer to the documentation on `ElasticsearchConfig` for more details.
pub struct Elasticsearch {
    buffer: Vec<LogLine>,
    secure: bool,
    host: String,
    port: usize,
    index_prefix: Option<String>,
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
            flush_interval: config.flush_interval,
        }
    }

    fn bulk_body(&self, buffer: &mut String) -> () {
        assert!(!self.buffer.is_empty());
        use serde_json::{to_string, Value};
        for m in &self.buffer {
            let uuid = Uuid::new_v4().hyphenated().to_string();
            let header: Value = json!({
                "index": {
                    "_index" : idx(&self.index_prefix, m.time),
                    "_type" : "payload",
                    "_id" : uuid.clone(),
                }
            });
            buffer.push_str(&to_string(&header).unwrap());
            buffer.push('\n');
            let mut payload: Value = json!({
                "uuid": uuid,
                "path": m.path.clone(),
                "payload": m.value.clone(),
                "timestamp": format_time(m.time),
            });
            let obj = payload.as_object_mut().unwrap();
            for &(ref k, ref v) in m.tags.iter() {
                obj.insert(k.clone(), Value::String(v.clone()));
            }
            for &(ref k, ref v) in m.fields.iter() {
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
        let client = Client::new(params).unwrap();

        let mut buffer = String::with_capacity(4048);
        self.bulk_body(&mut buffer);
        debug!("BODY: {:?}", buffer);
        let bulk_resp: Result<BulkResponse> = client
            .request(BulkRequest::new(buffer))
            .send()
            .and_then(into_response);
        
        match bulk_resp {
            Ok(bulk) => {
                self.buffer.clear();
                ELASTIC_RECORDS_DELIVERY.fetch_add(1, Ordering::Relaxed);
                ELASTIC_RECORDS_TOTAL_DELIVERED.fetch_add(bulk.items.ok.len(), Ordering::Relaxed);
                ELASTIC_RECORDS_TOTAL_FAILED
                    .fetch_add(bulk.items.err.len(), Ordering::Relaxed);
                if !bulk.items.err.is_empty() {
                    error!("Failed to write {} put records", bulk.items.err.len());
                    for bulk_err in bulk.items.err {
                        if let Some(cause) = bulk_err.cause() {
                            error!(
                                "Failed to write item with error {}, cause {}",
                                bulk_err.description(),
                                cause
                            );
                        } else {
                            error!(
                                "Failed to write item with error {}",
                                bulk_err.description()
                            );
                        }
                        match bulk_err.action {
                            BulkAction::Index => ELASTIC_BULK_ACTION_INDEX_ERR
                                .fetch_add(1, Ordering::Relaxed),
                            BulkAction::Create => ELASTIC_BULK_ACTION_CREATE_ERR
                                .fetch_add(1, Ordering::Relaxed),
                            BulkAction::Update => ELASTIC_BULK_ACTION_UPDATE_ERR
                                .fetch_add(1, Ordering::Relaxed),
                            BulkAction::Delete => ELASTIC_BULK_ACTION_DELETE_ERR
                                .fetch_add(1, Ordering::Relaxed),
                        };
                    }
                }
            }
            Err(err) => {
                use elastic::error::ErrorKind;
                match *err.kind() {
                    ErrorKind::Response(ref response_err) => {
                        use elastic::client::responses::parse::ParseResponseError;
                        match *response_err {
                            ParseResponseError::Json(ref e) => {
                                ELASTIC_ERROR_RESPONSE_JSON.fetch_add(1, Ordering::Relaxed);
                                error!("Unable to write, Parse Response Error (JSON): {}", e.description());
                            },
                            ParseResponseError::Io(ref e) => {
                                ELASTIC_ERROR_RESPONSE_IO.fetch_add(1, Ordering::Relaxed);
                                error!("Unable to write, Parse Response Error (IO): {}", e.description());
                            },
                        }
                    },
                    ErrorKind::Json(ref e) => {
                        ELASTIC_ERROR_JSON.fetch_add(1, Ordering::Relaxed);
                        error!("Unable to write, JSON: {}", e.description());
                    },
                    ErrorKind::Client(ref e) => {
                        ELASTIC_ERROR_REQUEST_FAILURE.fetch_add(1, Ordering::Relaxed);
                        error!("Unable to write, Reqwest Client Error: {}", e.description());
                    },
                    ErrorKind::Msg(ref msg) => {
                        error!("Unable to write, msg: {}", msg);
                    },
                    ErrorKind::Api(ref err) => {
                        use elastic::error::ApiError;
                        match *err {
                            ApiError::IndexNotFound { ref index } => {
                                ELASTIC_ERROR_API_INDEX_NOT_FOUND.fetch_add(1, Ordering::Relaxed);
                                error!("Unable to write, API Error (Index Not Found): {}", index);
                            },
                            ApiError::Parsing { ref reason, .. } => {
                                ELASTIC_ERROR_API_PARSING.fetch_add(1, Ordering::Relaxed);
                                error!("Unable to write, API Error (Parsing): {}", reason);
                            },
                            ApiError::MapperParsing { ref reason, .. } => {
                                ELASTIC_ERROR_API_MAPPER_PARSING.fetch_add(1, Ordering::Relaxed);
                                error!("Unable to write, API Error (Mapper Parsing): {}", reason);
                            },
                            ApiError::ActionRequestValidation { ref reason, .. } => {
                                ELASTIC_ERROR_API_ACTION_REQUEST_VALIDATION.fetch_add(1, Ordering::Relaxed);
                                error!("Unable to write, API Error (Action Request Validation): {}", reason);
                            },
                            ApiError::Other { .. } => {
                                ELASTIC_ERROR_API_UNKNOWN.fetch_add(1, Ordering::Relaxed);
                                error!("Unable to write, API Error (Unknown)");
                            },
                        }
                    },
                }
            }
        }
    }

    fn deliver(&mut self, _: sync::Arc<Option<Telemetry>>) -> () {
        // nothing, intentionally
    }

    fn deliver_line(&mut self, mut lines: sync::Arc<Option<LogLine>>) -> () {
        let line: LogLine = sync::Arc::make_mut(&mut lines).take().unwrap();
        self.buffer.push(line);
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
