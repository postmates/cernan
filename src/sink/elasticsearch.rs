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
use time;
use uuid::Uuid;

lazy_static! {
    /// Total deliveries made
    pub static ref ELASTIC_RECORDS_DELIVERY: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    /// Total records delivered in the last delivery
    pub static ref ELASTIC_RECORDS_TOTAL_DELIVERED: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    /// Total records that failed to be delivered due to error
    pub static ref ELASTIC_RECORDS_TOTAL_FAILED: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    /// Total errors during attempted delivery
    pub static ref ELASTIC_ERROR_ATTEMPTS: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    /// Total errors during attempted delivery, unknown
    pub static ref ELASTIC_ERROR_UNKNOWN: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    /// Total number of index bulk action errors
    pub static ref ELASTIC_BULK_ACTION_INDEX_ERR: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    /// Total number of create bulk action errors
    pub static ref ELASTIC_BULK_ACTION_CREATE_ERR: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    /// Total number of update bulk action errors
    pub static ref ELASTIC_BULK_ACTION_UPDATE_ERR: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    /// Total number of delete bulk action errors
    pub static ref ELASTIC_BULK_ACTION_DELETE_ERR: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
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
            flush_interval: 10,
        }
    }
}

/// The elasticsearch sink struct.
///
/// Refer to the documentation on `ElasticsearchConfig` for more details.
pub struct Elasticsearch {
    buffer: Vec<LogLine>,
    client: Client,
    index_prefix: Option<String>,
    flush_interval: u64,
}

impl Elasticsearch {
    /// Construct a new Elasticsearch.
    ///
    /// Refer to the documentation on Elasticsearch for more details.
    pub fn new(config: ElasticsearchConfig) -> Elasticsearch {
        let proto = if config.secure { "https" } else { "http" };
        let params =
            RequestParams::new(format!("{}://{}:{}", proto, config.host, config.port));
        let client = Client::new(params).unwrap();

        Elasticsearch {
            buffer: Vec::new(),
            client: client,
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

        let mut attempts: u32 = 0;
        loop {
            let mut buffer = String::with_capacity(4048);
            self.bulk_body(&mut buffer);
            debug!("BODY: {:?}", buffer);
            let bulk_resp: Result<BulkResponse> = self.client
                .request(BulkRequest::new(buffer))
                .send()
                .and_then(into_response);

            match bulk_resp {
                Ok(bulk) => {
                    self.buffer.clear();
                    ELASTIC_RECORDS_DELIVERY.fetch_add(1, Ordering::Relaxed);
                    ELASTIC_RECORDS_TOTAL_DELIVERED.fetch_add(1, Ordering::Relaxed);
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
                    return;
                }
                Err(err) => {
                    ELASTIC_ERROR_ATTEMPTS
                        .fetch_add(attempts as usize, Ordering::Relaxed);
                    ELASTIC_ERROR_UNKNOWN.fetch_add(1, Ordering::Relaxed);
                    error!("Unable to write, unknown failure: {}", err);
                    attempts += 1;
                    time::delay(attempts);
                    if attempts > 10 {
                        break;
                    }
                    continue;
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
