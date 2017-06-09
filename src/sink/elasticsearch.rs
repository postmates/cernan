use chrono::datetime::DateTime;
use chrono::naive::datetime::NaiveDateTime;
use chrono::offset::utc::UTC;

use elastic::prelude::*;

use metric::{LogLine, Telemetry};

use sink::{Sink, Valve};
use source::report_telemetry;
use std::collections::vec_deque::VecDeque;
use std::sync;
use time;
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize, ElasticType)]
struct Payload {
    uuid: String,
    path: String,
    payload: String,
    timestamp: String,
}

#[derive(Debug, Clone)]
pub struct ElasticsearchConfig {
    pub config_path: Option<String>,
    pub index_prefix: String,
    pub secure: bool, // whether http or https
    pub host: String,
    pub port: usize,
    pub flush_interval: u64,
}

impl Default for ElasticsearchConfig {
    fn default() -> Self {
        ElasticsearchConfig {
            config_path: Some("sinks.elasticsearch".to_string()),
            secure: false,
            host: "127.0.0.1".to_string(),
            index_prefix: "".to_string(),
            port: 9200,
            flush_interval: 10,
        }
    }
}

pub struct Elasticsearch {
    buffer: VecDeque<LogLine>,
    client: Client,
    index_prefix: String,
    flush_interval: u64,
}

impl Elasticsearch {
    pub fn new(config: ElasticsearchConfig) -> Elasticsearch {
        let proto = if config.secure { "https" } else { "http" };
        let params =
            RequestParams::new(format!("{}://{}:{}", proto, config.host, config.port));
        let client = Client::new(params).unwrap();

        Elasticsearch {
            buffer: VecDeque::new(),
            client: client,
            index_prefix: config.index_prefix,
            flush_interval: config.flush_interval,
        }
    }
}

impl Sink for Elasticsearch {
    fn flush_interval(&self) -> Option<u64> {
        Some(self.flush_interval)
    }

    fn flush(&mut self) {
        let mut attempts: u32 = 0;
        while let Some(m) = self.buffer.pop_front() {
            let doc = Payload {
                uuid: Uuid::new_v4().hyphenated().to_string(),
                path: m.path.clone(),
                payload: m.value.clone(),
                timestamp: format_time(m.time),
            };

            match self.client
                      .index_document(index(idx(&self.index_prefix, m.time)),
                                      id(doc.uuid.clone()),
                                      doc)
                      .send() {
                Ok(_) => {
                    attempts = attempts.saturating_sub(1);
                    report_telemetry("sinks.elasticsearch.index_document.success",
                                     1.0);
                    debug!("Wrote one record into Elasticsearch");
                }
                Err(err) => {
                    report_telemetry("sinks.elasticsearch.index_document.failure",
                                     1.0);
                    debug!("Failed to write record into Elasticsearch {}", err);
                    // Unfortunately the errors that we get out of our
                    // client library are not structured. We _can_
                    // parse them but the responses will depend on the
                    // underlying Elasticsearch.
                    //
                    // We're going to _hope_ that the error is
                    // recoverable and swing back around again.
                    self.buffer.push_back(m);
                    attempts += 1;
                    time::delay(attempts);
                    if attempts > 10 {
                        break;
                    }
                }
            }
        }
    }

    fn deliver(&mut self, _: sync::Arc<Option<Telemetry>>) -> () {
        // nothing, intentionally
    }

    fn deliver_line(&mut self, mut lines: sync::Arc<Option<LogLine>>) -> () {
        let line: LogLine = sync::Arc::make_mut(&mut lines).take().unwrap();
        self.buffer.push_back(line);
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
    let utc_time: DateTime<UTC> = DateTime::from_utc(naive_time, UTC);
    format!("{}", utc_time.format("%Y-%m-%dT%H:%M:%S%.3fZ"))
}

#[inline]
fn idx(prefix: &str, time: i64) -> String {
    let naive_time = NaiveDateTime::from_timestamp(time, 0);
    let utc_time: DateTime<UTC> = DateTime::from_utc(naive_time, UTC);
    format!("{}-{}", prefix, utc_time.format("%Y-%m-%d"))
}
