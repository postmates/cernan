use chrono::datetime::DateTime;
use chrono::naive::datetime::NaiveDateTime;
use chrono::offset::utc::UTC;
use elastic::error::Result;
use elastic::prelude::*;

use metric::{LogLine, Telemetry};

use sink::{Sink, Valve};
use source::report_telemetry3;
use std::sync;
use time;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct ElasticsearchConfig {
    pub config_path: Option<String>,
    pub index_prefix: Option<String>,
    pub secure: bool, // whether http or https
    pub host: String,
    pub port: usize,
    pub flush_interval: u64,
    pub telemetry_error_bound: f64,
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
            telemetry_error_bound: 0.001,
        }
    }
}

pub struct Elasticsearch {
    buffer: Vec<LogLine>,
    client: Client,
    index_prefix: Option<String>,
    flush_interval: u64,
    telemetry_error_bound: f64,
}

impl Elasticsearch {
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
            telemetry_error_bound: config.telemetry_error_bound,
        }
    }

    fn bulk_body(&self, mut buffer: &mut String) -> () {
        assert!(!self.buffer.is_empty());
        use serde_json::{Value, to_string};
        for m in self.buffer.iter() {
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
            let mut obj = payload.as_object_mut().unwrap();
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
                    report_telemetry3("cernan.sinks.elasticsearch.records.delivery",
                                     1.0,
                                     self.telemetry_error_bound);
                    report_telemetry3("cernan.sinks.elasticsearch.records.total_delivered",
                                     bulk.items.ok.len() as f64,
                                     self.telemetry_error_bound);
                    let failed_count = bulk.items.err.len();
                    if failed_count > 0 {
                        report_telemetry3("cernan.sinks.elasticsearch.records.total_failed",
                                         failed_count as f64,
                                         self.telemetry_error_bound);
                        error!("Failed to write {} put records", failed_count);
                    }
                    return;
                }
                Err(err) => {
                    report_telemetry3("cernan.sinks.elasticsearch.error.attempts",
                                     attempts as f64,
                                     self.telemetry_error_bound);
                    report_telemetry3("cernan.sinks.elasticsearch.error.reason.unknown",
                                     1.0,
                                     self.telemetry_error_bound);
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
    let utc_time: DateTime<UTC> = DateTime::from_utc(naive_time, UTC);
    format!("{}", utc_time.format("%+"))
}

#[inline]
fn idx(prefix: &Option<String>, time: i64) -> String {
    let naive_time = NaiveDateTime::from_timestamp(time, 0);
    let utc_time: DateTime<UTC> = DateTime::from_utc(naive_time, UTC);
    match prefix {
        &Some(ref p) => format!("{}-{}", p, utc_time.format("%Y-%m-%d")),
        &None => format!("{}", utc_time.format("%Y-%m-%d")),
    }
}
