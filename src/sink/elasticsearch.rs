use chrono::datetime::DateTime;
use chrono::naive::datetime::NaiveDateTime;
use chrono::offset::utc::UTC;

use elastic;
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
    pub index_prefix: Option<String>,
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
            index_prefix: None,
            port: 9200,
            flush_interval: 10,
        }
    }
}

pub struct Elasticsearch {
    buffer: VecDeque<LogLine>,
    client: Client,
    index_prefix: Option<String>,
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

    fn ensure_indexed(&mut self,
                      idx: String,
                      doc: Payload)
                      -> Result<(), elastic::error::Error> {
        use elastic::error;
        let get_res = self.client
            .get_document::<Payload>(index(idx.clone()), id(doc.uuid.clone()))
            .send();

        match get_res {
            // The doc was found: no need to index
            Ok(GetResponse { source: Some(_), .. }) => {
                report_telemetry("sinks.elasticsearch.success.doc_existed", 1.0);
                Ok(())
            }
            // The index exists, but the doc wasn't found: map and index
            Ok(_) => self.put_doc(idx.clone(), doc).map(|_| ()),
            // No index: create it, then map and index
            Err(error::Error(error::ErrorKind::Api(error::ApiError::IndexNotFound { .. }), _)) => {
                self.put_index(idx.clone())
                    .and(self.put_doc(idx, doc))
                    .map(|_| ())
            }
            // Something went wrong, who knows what
            Err(e) => Err(e),
        }
    }

    fn put_index(&mut self,
                 idx: String)
                 -> Result<CommandResponse, elastic::error::Error> {
        let res = self.client
            .create_index(index(idx.clone()))
            .send()
            .and(self.client.put_mapping::<Payload>(index(idx)).send());
        report_telemetry("sinks.elasticsearch.put_index", 1.0);
        res
    }

    fn put_doc(&mut self,
               idx: String,
               doc: Payload)
               -> Result<IndexResponse, elastic::error::Error> {
        let res = self.client
            .index_document(index(idx), id(doc.uuid.clone()), doc)
            .params(|p| p.url_param("refresh", true))
            .send();
        report_telemetry("sinks.elasticsearch.put_doc", 1.0);
        res
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

            let index = idx(&self.index_prefix, m.time);
            match self.ensure_indexed(index, doc) {
                Ok(()) => {
                    report_telemetry("sinks.elasticsearch.success", 1.0);
                }
                Err(err) => {
                    report_telemetry("sinks.elasticsearch.failure", 1.0);
                    debug!("Failed to create index, error: {}", err);
                    self.buffer.push_back(m);
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
fn idx(prefix: &Option<String>, time: i64) -> String {
    let naive_time = NaiveDateTime::from_timestamp(time, 0);
    let utc_time: DateTime<UTC> = DateTime::from_utc(naive_time, UTC);
    match prefix {
        &Some(ref p) => format!("{}-{}", p, utc_time.format("%Y-%m-%d")),
        &None => format!("{}", utc_time.format("%Y-%m-%d")),
    }
}
