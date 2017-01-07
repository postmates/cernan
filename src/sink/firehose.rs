use chrono::datetime::DateTime;
use chrono::naive::datetime::NaiveDateTime;
use chrono::offset::utc::UTC;
use metric::{LogLine, Telemetry};

use rusoto::{DefaultCredentialsProvider, Region};
use rusoto::firehose::{KinesisFirehoseClient, PutRecordBatchInput, Record};
use rusoto::firehose::PutRecordBatchError::*;

use serde_json;
use serde_json::Map;
use sink::{Sink, Valve};
use std::sync;
use time;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct FirehoseConfig {
    pub delivery_stream: String,
    pub batch_size: usize,
    pub region: Region,
    pub config_path: String,
}

pub struct Firehose {
    buffer: Vec<LogLine>,
    delivery_stream_name: String,
    region: Region,
    batch_size: usize,
}

impl Firehose {
    pub fn new(config: FirehoseConfig) -> Firehose {
        Firehose {
            buffer: Vec::new(),
            delivery_stream_name: config.delivery_stream,
            region: config.region,
            batch_size: config.batch_size,
        }
    }
}

impl Sink for Firehose {
    fn flush(&mut self) {
        let provider = DefaultCredentialsProvider::new().unwrap();
        let client = KinesisFirehoseClient::new(provider, self.region);

        if self.buffer.is_empty() {
            return;
        }

        for chunk in self.buffer.chunks(self.batch_size) {
            let prbi = PutRecordBatchInput {
                delivery_stream_name: self.delivery_stream_name.clone(),
                records: chunk.iter()
                    .map(|m| {
                        let mut pyld = Map::new();
                        pyld.insert(String::from("Path"), (*m.path).to_string());
                        pyld.insert(String::from("Payload"), m.value.clone());
                        pyld.insert(String::from("timestamp"), format_time(m.time));
                        pyld.insert(String::from("Uuid"),
                                    Uuid::new_v4().hyphenated().to_string());
                        for &(ref k, ref v) in m.tags.iter() {
                            pyld.insert(k.clone(), v.clone());
                        }
                        Record { data: serde_json::ser::to_vec(&pyld).unwrap() }
                    })
                    .collect(),
            };
            let mut attempts = 0;
            loop {
                time::delay(attempts);
                match client.put_record_batch(&prbi) {
                    Ok(prbo) => {
                        let failed_put_count = prbo.failed_put_count;
                        if failed_put_count > 0 {
                            error!("Failed to write {} put records", failed_put_count);
                        }
                        break;
                    }
                    Err(err) => {
                        match err {
                            // The following errors cannot be recovered from. We
                            // drop the payload lines and move on to the next
                            // batch. We might choose to split the chunk smaller in
                            // the hopes that the failure is a result of a subset of
                            // the payload being wonky. This is an optimization for
                            // the future.
                            ResourceNotFound(rnf_err) => {
                                error!("Unable to write to resource, not found: {}", rnf_err);
                                break;
                            }
                            InvalidArgument(ia_err) => {
                                error!("Unable to write, invalid argument: {}", ia_err);
                                break;
                            }
                            HttpDispatch(hd_err) => {
                                error!("Unable to write, http dispatch: {}", hd_err);
                                break;
                            }
                            Validation(v_err) => {
                                error!("Unable to write, validation failure: {}", v_err);
                                break;
                            }
                            Unknown(u_err) => {
                                error!("Unable to write, unknown failure: {}", u_err);
                                break;
                            }
                            // The following errors are recoverable, potentially.
                            Credentials(c_err) => {
                                error!("Unable to write, credential failure: {}", c_err);
                            }
                            ServiceUnavailable(su_err) => {
                                error!("Service unavailable, will retry: {}", su_err);
                            }
                        }
                    }
                }
                attempts += 1;
            }
        }
        self.buffer.clear();
    }

    fn deliver(&mut self, _: sync::Arc<Option<Telemetry>>) -> () {
        // nothing, intentionally
    }

    fn deliver_line(&mut self, _: sync::Arc<Option<LogLine>>) -> () {
        // nothing, intentionally
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
