use sink::Sink;
use metric::{Metric,LogLine};
use chrono::naive::datetime::NaiveDateTime;
use chrono::datetime::DateTime;
use chrono::offset::utc::UTC;
use time;

use serde_json;
use serde_json::Map;

use rusoto::{DefaultCredentialsProvider, Region};
use rusoto::firehose::{KinesisFirehoseClient, PutRecordBatchInput, Record};
use rusoto::firehose::PutRecordBatchError::*;

pub struct Firehose {
    buffer: Vec<LogLine>,
    delivery_stream_name: String,
}

impl Firehose {
    pub fn new(delivery_stream: &str) -> Firehose {
        Firehose {
            buffer: Vec::new(),
            delivery_stream_name: delivery_stream.to_string(),
        }
    }
}

impl Sink for Firehose {
    fn flush(&mut self) {
        let provider = DefaultCredentialsProvider::new().unwrap();
        let client = KinesisFirehoseClient::new(provider, Region::UsWest2);

        if self.buffer.len() == 0 {
            return;
        }

        for chunk in self.buffer.chunks(450) {
            let prbi = PutRecordBatchInput {
                delivery_stream_name: self.delivery_stream_name.clone(),
                records: chunk.iter().map(|m| {
                    let mut pyld = Map::new();
                    pyld.insert(String::from("fs_path"),
                                (*m.path).to_string());
                    pyld.insert(String::from("line"),
                                m.value.clone());
                    pyld.insert(String::from("timestamp"),
                                format_time(m.time));
                    Record {
                        data: serde_json::ser::to_vec(&pyld).unwrap(),
                    }
                }).collect(),
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
                    },
                    Err(err) => match err {
                        // The following errors cannot be recovered from. We
                        // drop the payload lines and move on to the next
                        // batch. We might choose to split the chunk smaller in
                        // the hopes that the failure is a result of a subset of
                        // the payload being wonky. This is an optimization for
                        // the future.
                        ResourceNotFound(rnf_err) => {
                            error!("Unable to write to resource, not found: {}", rnf_err);
                            break;
                        },
                        InvalidArgument(ia_err) => {
                            error!("Unable to write, invalid argument: {}", ia_err);
                            break;
                        },
                        HttpDispatch(hd_err) => {
                            error!("Unable to write, http dispatch: {}", hd_err);
                            break;
                        },
                        Validation(v_err) => {
                            error!("Unable to write, validation failure: {}", v_err);
                            break;
                        },
                        Unknown(u_err) => {
                            error!("Unable to write, unknown failure: {}", u_err);
                            break;
                        },
                        // The following errors are recoverable, potentially.
                        Credentials(c_err) => {
                            error!("Unable to write, credential failure: {}", c_err);
                        },
                        ServiceUnavailable(su_err) => {
                            error!("Service unavailable, will retry: {}", su_err);
                        }
                    }
                }
                attempts += 1;
            }
        }
        self.buffer.clear();
    }

    fn deliver(&mut self, _: Metric) {
        // nothing, intentionally
    }

    fn deliver_lines(&mut self, mut lines: Vec<LogLine>) {
        let l = &mut lines;
        self.buffer.append(l);
    }
}

#[inline]
fn format_time(time: i64) -> String {
    let naive_time = NaiveDateTime::from_timestamp(time, 0);
    let utc_time : DateTime<UTC> = DateTime::from_utc(naive_time, UTC);
    format!("{}", utc_time.format("%Y-%m-%dT%H:%M:%S%.3fZ"))
}
