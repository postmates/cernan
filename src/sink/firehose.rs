use chrono::datetime::DateTime;
use chrono::naive::datetime::NaiveDateTime;
use chrono::offset::utc::UTC;
use metric::{LogLine, Telemetry};

use rusoto::{DefaultCredentialsProvider, Region};
use rusoto::default_tls_client;
use rusoto::firehose::{KinesisFirehoseClient, PutRecordBatchInput, Record};
use rusoto::firehose::PutRecordBatchError::*;

use serde_json;
use serde_json::Map;
use serde_json::value::Value;
use sink::{Sink, Valve};
use source::report_telemetry;
use std::sync;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct FirehoseConfig {
    pub delivery_stream: String,
    pub batch_size: usize,
    pub region: Region,
    pub config_path: String,
    pub flush_interval: u64,
}

pub struct Firehose {
    buffer: Vec<LogLine>,
    delivery_stream_name: String,
    region: Region,
    batch_size: usize,
    flush_interval: u64,
}

impl Firehose {
    pub fn new(config: FirehoseConfig) -> Firehose {
        Firehose {
            buffer: Vec::new(),
            delivery_stream_name: config.delivery_stream,
            region: config.region,
            batch_size: config.batch_size,
            flush_interval: config.flush_interval,
        }
    }
}

impl Sink for Firehose {
    fn flush_interval(&self) -> Option<u64> {
        Some(self.flush_interval)
    }

    fn flush(&mut self) {
        let provider = DefaultCredentialsProvider::new().unwrap();
        let dispatcher = default_tls_client().unwrap();
        let client = KinesisFirehoseClient::new(dispatcher, provider, self.region);

        if self.buffer.is_empty() {
            return;
        }

        for chunk in self.buffer.chunks(self.batch_size) {
            let prbi = PutRecordBatchInput {
                delivery_stream_name: self.delivery_stream_name.clone(),
                records: chunk.iter()
                    .filter(|m| m.value.len() < 1_024_000)
                    .map(|m| {
                        let mut pyld = Map::new();
                        pyld.insert(String::from("Path"), Value::String((*m.path).to_string()));
                        pyld.insert(String::from("Payload"), Value::String(m.value.clone()));
                        pyld.insert(String::from("timestamp"),
                                    Value::String(format_time(m.time)));
                        pyld.insert(String::from("Uuid"),
                                    Value::String(Uuid::new_v4().hyphenated().to_string()));
                        for &(ref k, ref v) in m.tags.iter() {
                            pyld.insert(k.clone(), Value::String(v.clone()));
                        }
                        for &(ref k, ref v) in m.fields.iter() {
                            pyld.insert(k.clone(), Value::String(v.clone()));
                        }
                        Record { data: serde_json::ser::to_vec(&pyld).unwrap() }
                    })
                    .collect(),
            };
            loop {
                match client.put_record_batch(&prbi) {
                    Ok(prbo) => {
                        debug!("Wrote {} records to delivery stream {}",
                               prbi.records.len(),
                               prbi.delivery_stream_name);
                        report_telemetry(format!("cernan.sinks.firehose.{}.records.delivery",
                                                 prbi.delivery_stream_name),
                                         1.0);
                        report_telemetry(format!("cernan.sinks.firehose.{}.records.\
                                                  total_delivered",
                                                 prbi.delivery_stream_name),
                                         prbi.records.len() as f64);
                        let failed_put_count = prbo.failed_put_count;
                        if failed_put_count > 0 {
                            report_telemetry(format!("cernan.sinks.firehose.{}.records.\
                                                      total_failed",
                                                     prbi.delivery_stream_name),
                                             failed_put_count as f64);
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
                                report_telemetry(format!("cernan.sinks.firehose.{}.error.\
                                                          resource_not_found",
                                                         prbi.delivery_stream_name),
                                                 1.0);
                                error!("Unable to write to resource, not found: {}", rnf_err);
                                break;
                            }
                            InvalidArgument(ia_err) => {
                                report_telemetry(format!("cernan.sinks.firehose.{}.error.\
                                                          invalid_argument",
                                                         prbi.delivery_stream_name),
                                                 1.0);
                                error!("Unable to write, invalid argument: {}", ia_err);
                                break;
                            }
                            HttpDispatch(hd_err) => {
                                report_telemetry(format!("cernan.sinks.firehose.{}.error.\
                                                          http_dispatch",
                                                         prbi.delivery_stream_name),
                                                 1.0);
                                error!("Unable to write, http dispatch: {}", hd_err);
                                break;
                            }
                            Validation(v_err) => {
                                report_telemetry(format!("cernan.sinks.firehose.{}.error.\
                                                          validation",
                                                         prbi.delivery_stream_name),
                                                 1.0);
                                error!("Unable to write, validation failure: {}", v_err);
                                break;
                            }
                            Unknown(u_err) => {
                                report_telemetry(format!("cernan.sinks.firehose.{}.error.unknown",
                                                         prbi.delivery_stream_name),
                                                 1.0);
                                error!("Unable to write, unknown failure: {}", u_err);
                                break;
                            }
                            // The following errors are recoverable, potentially.
                            Credentials(c_err) => {
                                report_telemetry(format!("cernan.sinks.firehose.{}.error.\
                                                          credentials",
                                                         prbi.delivery_stream_name),
                                                 1.0);
                                error!("Unable to write, credential failure: {}", c_err);
                            }
                            ServiceUnavailable(su_err) => {
                                report_telemetry(format!("cernan.sinks.firehose.{}.error.\
                                                          service_unavailable",
                                                         prbi.delivery_stream_name),
                                                 1.0);
                                error!("Service unavailable, will retry: {}", su_err);
                            }
                        }
                    }
                }

            }
        }
        self.buffer.clear();
    }

    fn deliver(&mut self, _: sync::Arc<Option<Telemetry>>) -> () {
        // nothing, intentionally
    }

    fn deliver_line(&mut self, mut lines: sync::Arc<Option<LogLine>>) -> () {
        let line: LogLine = sync::Arc::make_mut(&mut lines).take().unwrap();
        self.buffer.append(&mut vec![line]);
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
