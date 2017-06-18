use chrono::datetime::DateTime;
use chrono::naive::datetime::NaiveDateTime;
use chrono::offset::utc::UTC;
use metric::{LogLine, Telemetry};

use rusoto_core::{DefaultCredentialsProvider, Region};
use rusoto_core::default_tls_client;
use rusoto_firehose::{KinesisFirehose, KinesisFirehoseClient, PutRecordBatchInput,
                      Record};
use rusoto_firehose::PutRecordBatchError::*;

use serde_json;
use serde_json::Map;
use serde_json::value::Value;
use sink::{Sink, Valve};
use source::report_full_telemetry;
use std::sync;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct FirehoseConfig {
    pub delivery_stream: Option<String>,
    pub batch_size: usize,
    pub region: Option<Region>,
    pub config_path: Option<String>,
    pub flush_interval: u64,
    pub telemetry_error: f64,
}

impl Default for FirehoseConfig {
    fn default() -> FirehoseConfig {
        FirehoseConfig {
            delivery_stream: None,
            batch_size: 400,
            region: None,
            config_path: None,
            flush_interval: 60,
            telemetry_error: 0.001,
        }
    }
}

pub struct Firehose {
    buffer: Vec<LogLine>,
    delivery_stream_name: String,
    region: Region,
    batch_size: usize,
    flush_interval: u64,
    telemetry_error: f64,
}

impl Firehose {
    pub fn new(config: FirehoseConfig) -> Firehose {
        Firehose {
            buffer: Vec::new(),
            delivery_stream_name:
                config.delivery_stream.expect("delivery_stream cannot be None"),
            region: config.region.expect("region cannot be None"),
            batch_size: config.batch_size,
            flush_interval: config.flush_interval,
            telemetry_error: config.telemetry_error,
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
                records: chunk
                    .iter()
                    .filter(|m| m.value.len() < 1_024_000)
                    .map(|m| {
                        let mut pyld = Map::new();
                        pyld.insert(String::from("Path"),
                                    Value::String((*m.path).to_string()));
                        pyld.insert(String::from("Payload"),
                                    Value::String(m.value.clone()));
                        pyld.insert(String::from("timestamp"),
                                    Value::String(format_time(m.time)));
                        pyld.insert(String::from("Uuid"),
                                    Value::String(Uuid::new_v4()
                                                      .hyphenated()
                                                      .to_string()));
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
                        report_full_telemetry("cernan.sinks.firehose.records.delivery",
                                              1.0,
                                              self.telemetry_error,
                                              None,
                                              Some(vec![("delivery_stream_name",
                                                         prbi.delivery_stream_name
                                                             .as_str())]));
                        report_full_telemetry("cernan.sinks.firehose.records.total_delivered",
                                              prbi.records.len() as f64,
                                              self.telemetry_error,
                                              None,
                                              Some(vec![("delivery_stream_name",
                                                         prbi.delivery_stream_name
                                                             .as_str())]));
                        let failed_put_count = prbo.failed_put_count;
                        if failed_put_count > 0 {
                            report_full_telemetry("cernan.sinks.firehose.records.total_failed",
                                                  failed_put_count as f64,
                                                  self.telemetry_error,
                                                  None,
                                                  Some(vec![("delivery_stream_name",
                                                        prbi.delivery_stream_name
                                                            .as_str())]));
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
                                report_full_telemetry("cernan.sinks.firehose.error.resource_not_found",
                                                 1.0,
                                                 self.telemetry_error,
                                                 None,
                                                 Some(vec![("delivery_stream_name",
                                                        prbi.delivery_stream_name
                                                            .as_str())]));
                                error!("Unable to write to resource, not found: {}",
                                       rnf_err);
                                break;
                            }
                            InvalidArgument(ia_err) => {
                                report_full_telemetry("cernan.sinks.firehose.error.invalid_argument",
                                                 1.0,
                                                 self.telemetry_error,
                                                 None,
                                                 Some(vec![("delivery_stream_name",
                                                        prbi.delivery_stream_name
                                                            .as_str())]));
                                error!("Unable to write, invalid argument: {}",
                                       ia_err);
                                break;
                            }
                            HttpDispatch(hd_err) => {
                                report_full_telemetry("cernan.sinks.firehose.error.http_dispatch",
                                                 1.0,
                                                 self.telemetry_error,
                                                 None,
                                                 Some(vec![("delivery_stream_name",
                                                        prbi.delivery_stream_name
                                                            .as_str())]));
                                error!("Unable to write, http dispatch: {}", hd_err);
                                break;
                            }
                            Validation(v_err) => {
                                report_full_telemetry("cernan.sinks.firehose.error.validation",
                                                 1.0,
                                                 self.telemetry_error,
                                                 None,
                                                 Some(vec![("delivery_stream_name",
                                                        prbi.delivery_stream_name
                                                            .as_str())]));
                                error!("Unable to write, validation failure: {}",
                                       v_err);
                                break;
                            }
                            Unknown(u_err) => {
                                report_full_telemetry("cernan.sinks.firehose.error.unknown",
                                                 1.0,
                                                 self.telemetry_error,
                                                 None,
                                                 Some(vec![("delivery_stream_name",
                                                        prbi.delivery_stream_name
                                                            .as_str())]));
                                error!("Unable to write, unknown failure: {}", u_err);
                                break;
                            }
                            // The following errors are recoverable, potentially.
                            Credentials(c_err) => {
                                report_full_telemetry("cernan.sinks.firehose.error.credentials",
                                                 1.0,
                                                 self.telemetry_error,
                                                 None,
                                                 Some(vec![("delivery_stream_name",
                                                        prbi.delivery_stream_name
                                                            .as_str())]));
                                error!("Unable to write, credential failure: {}",
                                       c_err);
                            }
                            ServiceUnavailable(su_err) => {
                                report_full_telemetry("cernan.sinks.firehose.error.service_unavailable",
                                                 1.0,
                                                 self.telemetry_error,
                                                 None,
                                                 Some(vec![("delivery_stream_name",
                                                        prbi.delivery_stream_name
                                                            .as_str())]));
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
