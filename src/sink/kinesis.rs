//! Kinesis sink for Raw events.

extern crate base64;
extern crate rusoto_core;
extern crate rusoto_kinesis;

use rusoto_core::default_tls_client;
use rusoto_core::DefaultCredentialsProvider;
use rusoto_kinesis::Kinesis as RusotoKinesis;

use rusoto_kinesis::{KinesisClient, PutRecordInput, PutRecordError};

use sink::Sink;
use hyper;
use metric;
use metric::{LogLine, Telemetry};
use std::ops::DerefMut;
use std::sync;
use util::Valve;

/// Config options for Kinesis config.
#[derive(Clone, Debug, Deserialize)]
pub struct KinesisConfig {
    stream_name: Option<String>,
}

impl Default for KinesisConfig {
    fn default() -> KinesisConfig {
        KinesisConfig {
            stream_name: None,
        }
    }
}

/// Kinesis sink internal state.
pub struct Kinesis {
    client: Box<KinesisClient<DefaultCredentialsProvider, hyper::client::Client>>,

    stream_name: String,
}

impl Sink<KinesisConfig> for Kinesis {

    fn init(config: KinesisConfig) -> Self {
        if config.stream_name.is_none() {
            panic!("No Kinesis stream provided!");
        };

        let tls = default_tls_client().unwrap();
        let provider = DefaultCredentialsProvider::new().unwrap();
        let region = rusoto_core::region::default_region();
        Kinesis {
            client: Box::new(KinesisClient::new(tls, provider, region)),
            stream_name: config.stream_name.unwrap(),
        }
    }

    fn valve_state(&self) -> Valve {
        // TODO - Something more clever.
        Valve::Open
    }

    fn deliver(&mut self, _: sync::Arc<Option<Telemetry>>) -> () {
        // Discard point
    }

    fn deliver_line(&mut self, _: sync::Arc<Option<LogLine>>) -> () {
        // Discard line
    }

    fn deliver_raw(&mut self, _encoding: metric::Encoding, bytes: Vec<u8>) {
        //  Try to publish to Kinesis
        let client = self.client.deref_mut();
        let input = PutRecordInput {
            stream_name: self.stream_name.clone(),
            data: base64::encode(&bytes).into_bytes(),
            explicit_hash_key: None,
            partition_key: String::from("foobar"),
            sequence_number_for_ordering: None,
        };

        loop {
            match client.put_record(&input) {
                Ok(_output) => {
                    // Nothing to see here.  All is well.
                    break;
                }

                Err(PutRecordError::ProvisionedThroughputExceeded(_)) => {
                    info!("Provisioned throughput exceeded on {:?}.  Retrying...", self.stream_name)
                }

                Err(err) => {
                    panic!("Fatal exception during put_record: {:?}", err);
                }
            }
        }
    }

    fn flush(&mut self) {
        // We don't care about flushes.
    }

    fn flush_interval(&self) -> Option<u64> {
        Some(1)
    }

    fn shutdown(self) -> () {
        // Nothing to do as we don't batch internally.
    }
}
