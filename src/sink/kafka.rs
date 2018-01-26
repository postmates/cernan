//! Kafka sink for Raw events.
use futures::future::Future;
use metric;
use metric::{LogLine, Telemetry};
use rdkafka::client::EmptyContext;
use rdkafka::config::ClientConfig;
use rdkafka::error::{KafkaError, RDKafkaError};
use rdkafka::message::{Message, OwnedMessage};
use rdkafka::producer::FutureProducer;
use rdkafka::producer::future_producer::DeliveryFuture;
use sink::Sink;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use util::Valve;

lazy_static! {
    /// Total records published.
    pub static ref KAFKA_PUBLISH_SUCCESS_SUM: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    /// Total record publish retries.
    pub static ref KAFKA_PUBLISH_RETRY_SUM: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    /// Total record publish failures.
    pub static ref KAFKA_PUBLISH_FAILURE_SUM: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
}

/// Config options for Kafka config.
#[derive(Clone, Debug, Deserialize)]
pub struct KafkaConfig {
    /// Canonical name for the given Kafka sink.
    pub config_path: Option<String>,
    /// Kafka topic to publish to.
    pub topic_name: Option<String>,
    /// Kafka brokers. This is a comma-separated list of host or host:port.
    pub brokers: Option<String>,
    /// Underlying librdkafka configuration.
    pub rdkafka_config: Option<HashMap<String, String>>,
    /// How often (seconds) the in-flight messages are checked for delivery.
    /// Default = 1 second
    pub flush_interval: u64,
}

impl Default for KafkaConfig {
    fn default() -> KafkaConfig {
        KafkaConfig {
            config_path: None,
            topic_name: None,
            brokers: None,
            rdkafka_config: None,
            flush_interval: 1,
        }
    }
}

/// Kafka sink internal state.
pub struct Kafka {
    /// Name of the stream we are publishing to.
    topic_name: String,
    /// A message producers.
    producer: FutureProducer<EmptyContext>,
    // In-flight messages.
    messages: Vec<DeliveryFuture>,
    /// How often (seconds) the in-flight messages are checked for delivery.
    flush_interval: u64,
}

impl Sink<KafkaConfig> for Kafka {
    fn init(config: KafkaConfig) -> Self {
        if config.topic_name.is_none() {
            panic!("No Kafka topic name provided!");
        }
        if config.brokers.is_none() {
            panic!("No Kafka brokers provided!")
        }

        let mut producer_config = ClientConfig::new();
        if let Some(ref map) = config.rdkafka_config {
            for (key, value) in map.iter() {
                producer_config.set(key, value);
            }
        }
        producer_config.set("bootstrap.servers", &config.brokers.unwrap()[..]);

        Kafka {
            topic_name: config.topic_name.unwrap(),
            producer: producer_config.create::<FutureProducer<_>>().unwrap(),
            messages: Vec::new(),
            flush_interval: config.flush_interval,
        }
    }

    fn valve_state(&self) -> Valve {
        // We never close up shop.
        Valve::Open
    }

    fn deliver(&mut self, _: Arc<Option<Telemetry>>) -> () {
        // Discard point
    }

    fn deliver_line(&mut self, _: Arc<Option<LogLine>>) -> () {
        // Discard line
    }

    /// Fire off the given event to librdkafka. That library handles buffering and
    /// batching internally.
    fn deliver_raw(
        &mut self,
        order_by: u64,
        _encoding: metric::Encoding,
        bytes: Vec<u8>,
    ) {
        let key = format!("{:X}", order_by);
        let future = self.try_payload(bytes.as_slice(), key.as_bytes());
        self.messages.push(future);
    }

    fn flush(&mut self) {
        let retry_payload_and_keys: Vec<Option<OwnedMessage>> = self.messages
            .iter_mut()
            .map(|future| {
                let result = future.wait();
                match result {
                    Ok(inner) => match inner {
                        Ok((_partition, _offset)) => {
                            KAFKA_PUBLISH_SUCCESS_SUM.fetch_add(1, Ordering::Relaxed);
                            None
                        }

                        Err((err, message)) => match err {
                            KafkaError::MessageProduction(err) => match err {
                                RDKafkaError::InvalidMessage
                                | RDKafkaError::UnknownTopicOrPartition
                                | RDKafkaError::LeaderNotAvailable
                                | RDKafkaError::NotLeaderForPartition
                                | RDKafkaError::RequestTimedOut
                                | RDKafkaError::NetworkException
                                | RDKafkaError::GroupLoadInProgress
                                | RDKafkaError::GroupCoordinatorNotAvailable
                                | RDKafkaError::NotCoordinatorForGroup
                                | RDKafkaError::NotEnoughReplicas
                                | RDKafkaError::NotEnoughReplicasAfterAppend
                                | RDKafkaError::NotController => {
                                    KAFKA_PUBLISH_RETRY_SUM
                                        .fetch_add(1, Ordering::Relaxed);
                                    Some(message)
                                }

                                _ => {
                                    error!("Kafka broker returned an unrecoverable error: {:?}", err);
                                    KAFKA_PUBLISH_FAILURE_SUM
                                        .fetch_add(1, Ordering::Relaxed);
                                    None
                                }
                            },

                            _ => {
                                error!("Failed in send to kafka broker: {:?}", err);
                                KAFKA_PUBLISH_FAILURE_SUM
                                    .fetch_add(1, Ordering::Relaxed);
                                None
                            }
                        },
                    },

                    _ => {
                        error!("Failed in send to kafka broker: {:?}", result);
                        KAFKA_PUBLISH_FAILURE_SUM.fetch_add(1, Ordering::Relaxed);
                        None
                    }
                }
            })
            .collect();
        let new_messages = retry_payload_and_keys
            .iter()
            .map(|maybe_retry| match *maybe_retry {
                Some(ref message) => {
                    let payload = message.payload();
                    let key = message.key();
                    if payload.is_some() && key.is_some() {
                        Some(self.try_payload(payload.unwrap(), key.unwrap()))
                    } else {
                        error!("Unable to retry message. It was lost to the ether.");
                        None
                    }
                }
                None => None,
            })
            .filter(|x| x.is_some())
            .map(|x| x.unwrap())
            .collect();
        self.messages = new_messages;
    }

    fn flush_interval(&self) -> Option<u64> {
        Some(self.flush_interval)
    }

    fn shutdown(mut self) -> () {
        self.flush();
    }
}

impl Kafka {
    fn try_payload(&self, payload: &[u8], key: &[u8]) -> DeliveryFuture {
        self.producer.send_copy(
            &self.topic_name[..],
            /* partition */ None,
            Some(&payload[..]),
            Some(&key[..]),
            /* timestamp */ None,
            /* block_ms */ 0,
        )
    }
}
