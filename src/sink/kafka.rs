//! Kafka sink for Raw events.
use futures::future::Future;
use metric::{self, LogLine, Telemetry};
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
    /// Total record publish retry failures. This occurs when the error signal does not include the original message.
    pub static ref KAFKA_PUBLISH_RETRY_FAILURE_SUM: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
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
    /// Maximum number of bytes that can be in-flight. Once we go over this, the
    /// valve closes. Default = 10Mb.
    pub max_message_bytes: usize,
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
            max_message_bytes: 10 * (1 << 20),
            flush_interval: 1,
        }
    }
}

type PublishResult = Option<Result<(i32, i64), (KafkaError, OwnedMessage)>>;

trait KafkaPublishable {
    fn finalize(&mut self) -> PublishResult;
}

struct KafkaPublishResult {
    inner: Option<DeliveryFuture>,
}

impl KafkaPublishable for KafkaPublishResult {
    fn finalize(&mut self) -> PublishResult {
        match self.inner {
            Some(ref mut future) => match future.wait() {
                Ok(r) => Some(r),
                _ => None,
            },
            None => None,
        }
    }
}

type BoxedKafkaPublishable = Box<KafkaPublishable + Send + Sync>;

trait KafkaMessageSender {
    fn try_payload(
        &self,
        topic: &str,
        payload: &[u8],
        key: &[u8],
    ) -> BoxedKafkaPublishable;
}

impl KafkaMessageSender for FutureProducer<EmptyContext> {
    fn try_payload(
        &self,
        topic: &str,
        payload: &[u8],
        key: &[u8],
    ) -> BoxedKafkaPublishable {
        Box::new(KafkaPublishResult {
            inner: Some(self.send_copy(
                topic,
                /* partition */ None,
                Some(payload),
                Some(key),
                /* timestamp */ None,
                /* block_ms */ 0,
            )),
        })
    }
}

// Enables testability of the global statics when tests are run in parallel.
trait StatsCollector {
    fn increment_publish(&self, inc: usize);
    fn increment_retry(&self, inc: usize);
    fn increment_publish_failed(&self, inc: usize);
    fn increment_retry_failed(&self, inc: usize);
    fn get_publish(&self) -> usize {
        0
    }
    fn get_publish_failed(&self) -> usize {
        0
    }
    fn get_retry(&self) -> usize {
        0
    }
    fn get_retry_failed(&self) -> usize {
        0
    }
}

struct DefaultStatsCollector;
impl StatsCollector for DefaultStatsCollector {
    fn increment_publish(&self, inc: usize) {
        KAFKA_PUBLISH_SUCCESS_SUM.fetch_add(inc, Ordering::Relaxed);
    }
    fn increment_retry(&self, inc: usize) {
        KAFKA_PUBLISH_RETRY_SUM.fetch_add(inc, Ordering::Relaxed);
    }
    fn increment_publish_failed(&self, inc: usize) {
        KAFKA_PUBLISH_FAILURE_SUM.fetch_add(inc, Ordering::Relaxed);
    }
    fn increment_retry_failed(&self, inc: usize) {
        KAFKA_PUBLISH_RETRY_FAILURE_SUM.fetch_add(inc, Ordering::Relaxed);
    }
}

type BoxedKafkaMessageSender = Box<KafkaMessageSender + Sync + Send>;

/// Kafka sink internal state.
pub struct Kafka {
    /// Name of the stream we are publishing to.
    topic_name: String,
    /// A message producers.
    producer: BoxedKafkaMessageSender,
    // In-flight messages.
    messages: Vec<BoxedKafkaPublishable>,
    /// Total byte length of in-flight messages. This is used to open and close
    /// the sink valve.
    message_bytes: usize,
    /// Maximum number of bytes that can be in-flight. Once we go over this,
    /// the valve closes.
    max_message_bytes: usize,
    /// How often (seconds) the in-flight messages are checked for delivery.
    flush_interval: u64,
    /// An object responsible for incrementing publication statistics.
    stats: Box<StatsCollector + Send + Sync>,
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
            producer: Box::new(producer_config.create::<FutureProducer<_>>().unwrap()),
            messages: Vec::new(),
            message_bytes: 0,
            max_message_bytes: config.max_message_bytes,
            flush_interval: config.flush_interval,
            stats: Box::new(DefaultStatsCollector),
        }
    }

    fn valve_state(&self) -> Valve {
        if self.message_bytes < self.max_message_bytes {
            Valve::Open
        } else {
            Valve::Closed
        }
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
        let future = self.producer.try_payload(
            &self.topic_name[..],
            bytes.as_slice(),
            key.as_bytes(),
        );
        self.messages.push(future);
        self.message_bytes += bytes.len();
    }

    fn flush(&mut self) {
        while !self.messages.is_empty() {
            let retry_payload_and_keys = self.await_inflight_messages();
            let new_messages = retry_payload_and_keys
                .iter()
                .filter_map(|message| {
                    let payload = message.payload();
                    let key = message.key();
                    if payload.is_some() && key.is_some() {
                        Some(self.producer.try_payload(
                            &self.topic_name[..],
                            payload.unwrap(),
                            key.unwrap(),
                        ))
                    } else {
                        error!("Unable to retry message. It was lost to the ether.");
                        self.stats.increment_retry_failed(1);
                        None
                    }
                })
                .collect();
            self.messages = new_messages;
        }
        self.message_bytes = 0;
    }

    fn flush_interval(&self) -> Option<u64> {
        Some(self.flush_interval)
    }

    fn shutdown(mut self) -> () {
        self.flush();
    }
}

impl Kafka {
    /// Wait on all in-flight messages, and return an `OwnedMessage` for each message
    /// that needs to be retried.
    fn await_inflight_messages(&mut self) -> Vec<OwnedMessage> {
        let mut inc_pub = 0usize;
        let mut inc_retry = 0usize;
        let mut inc_fail = 0usize;
        let result = self.messages
            .iter_mut()
            .filter_map(|future| {
                let result = future.finalize();
                match result {
                    Some(result) => match result {
                        Ok((_partition, _offset)) => {
                            inc_pub += 1;
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
                                    warn!("Kafka broker returned a recoverable error, will retry: {:?}", err);
                                    inc_retry += 1;
                                    Some(message)
                                }

                                _ => {
                                    error!("Kafka broker returned an unrecoverable error: {:?}", err);
                                    inc_fail += 1;
                                    None
                                }
                            },

                            _ => {
                                error!("Failed in send to kafka broker: {:?}", err);
                                inc_fail += 1;
                                None
                            }
                        },
                    },

                    _ => {
                        error!("Failed in send to kafka broker, operation canceled");
                        inc_fail += 1;
                        None
                    }
                }
            })
            .collect();
        self.stats.increment_publish(inc_pub);
        self.stats.increment_publish_failed(inc_fail);
        self.stats.increment_retry(inc_retry);
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::metric::Encoding;
    use rdkafka::message::Timestamp;
    use std::sync::{Arc, RwLock};

    struct MockKafkaPublishResult {
        return_value: PublishResult,
    }

    impl KafkaPublishable for MockKafkaPublishResult {
        fn finalize(&mut self) -> PublishResult {
            match self.return_value {
                Some(ref mut i) => match *i {
                    Ok(r) => Some(Ok(r)),
                    Err((ref e, ref m)) => {
                        let om = OwnedMessage::new(
                            m.key().map(|k| k.to_vec()),
                            m.payload().map(|p| p.to_vec()),
                            m.topic().to_owned(),
                            m.timestamp(),
                            m.partition(),
                            m.offset(),
                        );
                        Some(Err((e.clone(), om)))
                    }
                },
                _ => None,
            }
        }
    }

    struct MockKafkaSender;
    impl KafkaMessageSender for MockKafkaSender {
        fn try_payload(
            &self,
            _topic: &str,
            _payload: &[u8],
            _key: &[u8],
        ) -> BoxedKafkaPublishable {
            Box::new(MockKafkaPublishResult {
                return_value: Some(Ok((0, 1))),
            })
        }
    }

    #[derive(Debug)]
    struct RecordingStatsCollector {
        publish: Arc<RwLock<usize>>,
        retry: Arc<RwLock<usize>>,
        publish_failed: Arc<RwLock<usize>>,
        retry_failed: Arc<RwLock<usize>>,
    }
    impl StatsCollector for RecordingStatsCollector {
        fn increment_publish(&self, inc: usize) {
            *self.publish.write().unwrap() += inc;
        }
        fn increment_publish_failed(&self, inc: usize) {
            *self.publish_failed.write().unwrap() += inc;
        }
        fn increment_retry(&self, inc: usize) {
            *self.retry.write().unwrap() += inc;
        }
        fn increment_retry_failed(&self, inc: usize) {
            *self.retry_failed.write().unwrap() += inc;
        }
        fn get_publish(&self) -> usize {
            *self.publish.read().unwrap()
        }
        fn get_publish_failed(&self) -> usize {
            *self.publish_failed.read().unwrap()
        }
        fn get_retry(&self) -> usize {
            *self.retry.read().unwrap()
        }
        fn get_retry_failed(&self) -> usize {
            *self.retry_failed.read().unwrap()
        }
    }
    impl RecordingStatsCollector {
        fn new() -> Self {
            RecordingStatsCollector {
                publish: Arc::new(RwLock::new(0)),
                publish_failed: Arc::new(RwLock::new(0)),
                retry: Arc::new(RwLock::new(0)),
                retry_failed: Arc::new(RwLock::new(0)),
            }
        }
    }

    #[test]
    fn test_valve_closes_at_max_bytes() {
        let mut k = Kafka {
            topic_name: String::from("test-topic"),
            producer: Box::new(MockKafkaSender {}),
            messages: Vec::new(),
            message_bytes: 0,
            max_message_bytes: 10,
            flush_interval: 1,
            stats: Box::new(RecordingStatsCollector::new()),
        };

        assert_eq!(k.valve_state(), Valve::Open);

        k.deliver_raw(0, Encoding::Raw, vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);
        assert_eq!(k.valve_state(), Valve::Open);
        assert_eq!(k.message_bytes, 9);
        assert_eq!(k.messages.len(), 1);

        k.deliver_raw(0, Encoding::Raw, vec![10]);
        assert_eq!(k.valve_state(), Valve::Closed);
        assert_eq!(k.message_bytes, 10);
        assert_eq!(k.messages.len(), 2);

        k.flush();
        assert_eq!(k.valve_state(), Valve::Open);
        assert_eq!(k.message_bytes, 0);
        assert_eq!(k.messages.len(), 0);

        assert_eq!(k.stats.get_publish(), 2);
    }

    #[derive(Debug, PartialEq)]
    struct TopicKeyPayloadEntry {
        topic: String,
        payload: Vec<u8>,
        key: Vec<u8>,
    }
    #[derive(Clone)]
    struct RetryOnceMockKafkaSender {
        call_count: Arc<RwLock<i32>>,
        send_entries: Arc<RwLock<Vec<TopicKeyPayloadEntry>>>,
        error_type: KafkaError,
        fail_retry: bool,
    }
    impl KafkaMessageSender for RetryOnceMockKafkaSender {
        fn try_payload(
            &self,
            topic: &str,
            payload: &[u8],
            key: &[u8],
        ) -> BoxedKafkaPublishable {
            let mut entries = self.send_entries.write().unwrap();
            let entry = TopicKeyPayloadEntry {
                topic: topic.to_owned(),
                payload: payload.to_vec(),
                key: key.to_vec(),
            };
            entries.push(entry);

            let mut count = self.call_count.write().unwrap();
            *count += 1;
            if *count > 1 {
                Box::new(MockKafkaPublishResult {
                    return_value: Some(Ok((0, 1))),
                })
            } else {
                let om = OwnedMessage::new(
                    if self.fail_retry {
                        None
                    } else {
                        Some(key.to_vec())
                    },
                    if self.fail_retry {
                        None
                    } else {
                        Some(payload.to_vec())
                    },
                    topic.to_owned(),
                    Timestamp::NotAvailable,
                    -1,
                    -1,
                );
                Box::new(MockKafkaPublishResult {
                    return_value: Some(Err((self.error_type.clone(), om))),
                })
            }
        }
    }

    #[test]
    fn test_kafka_retryable_error_goes_through() {
        let retry_errors = vec![
            RDKafkaError::InvalidMessage,
            RDKafkaError::UnknownTopicOrPartition,
            RDKafkaError::LeaderNotAvailable,
            RDKafkaError::NotLeaderForPartition,
            RDKafkaError::RequestTimedOut,
            RDKafkaError::NetworkException,
            RDKafkaError::GroupLoadInProgress,
            RDKafkaError::GroupCoordinatorNotAvailable,
            RDKafkaError::NotCoordinatorForGroup,
            RDKafkaError::NotEnoughReplicas,
            RDKafkaError::NotEnoughReplicasAfterAppend,
            RDKafkaError::NotController,
        ];
        for error_type in retry_errors {
            let producer = RetryOnceMockKafkaSender {
                call_count: Arc::new(RwLock::new(0)),
                send_entries: Arc::new(RwLock::new(Vec::new())),
                error_type: KafkaError::MessageProduction(error_type),
                fail_retry: false,
            };
            let mut k = Kafka {
                topic_name: String::from("test-topic"),
                producer: Box::new(producer.clone()),
                messages: Vec::new(),
                message_bytes: 0,
                max_message_bytes: 1000,
                flush_interval: 1,
                stats: Box::new(RecordingStatsCollector::new()),
            };

            k.deliver_raw(1024, Encoding::Raw, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
            k.flush();

            let count = producer.call_count.read().unwrap();
            assert_eq!(*count, 2);

            let entries = producer.send_entries.read().unwrap();
            assert_eq!(
                entries[0],
                TopicKeyPayloadEntry {
                    topic: String::from("test-topic"),
                    key: format!("{:X}", 1024).as_bytes().to_vec(),
                    payload: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                }
            );
            assert_eq!(
                entries[1],
                TopicKeyPayloadEntry {
                    topic: String::from("test-topic"),
                    key: format!("{:X}", 1024).as_bytes().to_vec(),
                    payload: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                }
            );
            assert_eq!(k.stats.get_publish(), 1);
            assert_eq!(k.stats.get_retry(), 1);
        }
    }

    #[test]
    fn test_kafka_retry_with_message_loss() {
        let producer = RetryOnceMockKafkaSender {
            call_count: Arc::new(RwLock::new(0)),
            send_entries: Arc::new(RwLock::new(Vec::new())),
            error_type: KafkaError::MessageProduction(RDKafkaError::InvalidMessage),
            fail_retry: true,
        };
        let mut k = Kafka {
            topic_name: String::from("test-topic"),
            producer: Box::new(producer.clone()),
            messages: Vec::new(),
            message_bytes: 0,
            max_message_bytes: 1000,
            flush_interval: 1,
            stats: Box::new(RecordingStatsCollector::new()),
        };

        k.deliver_raw(1024, Encoding::Raw, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        k.flush();

        let count = producer.call_count.read().unwrap();
        assert_eq!(*count, 1);
        assert_eq!(k.stats.get_retry_failed(), 1);
    }

    #[test]
    fn test_unrecoverable_kafka_error() {
        let producer = RetryOnceMockKafkaSender {
            call_count: Arc::new(RwLock::new(0)),
            send_entries: Arc::new(RwLock::new(Vec::new())),
            error_type: KafkaError::MessageProduction(
                RDKafkaError::InvalidReplicaAssignment,
            ),
            fail_retry: true,
        };
        let mut k = Kafka {
            topic_name: String::from("test-topic"),
            producer: Box::new(producer.clone()),
            messages: Vec::new(),
            message_bytes: 0,
            max_message_bytes: 1000,
            flush_interval: 1,
            stats: Box::new(RecordingStatsCollector::new()),
        };

        k.deliver_raw(1024, Encoding::Raw, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        k.flush();

        let count = producer.call_count.read().unwrap();
        assert_eq!(*count, 1);
        assert_eq!(k.stats.get_publish_failed(), 1);
    }

    #[test]
    fn test_unreachable_kafka_error() {
        let producer = RetryOnceMockKafkaSender {
            call_count: Arc::new(RwLock::new(0)),
            send_entries: Arc::new(RwLock::new(Vec::new())),
            error_type: KafkaError::FutureCanceled,
            fail_retry: true,
        };
        let mut k = Kafka {
            topic_name: String::from("test-topic"),
            producer: Box::new(producer.clone()),
            messages: Vec::new(),
            message_bytes: 0,
            max_message_bytes: 1000,
            flush_interval: 1,
            stats: Box::new(RecordingStatsCollector::new()),
        };

        k.deliver_raw(1024, Encoding::Raw, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        k.flush();

        let count = producer.call_count.read().unwrap();
        assert_eq!(*count, 1);
        assert_eq!(k.stats.get_publish_failed(), 1);
    }

    struct FutureFailedMockKafkaSender;
    impl KafkaMessageSender for FutureFailedMockKafkaSender {
        fn try_payload(
            &self,
            _topic: &str,
            _payload: &[u8],
            _key: &[u8],
        ) -> BoxedKafkaPublishable {
            Box::new(MockKafkaPublishResult { return_value: None })
        }
    }

    #[test]
    fn test_kafka_send_future_failed() {
        let mut k = Kafka {
            topic_name: String::from("test-topic"),
            producer: Box::new(FutureFailedMockKafkaSender {}),
            messages: Vec::new(),
            message_bytes: 0,
            max_message_bytes: 10,
            flush_interval: 1,
            stats: Box::new(RecordingStatsCollector::new()),
        };

        k.deliver_raw(1024, Encoding::Raw, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        k.flush();
        assert_eq!(k.stats.get_publish_failed(), 1);
    }

    #[test]
    fn test_default_stats_collector() {
        // Note this is the only test that should be touching the global statics.
        // Things become flakey when tests are run in parallel otherwise.
        let stats = DefaultStatsCollector {};

        stats.increment_publish(1);
        assert_eq!(KAFKA_PUBLISH_SUCCESS_SUM.load(Ordering::Relaxed), 1);
        assert_eq!(KAFKA_PUBLISH_RETRY_SUM.load(Ordering::Relaxed), 0);
        assert_eq!(KAFKA_PUBLISH_FAILURE_SUM.load(Ordering::Relaxed), 0);
        assert_eq!(KAFKA_PUBLISH_RETRY_FAILURE_SUM.load(Ordering::Relaxed), 0);

        stats.increment_retry(1);
        assert_eq!(KAFKA_PUBLISH_SUCCESS_SUM.load(Ordering::Relaxed), 1);
        assert_eq!(KAFKA_PUBLISH_RETRY_SUM.load(Ordering::Relaxed), 1);
        assert_eq!(KAFKA_PUBLISH_FAILURE_SUM.load(Ordering::Relaxed), 0);
        assert_eq!(KAFKA_PUBLISH_RETRY_FAILURE_SUM.load(Ordering::Relaxed), 0);

        stats.increment_publish_failed(1);
        assert_eq!(KAFKA_PUBLISH_SUCCESS_SUM.load(Ordering::Relaxed), 1);
        assert_eq!(KAFKA_PUBLISH_RETRY_SUM.load(Ordering::Relaxed), 1);
        assert_eq!(KAFKA_PUBLISH_FAILURE_SUM.load(Ordering::Relaxed), 1);
        assert_eq!(KAFKA_PUBLISH_RETRY_FAILURE_SUM.load(Ordering::Relaxed), 0);

        stats.increment_retry_failed(1);
        assert_eq!(KAFKA_PUBLISH_SUCCESS_SUM.load(Ordering::Relaxed), 1);
        assert_eq!(KAFKA_PUBLISH_RETRY_SUM.load(Ordering::Relaxed), 1);
        assert_eq!(KAFKA_PUBLISH_FAILURE_SUM.load(Ordering::Relaxed), 1);
        assert_eq!(KAFKA_PUBLISH_RETRY_FAILURE_SUM.load(Ordering::Relaxed), 1);
    }
}
