use crate::metric::{LogLine, Telemetry};
use crate::util::HashMap;
use uuid::Uuid;

/// Supported event encodings.
#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub enum Encoding {
    /// Raw bytes, no encoding.
    Raw,
    /// Avro
    Avro,
    /// JSON
    JSON,
}

/// Metadata: additional data attached to an event
pub type Metadata = HashMap<Vec<u8>, Vec<u8>>;

/// Event: the central cernan datastructure
///
/// Event is the heart of cernan, the enumeration that cernan works on in all
/// cases. The enumeration fields drive sink / source / filter operations
/// depending on their implementation.
#[allow(clippy::large_enum_variant)]
#[derive(PartialEq, Debug, Serialize, Deserialize, Clone)]
pub enum Event {
    /// A wrapper for `metric::Telemetry`. See its documentation for more
    /// detail.
    Telemetry(Telemetry),
    /// A wrapper for `metric::LogLine`. See its documentation for more
    /// detail.
    Log(LogLine),
    /// A flush pulse signal. The `TimerFlush` keeps a counter of the total
    /// flushes made in this cernan's run. See `source::Flush` for the origin of
    /// these pulses in cernan operation.
    TimerFlush(u64),
    /// Shutdown event which marks the location in the queue after which no
    /// more events will appear.  It is expected that after receiving this
    /// marker the given source will exit cleanly.
    Shutdown,
    /// Raw, encoded bytes.
    Raw {
        /// Ordering value used by some sinks accepting Raw events.
        order_by: u64,
        /// Encoding for the included bytes.
        encoding: Encoding,
        /// Encoded payload.
        bytes: Vec<u8>,
        /// Metadata used by some sinks
        metadata: Option<Metadata>,
        /// Connection ID of the source on which this raw event was received
        connection_id: Option<Uuid>,
    },
}

impl Event {
    /// Determine if an event is a `TimerFlush`.
    pub fn is_timer_flush(&self) -> bool {
        match *self {
            Event::TimerFlush(_) => true,
            _ => false,
        }
    }

    /// Retrieve the timestamp from an `Event` if such exists. `TimerFlush` has
    /// no sensible timestamp -- being itself a mechanism _of_ time, not inside
    /// time -- and these `Event`s will always return None.
    pub fn timestamp(&self) -> Option<i64> {
        match *self {
            Event::Telemetry(ref telem) => Some(telem.timestamp),
            Event::Log(ref log) => Some(log.time),
            Event::TimerFlush(_) | Event::Shutdown | Event::Raw { .. } => None,
        }
    }
}

impl Event {
    /// Create a new `Event::Telemetry` from an existing `metric::Telemetry`.
    #[inline]
    pub fn new_telemetry(metric: Telemetry) -> Event {
        Event::Telemetry(metric)
    }

    /// Create a new `Event::Log` from an existing `metric::LogLine`.
    #[inline]
    pub fn new_log(log: LogLine) -> Event {
        Event::Log(log)
    }
}
