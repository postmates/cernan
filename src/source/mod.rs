//! Staging ground for all sources
//!
//! In cernan a `Source` is a place where all `metric::Event` come from, feeding
//! down into the source's forwards for further processing. Statsd is a source
//! that creates `Telemetry`, `FileServer` is a source that creates `LogLine`s.
use thread;
use util;

mod avro;
mod file;
mod flush;
mod graphite;
mod internal;
mod native;
mod statsd;
mod tcp;

pub use self::avro::Avro;
pub use self::file::{FileServer, FileServerConfig};
pub use self::flush::{FlushTimer, FlushTimerConfig};
pub use self::graphite::{Graphite, GraphiteConfig};
pub use self::internal::{report_full_telemetry, Internal, InternalConfig};
pub use self::native::{NativeServer, NativeServerConfig};
pub use self::statsd::{Statsd, StatsdConfig, StatsdParseConfig};
pub use self::tcp::{TCPConfig, TCP};

/// cernan Source, the originator of all `metric::Event`.
///
/// A cernan Source creates all `metric::Event`, doing so by listening to
/// network IO, reading from files, etc etc. All sources push into the routing
/// topology.
pub trait Source<TConfig> {
    /// Constructs initial state for the given source.
    fn new(chans: util::Channel, config: TConfig) -> Self;

    /// Run the Source, consuming initial state and returning a
    /// handle to the running thread.
    fn run(self) -> thread::ThreadHandle;
}
