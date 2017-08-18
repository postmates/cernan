//! Staging ground for all sources
//!
//! In cernan a `Source` is a place where all `metric::Event` come from, feeding
//! down into the source's forwards for further processing. Statsd is a source
//! that creates `Telemetry`, `FileServer` is a source that creates `LogLine`s.
mod file;
mod flush;
mod graphite;
mod internal;
mod native;
mod statsd;

pub use self::file::{FileServer, FileServerConfig};
pub use self::flush::FlushTimer;
pub use self::graphite::{Graphite, GraphiteConfig};
pub use self::internal::{report_full_telemetry, Internal,
                         InternalConfig};
pub use self::native::{NativeServer, NativeServerConfig};
pub use self::statsd::{Statsd, StatsdConfig};

/// cernan Source, the originator of all `metric::Event`.
///
/// A cernan Source creates all `metric::Event`, doing so by listening to
/// network IO, reading from files, etc etc. All sources push into the routing
/// topology.
pub trait Source {
    /// Run the Source, the exact mechanism here depends on the Source itself.
    fn run(&mut self) -> ();
}
