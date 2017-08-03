mod file;
mod flush;
mod graphite;
mod internal;
mod native;
mod statsd;

pub use self::file::{FileServer, FileServerConfig};
pub use self::flush::FlushTimer;
pub use self::graphite::{Graphite, GraphiteConfig};
pub use self::internal::{report_full_telemetry, report_telemetry, Internal,
                         InternalConfig};
pub use self::native::{NativeServer, NativeServerConfig};
pub use self::statsd::{Statsd, StatsdConfig};

pub trait Source {
    fn run(&mut self) -> ();
}
