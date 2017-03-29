mod file;
mod flush;
mod graphite;
mod internal;
mod native;
mod statsd;

pub use self::file::{FileServer, FileServerConfig};
pub use self::flush::FlushTimer;
pub use self::graphite::{Graphite, GraphiteConfig};
pub use self::internal::{Internal, InternalConfig};
pub use self::native::{NativeServer, NativeServerConfig};
pub use self::statsd::{Statsd, StatsdConfig};

pub trait Source {
    fn run(&mut self) -> ();
}
