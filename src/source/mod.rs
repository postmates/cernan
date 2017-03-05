use util;

mod graphite;
mod statsd;
mod file;
mod flush;
mod native;

pub use self::file::{FileServer, FileServerConfig};
pub use self::flush::FlushTimer;
pub use self::graphite::{Graphite, GraphiteConfig};
pub use self::native::{NativeServer, NativeServerConfig};
pub use self::statsd::{Statsd, StatsdConfig};

pub trait Source {
    fn run(&mut self, forwards: util::Channel) -> ();
}
