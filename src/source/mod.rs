use metric;
use mpsc;
use std::fmt;

mod graphite;
mod statsd;
mod file;
mod flush;
mod federation_receiver;

pub use self::graphite::{Graphite, GraphiteConfig};
pub use self::statsd::{Statsd, StatsdConfig};
pub use self::file::{FileServer, FileServerConfig};
pub use self::flush::FlushTimer;
pub use self::federation_receiver::{FederationReceiver, FederationReceiverConfig};

#[inline]
pub fn send<S>(_ctx: S, chans: &mut Vec<mpsc::Sender<metric::Event>>, event: metric::Event)
    where S: Into<String> + fmt::Display
{
    for mut chan in chans {
        chan.send(event.clone());
    }
}

pub trait Source {
    fn run(&mut self) -> ();
}
