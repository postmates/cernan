use metric;
use mpsc;
use std::str;
use std::time::Instant;
use std::fmt;
use time;

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
pub fn send<S>(ctx: S, chans: &mut Vec<mpsc::Sender<metric::Event>>, event: &metric::Event)
    where S: Into<String> + fmt::Display
{
    for mut chan in chans {
        let snd_time = Instant::now();
        chan.send(event);
    }
}

pub trait Source {
    fn run(&mut self) -> ();
}
