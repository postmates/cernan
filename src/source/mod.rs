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

pub use self::graphite::Graphite;
pub use self::statsd::Statsd;
pub use self::file::FileServer;
pub use self::flush::FlushTimer;
pub use self::federation_receiver::FederationReceiver;

#[inline]
pub fn send<S>(ctx: S, chans: &mut Vec<mpsc::Sender<metric::Event>>, event: &metric::Event)
    where S: Into<String> + fmt::Display
{
    for mut chan in chans {
        let snd_time = Instant::now();
        chan.send(event);
        trace!("[{}] channel send {:?} to {} elapsed (ns): {}",
               ctx,
               event,
               chan.name(),
               time::elapsed_ns(snd_time));
    }
}

pub trait Source {
    fn run(&mut self) -> ();
}
