use metric;
use mpsc;
use std::str;
use std::time::Instant;
use std::fmt;

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
        // NOTE this elapsed time is wrong! We must correctly include whole
        // seconds. STD makes this goofy, I believe chrono makes this simpler.
        trace!("[{}] channel send {:?} to {} elapsed (ns): {}",
               ctx,
               event,
               chan.name(),
               snd_time.elapsed().subsec_nanos());
    }
}

pub trait Source {
    fn run(&mut self) -> ();
}
