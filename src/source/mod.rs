//! Staging ground for all sources
//!
//! In cernan a `Source` is a place where all `metric::Event` come from, feeding
//! down into the source's forwards for further processing. Statsd is a source
//! that creates `Telemetry`, `FileServer` is a source that creates `LogLine`s.
use mio;
use std::marker::PhantomData;
use crate::thread;
use crate::util;

mod avro;
mod file;
mod flush;
mod graphite;
mod internal;
mod native;
mod nonblocking;
mod statsd;
mod tcp;

pub use self::avro::Avro;
pub use self::file::{FileServer, FileServerConfig};
pub use self::flush::{FlushTimer, FlushTimerConfig, flushes_per_second};
pub use self::graphite::{Graphite, GraphiteConfig};
pub use self::internal::{report_full_telemetry, Internal, InternalConfig};
pub use self::native::{NativeServer, NativeServerConfig};
use self::nonblocking::{BufferedPayload, PayloadErr};
pub use self::statsd::{Statsd, StatsdConfig, StatsdParseConfig};
pub use self::tcp::{TCPConfig, TCPStreamHandler, TCP};

/// Generic interface used to capture global source configuration
/// parameters as well as source specific parameters.
///
/// Stored configuration is consumed when the source is spawned,
/// resulting in a new thread which executes the given source.
pub struct RunnableSource<S, SConfig>
where
    S: Send + Source<SConfig>,
    SConfig: 'static + Send + Clone,
{
    chans: util::Channel,
    source: S,

    // Yes, compiler, we know that we aren't storing
    // anything of type SConfig.
    config: PhantomData<SConfig>,
}

impl<S, SConfig> RunnableSource<S, SConfig>
where
    S: Send + Source<SConfig>,
    SConfig: 'static + Send + Clone,
{
    /// Constructs a new RunnableSource.
    pub fn new(chans: util::Channel, config: SConfig) -> Self {
        RunnableSource {
            chans: chans,
            config: PhantomData,
            source: S::init(config),
        }
    }

    /// Spawns a thread corresponding to the given RunnableSource, consuming
    /// the given RunnableSource in the process.
    pub fn run(self) -> thread::ThreadHandle {
        thread::spawn(move |poller| self.source.run(self.chans, poller))
    }
}

/// cernan Source, the originator of all `metric::Event`.
///
/// A cernan Source creates all `metric::Event`, doing so by listening to
/// network IO, reading from files, etc etc. All sources push into the routing
/// topology.
pub trait Source<SConfig>
where
    Self: 'static + Send + Sized,
    SConfig: 'static + Send + Clone,
{
    /// Constructs a so-called runnable source for the given Source and
    /// config.`  See RunnableSource.
    fn new(chans: util::Channel, config: SConfig) -> RunnableSource<Self, SConfig> {
        RunnableSource::<Self, SConfig>::new(chans, config)
    }

    /// Initializes state for the given Source.
    fn init(config: SConfig) -> Self;

    /// Run method invoked by RunnableSource.
    /// It is from this method that Sources produce metric::Events.
    fn run(self, chans: util::Channel, poller: mio::Poll) -> ();
}
