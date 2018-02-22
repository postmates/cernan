use metric;
use mio;
use source;
use std::thread::sleep;
use std::time::Duration;
use util;
use util::send;

/// The source of all flush pulses.
pub struct FlushTimer;

/// Nil config for `FlushTimer`.
#[derive(Clone, Debug, Deserialize)]
pub struct FlushTimerConfig;

impl source::Source<FlushTimerConfig> for FlushTimer {
    /// Create a new FlushTimer. This will not produce a new thread, that must
    /// be managed by the end-user.
    fn init(_config: FlushTimerConfig) -> Self {
        FlushTimer {}
    }

    fn run(self, mut chans: util::Channel, _poller: mio::Poll) -> () {
        let one_second = Duration::new(1, 0);
        // idx will _always_ increase. If it's kept at u64 or greater it will
        // overflow long past the collapse of our industrial civilization only
        // so long as the intervals we sleep are kept to once a second or so.
        //
        // Heh, even past the length of the universe, really.
        //
        // Point being, there's a theoretical overflow problem here but it's not
        // going to be hit in practice.
        let mut idx: u64 = 0;
        loop {
            // We start with TimerFlush(1) as receivers start with
            // TimerFlush(0). This will update their last_flush_idx seen at
            // system boot.
            idx += 1;
            sleep(one_second);
            send(&mut chans, metric::Event::TimerFlush(idx));
        }
    }
}
