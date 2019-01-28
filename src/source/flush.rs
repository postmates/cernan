use crate::metric;
use crate::source;
use crate::util;
use crate::util::send;
use mio;
use std::thread::sleep;
use std::time::Duration;

/// The source of all flush pulses.
pub struct FlushTimer;

/// Nil config for `FlushTimer`.
#[derive(Clone, Debug, Deserialize)]
pub struct FlushTimerConfig;

/// Returns the number of discrete flushes per second.
pub fn flushes_per_second() -> u64 {
    // With 100 flushes per second, we have a maximum precision of 10ms.
    // Anything more than this is probably asking for the hopper queues to be
    // filled more by flushes than metrics.
    100
}

impl source::Source<FlushTimerConfig> for FlushTimer {
    /// Create a new FlushTimer. This will not produce a new thread, that must
    /// be managed by the end-user.
    fn init(_config: FlushTimerConfig) -> Self {
        FlushTimer {}
    }

    fn run(self, mut chans: util::Channel, _poller: mio::Poll) {
        let flush_duration = Duration::from_millis(1000 / flushes_per_second());
        // idx will _always_ increase. If it's kept at u64 or greater it will
        // overflow long past the collapse of our industrial civilization even
        // if the flush interval is set to a millisecond.
        //
        // Point being, there's a theoretical overflow problem here but it's not
        // going to be hit in practice.
        let mut idx: u64 = 0;
        loop {
            // We start with TimerFlush(1) as receivers start with
            // TimerFlush(0). This will update their last_flush_idx seen at
            // system boot.
            idx += 1;
            sleep(flush_duration);
            send(&mut chans, metric::Event::TimerFlush(idx));
        }
    }
}
