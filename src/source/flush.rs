use metric;
use source::Source;
use std::thread::sleep;
use std::time::Duration;
use util;
use util::send;

/// The source of all flush pulses. See `FlushTimer::run` for more details.
pub struct FlushTimer {
    chans: util::Channel,
}

impl FlushTimer {
    /// Create a new FlushTimer. This will not produce a new thread, that must
    /// be managed by the end-user.
    pub fn new(chans: util::Channel) -> FlushTimer {
        FlushTimer { chans: chans }
    }
}

impl Source for FlushTimer {
    fn run(&mut self) {
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
            send(&mut self.chans, metric::Event::TimerFlush(idx));
        }
    }
}
