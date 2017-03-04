use metric;
use source::Source;
use std::thread::sleep;
use std::time::Duration;
use util;
use util::send;

pub struct FlushTimer {
    chans: util::Channel,
    interval: u64,
}

impl FlushTimer {
    pub fn new(chans: util::Channel, interval: u64) -> FlushTimer {
        FlushTimer {
            chans: chans,
            interval: interval,
        }
    }
}

impl Source for FlushTimer {
    fn run(&mut self) {
        let duration = Duration::new(self.interval, 0);
        debug!("flush-interval: {:?}", duration);
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
            sleep(duration);
            send("flush", &mut self.chans, metric::Event::TimerFlush(idx));
        }
    }
}
