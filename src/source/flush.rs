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
        loop {
            sleep(duration);
            send("flush", &mut self.chans, metric::Event::TimerFlush);
        }
    }
}
