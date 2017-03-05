use metric;
use source::Source;
use std::thread::sleep;
use std::time::Duration;
use util;
use util::send;

pub struct FlushTimer {
    interval: u64,
}

impl FlushTimer {
    pub fn new(interval: u64) -> FlushTimer {
        FlushTimer { interval: interval }
    }
}

impl Source for FlushTimer {
    fn run(&mut self, mut chans: util::Channel) {
        let duration = Duration::new(self.interval, 0);
        debug!("flush-interval: {:?}", duration);
        loop {
            sleep(duration);
            send("flush", &mut chans, metric::Event::TimerFlush);
        }
    }
}
