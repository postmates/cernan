use metric;
use hopper;
use std::thread::sleep;
use std::time::Duration;

use super::send;
use source::Source;

pub struct FlushTimer {
    chans: Vec<hopper::Sender<metric::Event>>,
    interval: u64,
}

impl FlushTimer {
    pub fn new(chans: Vec<hopper::Sender<metric::Event>>, interval: u64) -> FlushTimer {
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
