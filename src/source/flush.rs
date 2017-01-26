use hopper;
use metric;
use source::Source;
use std::thread::sleep;
use std::time::Duration;

pub type Channel = hopper::Sender<metric::Event>;
pub struct FlushTimer {
    chan: Channel,
    interval: u64,
}

impl FlushTimer {
    pub fn new(chan: Channel, interval: u64) -> FlushTimer {
        FlushTimer {
            chan: chan,
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
            self.chan.send(metric::Event::TimerFlush);
        }
    }
}
