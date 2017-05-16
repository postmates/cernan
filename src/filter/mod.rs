use hopper;
use metric;
use time;
use util;

mod programmable_filter;

pub use self::programmable_filter::{ProgrammableFilter, ProgrammableFilterConfig};

#[derive(Debug)]
pub enum FilterError {
    NoSuchFunction(&'static str, metric::Event),
}

fn name_in_fe(fe: &FilterError) -> &'static str {
    match *fe {
        FilterError::NoSuchFunction(n, _) => n,
    }
}

fn event_in_fe(fe: FilterError) -> metric::Event {
    match fe {
        FilterError::NoSuchFunction(_, m) => m,
    }
}

pub trait Filter {
    fn process(&mut self,
               event: metric::Event,
               res: &mut Vec<metric::Event>)
               -> Result<(), FilterError>;
    fn run(&mut self,
           recv: hopper::Receiver<metric::Event>,
           mut chans: util::Channel) {
        let mut attempts = 0;
        let mut events = Vec::with_capacity(64);
        let mut recv = recv.into_iter();
        loop {
            time::delay(attempts);
            match recv.next() {
                None => attempts += 1,
                Some(event) => {
                    attempts = 0;
                    match self.process(event, &mut events) {
                        Ok(()) => {
                            for ev in events.drain(..) {
                                util::send("filter", &mut chans, ev)
                            }
                        }
                        Err(fe) => {
                            error!("Failed to run filter with error: {:?}",
                                   name_in_fe(&fe));
                            let event = event_in_fe(fe);
                            util::send("filter.error_path", &mut chans, event);
                        }
                    }
                }
            }
        }
    }
}
