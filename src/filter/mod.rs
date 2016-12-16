use metric;
use mpsc;
use time;
use source;

mod programmable_filter;

pub use self::programmable_filter::{ProgrammableFilter, ProgrammableFilterConfig};

#[derive(Debug)]
pub enum FilterError {
    NoSuchFunction(&'static str, metric::Event),
}

fn name_in_fe(fe: &FilterError) -> &'static str {
    match fe {
        &FilterError::NoSuchFunction(n, _) => n,
    }
}

fn event_in_fe(fe: FilterError) -> metric::Event {
    match fe {
        FilterError::NoSuchFunction(_, m) => m,
    }
}

pub trait Filter {
    fn process<'a>(&mut self, event: metric::Event) -> Result<Vec<metric::Event>, FilterError>;
    fn run(&mut self,
           mut recv: mpsc::Receiver<metric::Event>,
           mut chans: Vec<mpsc::Sender<metric::Event>>) {
        let mut attempts = 0;
        loop {
            time::delay(attempts);
            match recv.next() {
                None => attempts += 1,
                Some(event) => {
                    attempts = 0;
                    match self.process(event) {
                        Ok(events) => {
                            for ev in events {
                                source::send("filter", &mut chans, ev)
                            }
                        }
                        Err(fe) => {
                            error!("Failed to run filter with error: {:?}", name_in_fe(&fe));
                            let event = event_in_fe(fe);
                            source::send("filter.error_path", &mut chans, event);
                        }
                    }
                }
            }
        }
    }
}
