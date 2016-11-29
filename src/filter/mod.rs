use metric;
use mpsc;
use time;

mod programmable_filter;

pub use self::programmable_filter::{ProgrammableFilter, ProgrammableFilterConfig};

pub trait Filter {
    // TODO There should be a way to send a modified event to some channels, not
    // to others etc.
    fn process<'a>(&mut self,
                   event: &'a mut metric::Event,
                   chans: &'a mut Vec<mpsc::Sender<metric::Event>>)
                   -> Vec<(&'a mut mpsc::Sender<metric::Event>, Vec<metric::Event>)>;
    fn run(&mut self,
           mut recv: mpsc::Receiver<metric::Event>,
           mut chans: Vec<mpsc::Sender<metric::Event>>) {
        let mut attempts = 0;
        loop {
            time::delay(attempts);
            match recv.next() {
                None => attempts += 1,
                Some(mut event) => {
                    attempts = 0;
                    for &mut (ref mut chan, ref events) in
                        &mut self.process(&mut event, &mut chans) {
                        for ev in events {
                            chan.send(ev)
                        }
                    }
                }
            }
        }
    }
}
