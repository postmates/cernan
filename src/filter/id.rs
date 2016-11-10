use filter;
use metric;
use mpsc;

#[derive(Default)]
pub struct Id {
}

impl Id {
    pub fn new() -> Id {
        Id {}
    }
}

impl filter::Filter for Id {
    fn process<'a>(&mut self,
                   event: &'a mut metric::Event,
                   chans: &'a mut Vec<mpsc::Sender<metric::Event>>)
                   -> Vec<(&'a mut mpsc::Sender<metric::Event>, Vec<metric::Event>)> {
        let event = event.clone();
        vec![(&mut chans[0], vec![event])]
    }
}
