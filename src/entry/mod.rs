use hopper;
use metric::Event;

pub trait Entry {
    // fn run();
    fn get_config(&self) -> &EntryConfig;
    fn run1(&mut self, _forwards: Vec<hopper::Sender<Event>>, recv: hopper::Receiver<Event>);
}

pub trait EntryConfig {
    fn get_forwards(&self) -> Vec<String> {
        Vec::new()
    }
    fn get_config_path(&self) -> &String;
}
