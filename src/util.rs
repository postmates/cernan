use hopper;
use metric;

pub type Channel = Vec<hopper::Sender<metric::Event>>;

pub fn send(chans: &mut Channel, event: metric::Event) {
    let max: usize = chans.len().saturating_sub(1);
    if max == 0 {
        chans[0].send(event)
    } else {
        for mut chan in &mut chans[1..] {
            chan.send(event.clone());
        }
        chans[0].send(event);
    }
}
