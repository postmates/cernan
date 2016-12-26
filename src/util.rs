use hopper;
use metric;
use std::fmt;

pub type Channel = Vec<hopper::Sender<metric::Event>>;

#[inline]
pub fn send<S>(_ctx: S, chans: &mut Channel, event: metric::Event)
    where S: Into<String> + fmt::Display
{
    let max = chans.len() - 1;
    if max == 0 {
        chans[0].send(event)
    } else {
        for mut chan in &mut chans[1..] {
            chan.send(event.clone());
        }
        chans[0].send(event);
    }
}
