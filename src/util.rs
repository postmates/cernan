//! Utility module, a grab-bag of functionality

extern crate mio;

use hopper;
use metric;
use constants;
use std;

/// A vector of `hopper::Sender`s.
pub type Channel = Vec<hopper::Sender<metric::Event>>;

/// Send a `metric::Event` into a `Channel`.
pub fn send(chans: &mut Channel, event: metric::Event) {
    let max: usize = chans.len().saturating_sub(1);
    if max == 0 {
        chans[0].send(event)
    } else {
        for chan in &mut chans[1..] {
            chan.send(event.clone());
        }
        chans[0].send(event);
    }
}

/// Determine the state of a buffering queue, whether open or closed.
///
/// Cernan is architected to be a push-based system. It copes with demand rushes
/// by buffering to disk -- via the hopper queues -- and rejecting memory-based
/// storage with overload signals. This signal, in particular, limits the amount
/// of information delivered to a filter / sink by declaring that said filter /
/// sink's input 'valve' is closed. Exactly how and why a filter / sink declares
/// its valve state is left to the implementation.
pub enum Valve {
    /// In the `Open` state a filter / sink will accept new inputs
    Open,
    /// In the `Closed` state a filter / sink will reject new inputs, backing
    /// them up in the communication queue.
    Closed,
}


pub struct ChildThread {
    pub handle : std::thread::JoinHandle<()>,

    readiness : mio::SetReadiness,
}

impl ChildThread {
    pub fn new<F>(f : F) -> ChildThread where
        F: Send + 'static + FnOnce(mio::Poll) -> () {

        let poller = mio::Poll::new().unwrap();
        let (registration, readiness) = mio::Registration::new2();

        return ChildThread {
            readiness: readiness,

            handle: std::thread::spawn(move || {
                poller.register(
                    &registration,
                    constants::SYSTEM,
                    mio::Ready::readable(),
                    mio::PollOpt::edge()).expect("Failed to register system pipe");

                f(poller);
            }),
        }
    }

    pub fn shutdown(self) {
        self.readiness.set_readiness(mio::Ready::readable());
        self.handle.join();
    }
}
