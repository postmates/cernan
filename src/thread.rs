//! Mio enabled threading library.

extern crate mio;

use constants;
use std::thread;

pub type Poll = mio::Poll;
pub type Events = mio::Events;

/// Mio enabled thread state.
pub struct ThreadHandle {
    /// JoinHandle for the executing thread.
    pub handle: thread::JoinHandle<()>,

    /// Readiness signal used to notify the given thread when an event is ready
    /// to be consumed on the SYSTEM channel.
    readiness: mio::SetReadiness,
}

/// Trait for stoppable processes.
pub trait Stoppable {
    /// Join the given process, blocking until it exits.
    fn join(self) -> ();

    /// Gracefully shutdown the process, blocking until exit.
    fn shutdown(self) -> ();
}

impl Stoppable for ThreadHandle {
    /// Join the given Thread, blocking until it exits.
    fn join(self) {
        self.handle.join().expect("Failed to join child thread!");
    }

    /// Gracefully shutdown the given Thread, blocking until it exists.
    ///
    /// Note - It is the responsability of the developer to ensure
    /// that thread logic polls for events occuring on the SYSTEM token.
    fn shutdown(self) {
        self.readiness
            .set_readiness(mio::Ready::readable())
            .expect("Failed to notify child thread of shutdown!");
        self.join();
    }
}

/// Spawns a new thread executing the provided closure.
pub fn spawn<F>(f: F) -> ThreadHandle
where
    F: Send + 'static + FnOnce(mio::Poll) -> (),
{
    let poller = mio::Poll::new().unwrap();
    let (registration, readiness) = mio::Registration::new2();

    ThreadHandle {
        readiness: readiness,

        handle: thread::spawn(move || {
            poller
                .register(
                    &registration,
                    constants::SYSTEM,
                    mio::Ready::readable(),
                    mio::PollOpt::edge(),
                )
                .expect("Failed to register system pipe");

            f(poller);
        }),
    }
}
