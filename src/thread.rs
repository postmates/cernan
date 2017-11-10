//! Mio enabled threading library.

extern crate mio;

use std::thread;
use constants;

/// Mio enabled thread state.
pub struct ThreadHandle {

    /// JoinHandle for the executing thread.
    pub handle : thread::JoinHandle<()>,

    /// Readiness signal used to notify the given thread when an event is ready
    /// to be consumed on the SYSTEM channel.
    readiness : mio::SetReadiness,
}

impl ThreadHandle {

    /// Spawns a new thread executing the provided closure.
    pub fn new<F>(f : F) -> ThreadHandle where
        F: Send + 'static + FnOnce(mio::Poll) -> () {

        let poller = mio::Poll::new().unwrap();
        let (registration, readiness) = mio::Registration::new2();

        return ThreadHandle {
            readiness: readiness,

            handle: thread::spawn(move || {
                poller.register(
                    &registration,
                    constants::SYSTEM,
                    mio::Ready::readable(),
                    mio::PollOpt::edge()).expect("Failed to register system pipe");

                f(poller);
            }),
        }
    }

    /// Join the given Thread, blocking until it exits.
    pub fn join(self) {
        self.handle.join().expect("Failed to join child thread!");
    }

    /// Gracefully shutdown the given Thread, blocking until it exists.
    ///
    /// Note - It is the responsability of the developer to ensure
    /// that thread logic polls for events occuring on the SYSTEM token.
    pub fn shutdown(self) {
        self.readiness.set_readiness(mio::Ready::readable()).expect("Failed to notify child thread of shutdown!");
        self.join();
    }
}

/// Spawns a new thread executing the provided closure.
pub fn spawn<F>(f: F) -> ThreadHandle where
     F: Send + 'static + FnOnce(mio::Poll) -> ()
{
        let poller = mio::Poll::new().unwrap();
        let (registration, readiness) = mio::Registration::new2();

        return ThreadHandle {
            readiness: readiness,

            handle: thread::spawn(move || {
                poller.register(
                    &registration,
                    constants::SYSTEM,
                    mio::Ready::readable(),
                    mio::PollOpt::edge()).expect("Failed to register system pipe");

                f(poller);
            }),
        }
}
