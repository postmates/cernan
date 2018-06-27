//! Mio enabled threading library.
use constants;
use mio;
use std::option;
use std::thread;

/// Event polling structure. Alias of `mio::Poll`.
pub type Poll = mio::Poll;
/// Events buffer type. Alias of `mio::Events`.
pub type Events = mio::Events;

/// Mio enabled thread state.
pub struct ThreadHandle {
    /// JoinHandle for the executing thread.
    pub handle: thread::JoinHandle<()>,

    /// Readiness signal used to notify the given thread when an event is ready
    /// to be consumed on the SYSTEM channel.
    shutdown_event: mio::SetReadiness,

    /// Poller used by the parent to poll for child events.  Currently,
    /// the only event emitted by child threads signals that they are joinable.
    joinable: option::Option<mio::SetReadiness>,
}

/// Trait for stoppable processes.
pub trait Stoppable {
    /// Join the given process, blocking until it exits.
    fn join(self) -> ();

    /// Is the given thread joinable?
    fn ready(&self) -> bool;

    /// Gracefully shutdown the process, blocking until exit.
    fn shutdown(self) -> ();
}

impl Stoppable for ThreadHandle {
    /// Join the given Thread, blocking until it exits.
    fn join(self) {
        self.handle.join().expect("Failed to join child thread!");
    }

    fn ready(&self) -> bool {
        if let Some(joinable) = self.joinable.clone() {
            joinable.readiness().is_readable()
        } else {
            trace!("Only threads started with spawn2 are compatible with ready().");
            false
        }
    }

    /// Gracefully shutdown the given Thread, blocking until it exists.
    ///
    /// Note - It is the responsability of the developer to ensure
    /// that thread logic polls for events occuring on the SYSTEM token.
    fn shutdown(self) {
        self.shutdown_event
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
    let child_poller = mio::Poll::new().unwrap();
    let (shutdown_event_registration, shutdown_event) = mio::Registration::new2();
    ThreadHandle {
        shutdown_event: shutdown_event,
        joinable: None,

        handle: thread::spawn(move || {
            child_poller
                .register(
                    &shutdown_event_registration,
                    constants::SYSTEM,
                    mio::Ready::readable(),
                    mio::PollOpt::edge(),
                )
                .expect("Failed to register system pipe");

            f(child_poller);
        }),
    }
}

/// As spawn() exception the given SetReadiness will be set readable
/// when the thread returns normally.
pub fn spawn2<F>(joinable: mio::SetReadiness, f: F) -> ThreadHandle
where
    F: Send + 'static + FnOnce(mio::Poll) -> (),
{
    let child_poller = mio::Poll::new().unwrap();
    let (shutdown_event_registration, shutdown_event) = mio::Registration::new2();
    ThreadHandle {
        shutdown_event: shutdown_event,
        joinable: Some(joinable.clone()),

        handle: thread::spawn(move || {
            child_poller
                .register(
                    &shutdown_event_registration,
                    constants::SYSTEM,
                    mio::Ready::readable(),
                    mio::PollOpt::edge(),
                )
                .expect("Failed to register system pipe");

            f(child_poller);
            joinable.set_readiness(mio::Ready::readable()).expect("Failed to set joinable!");
        }),

    }

}
