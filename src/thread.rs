//! Mio enabled threading library.
use crate::constants;
use crate::util;
use mio;
use std::option;
use std::sync;
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

/// mio Eventable ThreadPool.
pub struct ThreadPool {
    /// thread_id counter.
    thread_id: usize,
    /// Listing of all the joinable threads in the pool.
    joinable: sync::Arc<sync::Mutex<Vec<usize>>>,
    /// Mapping of thread_id to ThreadHandle.
    threads: util::HashMap<usize, ThreadHandle>,
    /// Mio readiness flagging when threads finish execution.
    thread_event_readiness: option::Option<mio::SetReadiness>,
}

impl ThreadPool {
    /// Construct a new ThreadPool.
    pub fn new(thread_events_readiness: option::Option<mio::SetReadiness>) -> Self {
        ThreadPool {
            thread_id: 0,
            joinable: sync::Arc::new(sync::Mutex::new(Vec::new())),
            thread_event_readiness: thread_events_readiness,
            threads: util::HashMap::default(),
        }
    }

    /// Spawn a new thread and assign it to the pool.
    pub fn spawn<F>(&mut self, f: F) -> usize
    where
        F: Send + 'static + FnOnce(mio::Poll) -> (),
    {
        let id = self.next_thread_id();
        let joinable_arc = self.joinable.clone();
        let thread_event_readiness = self.thread_event_readiness.clone();
        let handler = spawn(move |poller| {
            f(poller);

            let mut joinable = joinable_arc.lock().unwrap();
            joinable.push(id);

            if let Some(readiness) = thread_event_readiness {
                readiness
                    .set_readiness(mio::Ready::readable())
                    .expect("Failed to flag readiness for ThreadPool event!");
            }
        });
        self.threads.insert(id, handler);
        id
    }

    fn next_thread_id(&mut self) -> usize {
        let thread_id = self.thread_id;
        self.thread_id += 1;
        thread_id
    }

    /// Block on completion of all executing threads.
    pub fn join(mut self) -> Vec<usize> {
        self.threads.drain().for_each(|(_, h)| h.join());
        self.join_ready()
    }

    /// Join all completed threads.
    pub fn join_ready(&mut self) -> Vec<usize> {
        let mut joinable = self.joinable.lock().unwrap();
        let mut joined = Vec::new();
        while let Some(id) = joinable.pop() {
            if let Some(handle) = self.threads.remove(&id) {
                handle.join();
            }
            joined.push(id);
        }
        joined
    }

    /// Serially signal shutdown and block for completion of all threads.
    pub fn shutdown(mut self) -> Vec<usize> {
        self.threads.drain().for_each(|(_, h)| h.shutdown());
        self.join_ready()
    }
}
