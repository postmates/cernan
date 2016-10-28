#![deny(missing_docs)]
//! durable multi-producer, single-consumer
//!
//! This module provides a durable version of the rust standard
//! [mpsc](https://doc.rust-lang.org/std/sync/mpsc/). The ambition here is to
//! support mpsc style communication without allocating unbounded amounts of
//! memory or dropping inputs on the floor. The durable mpsc will potentially
//! allocate unbounded amounts of _disk_ space but you can't have everything.
//!
//! # Inside Baseball
//!
//! How does durable mpsc work? From the outside, it looks very much like a
//! named pipe in Unix. You supply a name to either `channel_2` or
//! `channel_with_max_bytes_3` and you push bytes in and out. In private, this
//! name is used to create a directory under `data_dir` and this directory gets
//! filled up with monotonically increasing files. The on-disk structure looks
//! like this:
//!
//! ```text
//! data-dir/
//!    sink-name0/
//!       0
//!       1
//!    sink-name1/
//!       0
//! ```
//!
//! You'll notice exports of Sender and Receiver in this module's
//! namespace. These are the structures that back the send and receive side of
//! the named channel. The Senders--there may be multiples of them--are
//! responsible for _creating_ "queue files". In the above,
//! `data-dir/sink-name*/*` are queue files. These files are treated as
//! append-only logs by the Senders. The Receivers trawl through these logs to
//! read the data serialized there.
//!
//! ## Won't this fill up my disk?
//!
//! Nope! Each Sender has a notion of the maximum bytes it may read--which you
//! can set explicitly when creating a channel with
//! `channel_with_max_bytes`--and once the Sender has gone over that limit it'll
//! attempt to mark the queue file as read-only and create a new file. The
//! Receiver is programmed to read its current queue file until it reaches EOF
//! and finds the file is read-only, at which point it deletes the file--it is
//! the only reader--and moves on to the next.
//!
//! ## What kind of filesystem options will I need?
//!
//! The durable mspc is intended to work on any crazy old filesystem with any
//! options, even at high concurrency. As common filesystems do not support
//! interleaving [small atomic
//! writes](https://stackoverflow.com/questions/32851672/is-overwriting-a-small-file-atomic-on-ext4)
//! mpsc limits itself to one exclusive Sender or one exclusive Receiver at a
//! time. This potentially limits the concurrency of mpsc but maintains data
//! integrity.
mod receiver;
mod sender;

pub use self::receiver::Receiver;
pub use self::sender::Sender;

use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;
use std::sync::{Arc, Mutex};

/// PRIVATE -- exposed via `Sender::new`
#[derive(Default, Debug)]
pub struct FsSync {
    bytes_written: usize,
    writes_to_read: usize,
    sender_seq_num: usize,
}

/// PRIVATE -- exposed via `Sender::new`
pub type FSLock = Arc<Mutex<FsSync>>;

/// Create a (Sender, Reciever) pair in a like fashion to [`std::sync::mpsc::channel`](https://doc.rust-lang.org/std/sync/mpsc/fn.channel.html)
///
/// This function creates a Sender and Receiver pair with name `name` whose
/// queue files are stored in `data_dir`. The Sender is clonable.
pub fn channel<T>(name: &str, data_dir: &Path) -> (Sender<T>, Receiver<T>)
    where T: Serialize + Deserialize
{
    channel_with_max_bytes(name, data_dir, 1_048_576 * 100)
}

/// Create a (Sender, Reciever) pair in a like fashion to [`std::sync::mpsc::channel`](https://doc.rust-lang.org/std/sync/mpsc/fn.channel.html)
///
/// This function creates a Sender and Receiver pair with name `name` whose
/// queue files are stored in `data_dir`. The Sender is clonable.
///
/// This function gives control to the user over the maximum size of the durable
/// channel's queue files as `max_bytes`, though not the total disk allocation
/// that may be made. This function is largely useful for testing purposes. See
/// the tests in `mpsc::test` for more.
pub fn channel_with_max_bytes<T>(name: &str,
                                 data_dir: &Path,
                                 max_bytes: usize)
                                 -> (Sender<T>, Receiver<T>)
    where T: Serialize + Deserialize
{
    let root = data_dir.join(name);
    let snd_root = root.clone();
    let rcv_root = root.clone();
    if !root.is_dir() {
        fs::create_dir_all(root).expect("could not create directory");
    }
    let fs_lock = Arc::new(Mutex::new(FsSync::default()));

    let sender = sender::Sender::new(name, &snd_root, max_bytes, fs_lock.clone());
    let receiver = Receiver::new(name, &rcv_root, fs_lock);
    (sender, receiver)
}

#[cfg(test)]
mod test {
    extern crate tempdir;
    extern crate quickcheck;

    use super::*;
    use std::thread;
    use self::quickcheck::{QuickCheck, TestResult};

    #[test]
    fn one_item_round_trip() {
        let dir = tempdir::TempDir::new("cernan").unwrap();
        let (mut snd, mut rcv) = channel("one_item_round_trip", dir.path());

        snd.send(&1);

        assert_eq!(Some(1), rcv.next());
    }

    #[test]
    fn zero_item_round_trip() {
        let dir = tempdir::TempDir::new("cernan").unwrap();
        let (mut snd, mut rcv) = channel("zero_item_round_trip", dir.path());

        assert_eq!(None, rcv.next());

        snd.send(&1);
        assert_eq!(Some(1), rcv.next());
    }

    #[test]
    fn round_trip() {
        fn rnd_trip(max_bytes: usize, evs: Vec<Vec<u32>>) -> TestResult {
            let dir = tempdir::TempDir::new("cernan").unwrap();
            let (mut snd, mut rcv) =
                channel_with_max_bytes("round_trip_order_preserved", dir.path(), max_bytes);

            for ev in evs.clone() {
                snd.send(&ev);
            }

            for ev in evs {
                assert_eq!(Some(ev), rcv.next());
            }
            TestResult::passed()
        }
        QuickCheck::new()
            .tests(100)
            .max_tests(1000)
            .quickcheck(rnd_trip as fn(usize, Vec<Vec<u32>>) -> TestResult);
    }

    #[test]
    fn round_trip_small_max_bytes() {
        fn rnd_trip(evs: Vec<Vec<u32>>) -> TestResult {
            let max_bytes: usize = 128;
            let dir = tempdir::TempDir::new("cernan").unwrap();
            let (mut snd, mut rcv) =
                channel_with_max_bytes("small_max_bytes", dir.path(), max_bytes);

            for ev in evs.clone() {
                snd.send(&ev);
            }

            let mut total = evs.len();
            for ev in evs {
                println!("REMAINING: {}", total);
                assert_eq!(Some(ev), rcv.next());
                total -= 1;
            }
            TestResult::passed()
        }
        QuickCheck::new()
            .tests(100)
            .max_tests(1000)
            .quickcheck(rnd_trip as fn(Vec<Vec<u32>>) -> TestResult);
    }

    #[test]
    fn concurrent_snd_and_rcv_round_trip() {
        let max_bytes: usize = 512;
        let dir = tempdir::TempDir::new("cernan").unwrap();
        println!("CONCURRENT SND_RECV TESTDIR: {:?}", dir);
        let (snd, mut rcv) = channel_with_max_bytes("concurrent_snd_and_rcv_small_max_bytes",
                                                    dir.path(),
                                                    max_bytes);

        let max_thrs = 32;
        let max_sz = 1000;

        // construct the payload to send repeatedly 'pyld' and the final payload
        // the sender should receive
        let mut tst_pylds = Vec::new();
        for i in 0..(max_sz * max_thrs) {
            tst_pylds.push(i);
        }

        let mut joins = Vec::new();

        // start our receiver thread
        joins.push(thread::spawn(move || {
            // assert that we receive every element in tst_pylds by pulling a new
            // value from the rcv, then marking it out of tst_pylds
            for _ in 0..(max_sz * max_thrs) {
                loop {
                    if let Some(nxt) = rcv.next() {
                        let idx = tst_pylds.binary_search(&nxt).expect("DID NOT FIND ELEMENT");
                        tst_pylds.remove(idx);
                        break;
                    }
                }
            }
            assert!(tst_pylds.is_empty());
        }));

        // start all our sender threads and blast away
        for i in 0..max_thrs {
            let mut thr_snd = snd.clone();
            joins.push(thread::spawn(move || {
                let base = i * max_sz;
                for p in 0..max_sz {
                    thr_snd.send(&(base + p));
                }
            }));
        }

        // wait until the senders are for sure done
        for jh in joins {
            jh.join().expect("Uh oh, child thread paniced!");
        }
    }

    #[test]
    fn qc_concurrent_snd_and_rcv_round_trip() {
        fn snd_rcv(evs: Vec<Vec<u32>>) -> TestResult {
            let max_bytes: usize = 512;
            let dir = tempdir::TempDir::new("cernan").unwrap();
            println!("CONCURRENT SND_RECV TESTDIR: {:?}", dir);
            let (snd, mut rcv) = channel_with_max_bytes("concurrent_snd_and_rcv_small_max_bytes",
                                                        dir.path(),
                                                        max_bytes);

            let max_thrs = 32;

            let mut joins = Vec::new();

            // start our receiver thread
            let total_pylds = evs.len() * max_thrs;
            joins.push(thread::spawn(move || {
                for _ in 0..total_pylds {
                    loop {
                        if let Some(_) = rcv.next() {
                            break;
                        }
                    }
                }
            }));

            // start all our sender threads and blast away
            for _ in 0..max_thrs {
                let mut thr_snd = snd.clone();
                let thr_evs = evs.clone();
                joins.push(thread::spawn(move || {
                    for e in thr_evs {
                        thr_snd.send(&e);
                    }
                }));
            }

            // wait until the senders are for sure done
            for jh in joins {
                jh.join().expect("Uh oh, child thread paniced!");
            }
            TestResult::passed()
        }
        QuickCheck::new()
            .tests(100)
            .max_tests(1000)
            .quickcheck(snd_rcv as fn(Vec<Vec<u32>>) -> TestResult);
    }
}
