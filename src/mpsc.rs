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
//! named pipe in Unix. You supply a name to either channel_2 or
//! channel_with_max_bytes_3 and you push bytes in and out. In private, this
//! name is used to create a directory under data_dir and this directory gets
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
use bincode::SizeLimit;
use bincode::serde::{serialize, deserialize};
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::{BufReader, ErrorKind, Write, Read, SeekFrom, Seek};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::{atomic, Arc};
use std::{thread, time};

#[inline]
fn u32tou8abe(v: u32) -> [u8; 4] {
    [v as u8, (v >> 8) as u8, (v >> 24) as u8, (v >> 16) as u8]
}

#[inline]
fn u8tou32abe(v: &[u8]) -> u32 {
    (v[3] as u32) + ((v[2] as u32) << 8) + ((v[1] as u32) << 24) + ((v[0] as u32) << 16)
}

/// The 'send' side of a durable mpsc, similar to
/// [`std::sync::mpsc::Sender`](https://doc.rust-lang.org/std/sync/mpsc/struct.Sender.html).
///
/// A `Sender` does its work by writing to on-disk queue files in an append-only
/// fashion. Each `Sender` may write a soft maximum number of bytes to disk
/// before triggering a roll-over to the next queue file.
pub struct Sender<T> {
    root: PathBuf, // directory we store our queues in
    path: PathBuf, // active fp filename
    fp: fs::File, // active fp
    bytes_written: usize,
    max_bytes: usize,
    global_seq_num: Arc<atomic::AtomicUsize>,
    seq_num: usize,

    resource_type: PhantomData<T>,
}

impl<T> Clone for Sender<T>
    where T: Serialize + Deserialize
{
    fn clone(&self) -> Sender<T> {
        Sender::new(&self.root, self.max_bytes, self.global_seq_num.clone())
    }
}

/// The 'receive' side of a durable mpsc, similar to
/// [`std::sync::mpsc::Receiver`](https://doc.rust-lang.org/std/sync/mpsc/struct.Receiver.html).
///
/// A `Receiver` does its work by reading from on-disk queue files until it hits
/// an EOF. If it hits an EOF on a read-only file, the `Receiver` will delete
/// the current queue file and move on to the next.
pub struct Receiver<T> {
    root: PathBuf, // directory we store our queues in
    fp: BufReader<fs::File>, // active fp
    seq_num: usize,

    resource_type: PhantomData<T>,
}

/// Create a (Sender, Reciever) pair in a like fashion to [`
pub fn channel<T>(name: &str, data_dir: &Path) -> (Sender<T>, Receiver<T>)
    where T: Serialize + Deserialize
{
    channel_with_max_bytes(name, data_dir, 1_048_576 * 100)
}

/// TODO
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

    let sender = Sender::new(&snd_root, max_bytes, Arc::new(atomic::AtomicUsize::new(0)));
    let receiver = Receiver::new(&rcv_root);
    (sender, receiver)
}

impl<T: Deserialize> Receiver<T> {
    /// TODO
    pub fn new(data_dir: &Path) -> Receiver<T> {
        let seq_num = fs::read_dir(data_dir)
            .unwrap()
            .map(|de| {
                de.unwrap().path().file_name().unwrap().to_str().unwrap().parse::<usize>().unwrap()
            })
            .min()
            .unwrap();
        let log = data_dir.join(format!("{}", seq_num));
        let mut fp = fs::OpenOptions::new()
            .read(true)
            .open(log)
            .expect("RECEIVER could not open file");
        fp.seek(SeekFrom::End(0)).expect("could not get to end of file");

        Receiver {
            root: data_dir.to_path_buf(),
            fp: BufReader::new(fp),
            seq_num: seq_num,
            resource_type: PhantomData,
        }
    }
}


impl<T> Iterator for Receiver<T>
    where T: Serialize + Deserialize
{
    type Item = T;

    fn next(&mut self) -> Option<T> {
        let mut sz_buf = [0; 4];
        // The receive loop
        //
        // The receiver works by regularly attempting to read a payload from its
        // current log file. In the event we hit EOF without detecting that the
        // file is read-only, we wait and try again. If the file _is_ read-only
        // this is a signal from the senders that the file is no longer being
        // written to. It's safe for the sender to declare the log done by
        // trashing it.
        let mut attempts = 0;
        loop {
            match self.fp.read_exact(&mut sz_buf) {
                Ok(()) => {
                    let payload_size_in_bytes = u8tou32abe(&sz_buf);
                    let mut payload_buf = vec![0; (payload_size_in_bytes as usize)];
                    match self.fp.read_exact(&mut payload_buf) {
                        Ok(()) => {
                            match deserialize(&payload_buf) {
                                Ok(event) => return Some(event),
                                Err(e) => panic!("Failed decoding. Skipping {:?}", e),
                            }
                        }
                        Err(e) => {
                            panic!("Error, on-disk payload of advertised size not available! \
                                    Recv failed with error {:?}",
                                   e);
                        }
                    }
                }
                Err(e) => {
                    match e.kind() {
                        ErrorKind::UnexpectedEof => {
                            // It's possible that some data snuck into the file
                            // between our read and the below lookup of its
                            // metadata. We'll delay the file switch-over up to
                            // 10ms and retry the EOF.
                            attempts += 1;
                            if attempts < 10 {
                                let dur = time::Duration::from_millis(1);
                                thread::sleep(dur);
                                continue;
                            }
                            // Okay, we're pretty sure that no one snuck data in
                            // on us. We check the metadata condition of the
                            // file and, if we find it read-only, switch on over
                            // to a new log file.
                            let metadata = self.fp
                                .get_ref()
                                .metadata()
                                .expect("could not get metadata at UnexpectedEof");
                            if metadata.permissions().readonly() {
                                let old_log = self.root.join(format!("{}", self.seq_num));
                                fs::remove_file(old_log).expect("could not remove log");
                                self.seq_num = self.seq_num.wrapping_add(1);
                                let lg = self.root.join(format!("{}", self.seq_num));
                                loop {
                                    // Why loop? If the receiver is faster /
                                    // luckier than the sender we _might_ hit a
                                    // situation where the next log file doesn't
                                    // exist yet.
                                    match fs::OpenOptions::new().read(true).open(&lg) {
                                        Ok(fp) => {
                                            self.fp = BufReader::new(fp);
                                            attempts = 0;
                                            break;
                                        }
                                        Err(_) => continue,
                                    }
                                }
                            }
                        }
                        _ => {
                            panic!("unable to cope");
                        }
                    }
                }
            }
        }
    }
}

impl<T: Serialize> Sender<T> {
    /// TODO
    pub fn new(data_dir: &Path,
               max_bytes: usize,
               global_seq_num: Arc<atomic::AtomicUsize>)
               -> Sender<T> {
        loop {
            let seq_num = match fs::read_dir(data_dir)
                .unwrap()
                .map(|de| {
                    de.unwrap()
                        .path()
                        .file_name()
                        .unwrap()
                        .to_str()
                        .unwrap()
                        .parse::<usize>()
                        .unwrap()
                })
                .max() {
                Some(sn) => sn,
                None => 0,
            };
            let log = data_dir.join(format!("{}", seq_num));
            match fs::OpenOptions::new()
                .append(true)
                .create(true)
                .open(&log) {
                Ok(fp) => {
                    return Sender {
                        root: data_dir.to_path_buf(),
                        path: log,
                        fp: fp,
                        bytes_written: 0,
                        max_bytes: max_bytes,
                        global_seq_num: global_seq_num,
                        seq_num: seq_num,
                        resource_type: PhantomData,
                    }
                }
                Err(e) => {
                    match e.kind() {
                        ErrorKind::PermissionDenied |
                        ErrorKind::NotFound => {
                            trace!("[Sender] looping to next seq_num on account of {:?}", e);
                            continue;
                        }
                        _ => panic!("COULD NOT CREATE {:?}", e),
                    }
                }
            }
        }
    }

    /// send writes data out in chunks, like so:
    ///
    ///  u32: payload_size
    ///  [u8] payload
    ///
    pub fn send(&mut self, event: &T) {
        let mut t = serialize(event, SizeLimit::Infinite).expect("could not serialize");
        // NOTE The conversion of t.len to u32 and usize is _only_ safe when u32
        // <= usize. That's very likely to hold true for machines--for
        // now?--that cernan will run on. However! Once the u32 atomics land in
        // stable we'll be in business.
        let pyld_sz_bytes: [u8; 4] = u32tou8abe(t.len() as u32);
        t.insert(0, pyld_sz_bytes[0]);
        t.insert(0, pyld_sz_bytes[1]);
        t.insert(0, pyld_sz_bytes[2]);
        t.insert(0, pyld_sz_bytes[3]);
        // If the individual sender writes enough to go over the max we mark the
        // file read-only--which will help the receiver to decide it has hit the
        // end of its log file--and create a new log file.
        if (self.bytes_written + t.len()) > self.max_bytes {
            // Search for our place, setting paths as read-only as we go
            //
            // Once we've gone over the write limit for our current file we need
            // to seek forward in the space of queue files. It's possible that:
            //
            //   * files will have been deleted by a fast receiver
            //   * files will have been marked read-only already
            //   * our seq_num is well behind the global seq_num
            //
            // The goal here is find our place in the queue files and, while
            // doing so, ensure that each file we come across is marked
            // read-only.
            loop {
                let gsn = (*self.global_seq_num)
                    .compare_and_swap(self.seq_num, self.seq_num + 1, atomic::Ordering::SeqCst);
                assert!(self.seq_num <= gsn);
                while self.seq_num != gsn {
                    let _ = fs::metadata(&self.path).map(|p| {
                        let mut permissions = p.permissions();
                        permissions.set_readonly(true);
                        let _ = fs::set_permissions(&self.path, permissions);
                    });
                    self.seq_num = self.seq_num.wrapping_add(1);
                    self.path = self.root.join(format!("{}", self.seq_num));
                }
                // It's possible when we open a new file that an even faster
                // writer has already come, written its fill and zipped on down
                // the line. That'd result in an open causing a
                // PermissionDenied. If the receiver is very fast it's _also_
                // possible that a file will turn up missing, resulting in a
                // NotFound. The way of handling both is to give up the attempt
                // and start at the next GSN.
                match fs::OpenOptions::new().append(true).create(true).open(&self.path) {
                    Ok(fp) => {
                        self.fp = fp;
                        self.bytes_written = 0;
                        break;
                    }
                    Err(e) => {
                        match e.kind() {
                            ErrorKind::PermissionDenied |
                            ErrorKind::NotFound => {
                                trace!("[Sender] looping in send at {} to find next queue file", self.seq_num);
                                self.seq_num = self.seq_num.wrapping_add(1);
                                self.path = self.root.join(format!("{}", self.seq_num));
                                continue;
                            }
                            _ => {
                                panic!("FAILED TO OPEN {:?} WITH {:?}", &self.path, e);
                            }
                        }
                    }
                }
            }
        }

        // write to disk
        let fp = &mut self.fp;
        match fp.write(&t[..]) {
            Ok(written) => {
                self.bytes_written += written;
            }
            Err(e) => {
                panic!("Write error: {}", e);
            }
        }
        fp.flush().expect("unable to flush");
    }
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
    fn round_trip_order_preserved() {
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
    fn round_trip_order_preserved_small_max_bytes() {
        fn rnd_trip(evs: Vec<Vec<u32>>) -> TestResult {
            let max_bytes: usize = 128;
            let dir = tempdir::TempDir::new("cernan").unwrap();
            let (mut snd, mut rcv) =
                channel_with_max_bytes("small_max_bytes", dir.path(), max_bytes);

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
            .quickcheck(rnd_trip as fn(Vec<Vec<u32>>) -> TestResult);
    }

    #[test]
    fn concurrent_snd_and_rcv_round_trip_order_preserved() {
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
                if let Some(nxt) = rcv.next() {
                    let idx = tst_pylds.binary_search(&nxt).expect("DID NOT FIND ELEMENT");
                    tst_pylds.remove(idx);
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
}
