use std::fs;
use std::path::{Path,PathBuf};
use std::io::{ErrorKind,Write,Read,SeekFrom,Seek};
use metric::Event;
use bincode::serde::{serialize,deserialize};
use bincode::SizeLimit;
use std::sync::{atomic,Arc};
use std::{thread,time};

#[inline]
fn u32tou8abe(v: u32) -> [u8; 4] {
    [
        v as u8,
        (v >> 8) as u8,
        (v >> 24) as u8,
        (v >> 16) as u8,
    ]
}

#[inline]
fn u8tou32abe(v: &[u8]) -> u32 {
    (v[3] as u32) +
        ((v[2] as u32) << 8) +
        ((v[1] as u32) << 24) +
        ((v[0] as u32) << 16)
}

#[derive(Debug)]
pub struct Sender {
    root: PathBuf, // directory we store our queues in
    path: PathBuf, // active fp filename
    fp: fs::File,  // active fp
    bytes_written: usize,
    max_bytes: usize,
    global_seq_num: Arc<atomic::AtomicUsize>,
    seq_num: usize,
}

impl Clone for Sender {
    fn clone(&self) -> Sender {
        Sender::new(&self.root, self.max_bytes, self.global_seq_num.clone())
    }
}

#[derive(Debug)]
pub struct Receiver {
    root: PathBuf, // directory we store our queues in
    fp: fs::File,  // active fp
    seq_num: usize,
}

// Here's the plan. The filesystem structure will look like so:
//
//   data-dir/
//      sink-name0/
//         0
//         1
//      sink-name1/
//         0
//
// There will be one Receiver / Sender file. The Sender is responsible for
// switching over to a new file. That means marking the current file read-only
// and dinking up on the name of the next. The Receiver is responsible for
// knowing that if it hits EOF on a read-only file that it should unlink its
// existing file and move on to the next file.
//
// File names always increase.
pub fn channel(name: &str, data_dir: &Path) -> (Sender, Receiver) {
    channel_with_max_bytes(name, data_dir, 1_048_576 * 10)
}

pub fn channel_with_max_bytes(name: &str, data_dir: &Path, max_bytes: usize) -> (Sender, Receiver) {
    let root = data_dir.join(name);
    let snd_root = root.clone();
    let rcv_root = root.clone();
    if !root.is_dir() {
        debug!("MKDIR {:?}", root);
        fs::create_dir_all(root).expect("could not create directory");
    }

    let sender = Sender::new(&snd_root, max_bytes * 10, Arc::new(atomic::AtomicUsize::new(0)));
    let receiver = Receiver::new(&rcv_root);
    (sender, receiver)
}

impl Receiver {
    pub fn new(root: &Path) -> Receiver {
        let (_, queue_file) = fs::read_dir(root).unwrap().map(|de| {
            let d = de.unwrap();
            let created = d.metadata().unwrap().created().unwrap();
            (created, d.path())
        }).max().unwrap();

        let queue_file1 = queue_file.clone();
        let seq_file_path = queue_file1.file_name().unwrap();

        let seq_num : usize = seq_file_path.to_str().unwrap().parse::<usize>().unwrap();
        let mut fp = fs::OpenOptions::new().read(true).open(queue_file).expect("RECEIVER could not open file");
        fp.seek(SeekFrom::End(0)).expect("could not get to end of file");

        Receiver {
            root: root.to_path_buf(),
            fp: fp,
            seq_num: seq_num,
        }
    }
}


impl Iterator for Receiver {
    type Item = Event;

    fn next(&mut self) -> Option<Event> {
        let mut sz_buf = [0; 4];
        let mut payload_buf = [0; 1_048_576]; // TODO 1MB hard-limit is a rough business

        // The receive loop
        //
        // The receiver works by regularly attempting to read a payload from its
        // current log file. In the event we hit EOF without detecting that the
        // file is read-only, we wait and try again. If the file _is_ read-only
        // this is a signal from the senders that the file is no longer being
        // written to. It's safe for the sender to declare the log done by
        // trashing it.
        loop {
            match self.fp.read_exact(&mut sz_buf) {
                Ok(()) => {
                    let payload_size_in_bytes = u8tou32abe(&sz_buf);
                    match self.fp.read_exact(&mut payload_buf[..payload_size_in_bytes as usize]) {
                        Ok(()) => {
                            match deserialize(&payload_buf[..payload_size_in_bytes as usize]) {
                                Ok(event) => return Some(event),
                                Err(e) => panic!("Failed decoding. Skipping {:?}", e),
                            }
                        }
                        Err(e) => {
                            panic!("Error, on-disk payload of advertised size not available! Recv failed with error {:?}", e);
                        }
                    }
                }
                Err(e) => {
                    match e.kind() {
                        ErrorKind::UnexpectedEof => {
                            let metadata = self.fp.metadata().unwrap();
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
                                        Ok(fp) => { self.fp = fp; break; }
                                        Err(_) => continue
                                    }
                                }
                            }
                            // There's payloads to come--else the file would be
                            // read-only--so we wait for 10ms and try again.
                            else {
                                let dur = time::Duration::from_millis(10);
                                thread::sleep(dur);
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

impl Sender {
    pub fn new(data_dir: &Path, max_bytes: usize, global_seq_num: Arc<atomic::AtomicUsize>) -> Sender {
        let seq_num = match fs::read_dir(data_dir).unwrap().map(|de| {
            let d = de.unwrap();
            let created = d.metadata().unwrap().created().unwrap();
            (created, d.path())
        }).max() {
            Some((_, queue_file)) => {
                let seq_file_path = queue_file.file_name().unwrap();
                seq_file_path.to_str().unwrap().parse::<usize>().unwrap()
            },
            None => 0,
        };

        let log = data_dir.join(format!("{}", seq_num));
        let snd_log = log.clone();
        let fp = fs::OpenOptions::new().append(true).create(true).open(log).expect("SENDER NEW could not open file");
        Sender {
            root: data_dir.to_path_buf(),
            path: snd_log,
            fp: fp,
            bytes_written: 0,
            max_bytes: max_bytes,
            global_seq_num: global_seq_num,
            seq_num: seq_num,
        }
    }

    // send writes data out in chunks, like so:
    //
    //  u32: payload_size
    //  [u8] payload
    //
    pub fn send(&mut self, event: &Event) {
        let mut t = serialize(event, SizeLimit::Infinite).expect("could not serialize");
        // NOTE The conversion of t.len to u32 and usize is _only_ safe when u32
        // <= usize. That's very likely to hold true for machines--for
        // now?--that cernan will run on. However! Once the u32 atomics land in
        // stable we'll be in business.
        let pyld_sz_bytes : [u8; 4] = u32tou8abe(t.len() as u32);
        t.insert(0, pyld_sz_bytes[0]);
        t.insert(0, pyld_sz_bytes[1]);
        t.insert(0, pyld_sz_bytes[2]);
        t.insert(0, pyld_sz_bytes[3]);
        self.bytes_written = self.bytes_written + t.len();
        // The write loop
        //
        // We use a write loop to retry in the event of a collision between two
        // sender threads, detected by file-handler goofs. I've been unable to
        // force bad behaviour with this implementation run under extreme
        // conditions but, like most lock-free code, it ought to be viewed with
        // suspicion.
        loop {
            let global_seq_num = (*self.global_seq_num).load(atomic::Ordering::Relaxed);
            // Each sender operates independently of one another. We keep track of
            // the maximum bytes that are written in a non-shared manner. That is, a
            // very slow sender will tend to stick with the same log file while a
            // very fast one will tend to keep moving forward.
            //
            // To avoid having either all senders wait for the slow sender _or_ the
            // receiver wait on the slowest sender we have each sender pull the
            // "global_seq_num" and compare its current "seq_num". If less, the
            // sender is slow and needs to catch up.
            if self.seq_num < global_seq_num {
                self.seq_num = global_seq_num;
                self.path = self.root.join(format!("{}", self.seq_num));
                match fs::OpenOptions::new().append(true).create(false).open(&self.path) {
                    Ok(fp) => self.fp = fp,
                    Err(e) => { debug!("sender could not catch up with {}", e); continue }
                }
                self.bytes_written = 0;
            }
            // If the individual sender writes enough to go over the max we mark the
            // file read-only--which will help the receiver to decide it has hit the
            // end of its log file--and create a new log file.
            if self.bytes_written > self.max_bytes {
                self.seq_num = self.seq_num.wrapping_add(1);
                // set path to read-only
                let mut permissions = self.fp.metadata().unwrap().permissions();
                permissions.set_readonly(true);
                match fs::set_permissions(&self.path, permissions) {
                    Ok(()) => trace!("set path read-only"),
                    Err(e) => match e.kind() {
                        ErrorKind::NotFound => continue,
                        _ => panic!("failed to set read-only : {:?}", e),
                    }
                }
                // open new fp
                self.path = self.root.join(format!("{}", self.seq_num));
                loop {
                    match fs::OpenOptions::new().append(true).create(true).open(&self.path) {
                        Ok(fp) => { self.fp = fp; break; },
                        Err(_) => continue,
                    }
                }
                self.bytes_written = 0;
                (*self.global_seq_num).fetch_add(1, atomic::Ordering::AcqRel);
            }
            let ref mut fp = self.fp;
            match fp.write(&t[..]) {
                Ok(_) => break,
                Err(e) => { debug!("Write error: {}", e); continue; }
            }
        }
    }
}

#[cfg(test)]
mod test {
    extern crate tempdir;
    extern crate quickcheck;
    extern crate rand;

    use super::*;
    use metric::{Event,Metric,MetricKind,MetricSign};
    use self::quickcheck::{QuickCheck, TestResult, Arbitrary, Gen};
    use self::rand::{Rand,Rng};
    use string_cache::Atom;

    impl Rand for MetricSign {
        fn rand<R: Rng>(rng: &mut R) -> MetricSign {
            let i : usize = rng.gen();
            match i % 2 {
                0 => MetricSign::Positive,
                _ => MetricSign::Negative,
            }
        }
    }

    impl Rand for MetricKind {
        fn rand<R: Rng>(rng: &mut R) -> MetricKind {
            let i : usize = rng.gen();
            match i % 6 {
                0 => MetricKind::Counter(rng.gen()),
                1 => MetricKind::Gauge,
                2 => MetricKind::DeltaGauge,
                3 => MetricKind::Timer,
                4 => MetricKind::Histogram,
                _ => MetricKind::Raw,
            }
        }
    }

    impl Rand for Event {
        fn rand<R: Rng>(rng: &mut R) -> Event {
            let i : usize = rng.gen();
            match i % 4 {
                0 => Event::TimerFlush,
                1 => Event::Snapshot,
                _ => {
                    let name : String = rng.gen_ascii_chars().take(10).collect();
                    let val : f64 = rng.gen();
                    let kind : MetricKind = rng.gen();
                    let sign : Option<MetricSign> = rng.gen();
                    let m = Metric::new(Atom::from(name), val, kind, sign);
                    Event::Statsd(m)
                }
            }
        }
    }

    impl Arbitrary for Event {
        fn arbitrary<G: Gen>(g: &mut G) -> Event { g.gen() }
    }

    #[test]
    fn one_item_round_trip() {
        let dir = tempdir::TempDir::new("cernan").unwrap();
        let (mut snd, mut rcv) = channel("one_item_round_trip", dir.path());

        snd.send(&Event::TimerFlush);

        assert_eq!(Some(Event::TimerFlush), rcv.next());
    }

    #[test]
    fn round_trip_order_preserved() {
        fn rnd_trip(max_bytes: usize, evs: Vec<Event>) -> TestResult {
            let dir = tempdir::TempDir::new("cernan").unwrap();
            let (mut snd, mut rcv) = channel_with_max_bytes("round_trip_order_preserved", dir.path(), max_bytes);

            for ev in evs.clone() {
                snd.send(&ev);
            }

            for ev in evs {
                assert_eq!(Some(ev), rcv.next());
            }
            TestResult::passed()
        }
        QuickCheck::new()
            .tests(1000)
            .max_tests(10000)
            .quickcheck(rnd_trip as fn(usize, Vec<Event>) -> TestResult);
    }
}
