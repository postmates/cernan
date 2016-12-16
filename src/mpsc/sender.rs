use bincode::SizeLimit;
use bincode::serde::serialize_into;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::Write;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::fmt;
use mpsc::FSLock;
use std::cmp::Ordering;

#[inline]
fn u32tou8abe(v: u32) -> [u8; 4] {
    [v as u8, (v >> 8) as u8, (v >> 24) as u8, (v >> 16) as u8]
}

/// The 'send' side of a durable mpsc, similar to
/// [`std::sync::mpsc::Sender`](https://doc.rust-lang.org/std/sync/mpsc/struct.Sender.html).
///
/// A `Sender` does its work by writing to on-disk queue files in an append-only
/// fashion. Each `Sender` may write a soft maximum number of bytes to disk
/// before triggering a roll-over to the next queue file.
pub struct Sender<T> {
    name: String,
    root: PathBuf, // directory we store our queues in
    path: PathBuf, // active fp filename
    fp: fs::File, // active fp
    seq_num: usize,
    max_bytes: usize,
    fs_lock: FSLock<T>,
    resource_type: PhantomData<T>,
}

impl<T> PartialEq for Sender<T> {
    fn eq(&self, other: &Sender<T>) -> bool {
        self.name == other.name
    }
}

impl<T> Eq for Sender<T> {}

impl<T> Ord for Sender<T> {
    fn cmp(&self, other: &Sender<T>) -> Ordering {
        self.name.cmp(&other.name)
    }
}

impl<T> PartialOrd for Sender<T> {
    fn partial_cmp(&self, other: &Sender<T>) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Clone for Sender<T>
    where T: Serialize + Deserialize
{
    fn clone(&self) -> Sender<T> {
        Sender::new(self.name.clone(),
                    &self.root,
                    self.max_bytes,
                    self.fs_lock.clone())
    }
}

impl<T> Sender<T>
    where T: Serialize
{
    /// PRIVATE
    pub fn new<S>(name: S, data_dir: &Path, max_bytes: usize, fs_lock: FSLock<T>) -> Sender<T>
        where S: Into<String> + fmt::Display
    {
        let init_fs_lock = fs_lock.clone();
        let mut syn = init_fs_lock.lock().expect("Sender fs_lock poisoned");
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
                (*syn).sender_seq_num = seq_num;
                Sender {
                    name: name.into(),
                    root: data_dir.to_path_buf(),
                    path: log,
                    fp: fp,
                    seq_num: seq_num,
                    max_bytes: max_bytes,
                    fs_lock: fs_lock,
                    resource_type: PhantomData,
                }
            }
            Err(e) => panic!("[Sender] failed to start {:?}", e),
        }
    }

    /// send writes data out in chunks, like so:
    ///
    ///  u32: payload_size
    ///  [u8] payload
    ///
    pub fn send(&mut self, event: T) {
        let mut syn = self.fs_lock.lock().expect("Sender fs_lock poisoned");
        if (*syn).writes_to_read < (*syn).max_small_buffer {
            (*syn).small_buffer.push_front(event);
            (*syn).writes_to_read += 1;
        } else {
            let mut pyld = Vec::with_capacity(64);
            serialize_into(&mut pyld, &event, SizeLimit::Infinite).expect("could not serialize");
            // NOTE The conversion of t.len to u32 and usize is _only_ safe when u32
            // <= usize. That's very likely to hold true for machines--for
            // now?--that cernan will run on. However!
            let pyld_sz_bytes: [u8; 4] = u32tou8abe(pyld.len() as u32);
            let mut t = vec![0, 0, 0, 0];
            t[0] = pyld_sz_bytes[3];
            t[1] = pyld_sz_bytes[2];
            t[2] = pyld_sz_bytes[1];
            t[3] = pyld_sz_bytes[0];
            t.append(&mut pyld);
            // If the individual sender writes enough to go over the max we mark the
            // file read-only--which will help the receiver to decide it has hit the
            // end of its log file--and create a new log file.
            let bytes_written = (*syn).bytes_written + t.len();
            if (bytes_written > self.max_bytes) || (self.seq_num != (*syn).sender_seq_num) {
                // Once we've gone over the write limit for our current file or find
                // that we've gotten behind the current queue file we need to seek
                // forward to find our place in the space of queue files. We mark
                // our current file read-only--there's some possibility that this
                // will be done redundantly, but that's okay--and then read the
                // current sender_seq_num to get up to date.
                let _ = fs::metadata(&self.path).map(|p| {
                    let mut permissions = p.permissions();
                    permissions.set_readonly(true);
                    let _ = fs::set_permissions(&self.path, permissions);
                });
                if self.seq_num != (*syn).sender_seq_num {
                    // This thread is behind the leader. We've got to set our
                    // current notion of seq_num forward and then open the
                    // corresponding file.
                    self.seq_num = (*syn).sender_seq_num;
                } else {
                    // This thread is the leader. We reset the sender_seq_num and
                    // bytes written and open the next queue file. All follower
                    // threads will hit the branch above this one.
                    (*syn).sender_seq_num = self.seq_num.wrapping_add(1);
                    self.seq_num = (*syn).sender_seq_num;
                    (*syn).bytes_written = 0;
                }
                self.path = self.root.join(format!("{}", self.seq_num));
                match fs::OpenOptions::new().append(true).create(true).open(&self.path) {
                    Ok(fp) => self.fp = fp,
                    Err(e) => panic!("FAILED TO OPEN {:?} WITH {:?}", &self.path, e),
                }
            }

            let fp = &mut self.fp;
            match fp.write(&t[..]) {
                Ok(written) => (*syn).bytes_written += written,
                Err(e) => panic!("Write error: {}", e),
            }
            fp.flush().expect("unable to flush");
            // Let the Receiver know there's more to read.
            (*syn).writes_to_read += 1;
        }
    }

    /// Return the sender's name
    pub fn name(&self) -> &str {
        &self.name
    }
}
