use bincode::SizeLimit;
use bincode::serde::serialize;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::Write;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

use mpsc::FSLock;

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
    root: PathBuf, // directory we store our queues in
    path: PathBuf, // active fp filename
    fp: fs::File, // active fp
    seq_num: usize,
    max_bytes: usize,
    fs_lock: FSLock,
    resource_type: PhantomData<T>,
}

impl<T> Clone for Sender<T>
    where T: Serialize + Deserialize
{
    fn clone(&self) -> Sender<T> {
        Sender::new(&self.root, self.max_bytes, self.fs_lock.clone())
    }
}

impl<T> Sender<T> where T: Serialize {
    /// TODO
    pub fn new(data_dir: &Path,
               max_bytes: usize,
               fs_lock: FSLock)
               -> Sender<T> {
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
                    return Sender {
                        root: data_dir.to_path_buf(),
                        path: log,
                        fp: fp,
                        seq_num: seq_num,
                        max_bytes: max_bytes,
                        fs_lock: fs_lock,
                        resource_type: PhantomData,
                    }
                }
                Err(e) => panic!("[Sender] failed to start {:?}", e)
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
        let mut syn = self.fs_lock.lock().expect("Sender fs_lock poisoned");
//        println!("SYN: {:?}", *syn);
        if (((*syn).bytes_written + t.len()) > self.max_bytes) || (self.seq_num != (*syn).sender_seq_num) {
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
            let _ = fs::metadata(&self.path).map(|p| {
                let mut permissions = p.permissions();
                permissions.set_readonly(true);
                let _ = fs::set_permissions(&self.path, permissions);
            });
            // It's possible when we open a new file that an even faster writer
            // has already come, written its fill and zipped on down the
            // line. That'd result in an open causing a PermissionDenied. If the
            // receiver is very fast it's _also_ possible that a file will turn
            // up missing, resulting in a NotFound. The way of handling both is
            // to give up the attempt and start at the next GSN.

            if self.seq_num != (*syn).sender_seq_num {
                // if this thread is behind the leader
                self.seq_num = (*syn).sender_seq_num;
            }
            else {
                // this thread is the leader and reset the sender_seq_num,
                // bytes_written
                (*syn).sender_seq_num = self.seq_num.wrapping_add(1);
                self.seq_num = (*syn).sender_seq_num;
                (*syn).bytes_written = 0;
            }
            self.path = self.root.join(format!("{}", self.seq_num));
            match fs::OpenOptions::new().append(true).create(true).open(&self.path) {
                Ok(fp) => {
                    self.fp = fp;
                }
                Err(e) => panic!("FAILED TO OPEN {:?} WITH {:?}", &self.path, e),
            }
        }

        // write to disk
        let fp = &mut self.fp;
        match fp.write(&t[..]) {
            Ok(written) => {
                (*syn).bytes_written += written;
            }
            Err(e) => {
                panic!("Write error: {}", e);
            }
        }
        fp.flush().expect("unable to flush");

        // wake up the Receiver
        (*syn).writes_to_read += 1;
    }
}
