use bincode::serde::deserialize;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::{BufReader, ErrorKind, Read, SeekFrom, Seek};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

use mpsc::FSLock;

#[inline]
fn u8tou32abe(v: &[u8]) -> u32 {
    (v[3] as u32) + ((v[2] as u32) << 8) + ((v[1] as u32) << 24) + ((v[0] as u32) << 16)
}

/// The 'receive' side of a durable mpsc, similar to
/// [`std::sync::mpsc::Receiver`](https://doc.rust-lang.org/std/sync/mpsc/struct.Receiver.html).
///
/// A `Receiver` does its work by reading from on-disk queue files until it hits
/// an EOF. If it hits an EOF on a read-only file, the `Receiver` will delete
/// the current queue file and move on to the next.
pub struct Receiver<T> {
    name: String,
    root: PathBuf, // directory we store our queues in
    fp: BufReader<fs::File>, // active fp
    fs_lock: FSLock,
    resource_type: PhantomData<T>,
}

impl<T> Receiver<T>
    where T: Deserialize
{
    /// TODO
    pub fn new<S>(name: S, data_dir: &Path, fs_lock: FSLock) -> Receiver<T> where S: Into<String> {
        let _ = fs_lock.lock().expect("Sender fs_lock poisoned");
        let seq_num = fs::read_dir(data_dir)
            .unwrap()
            .map(|de| {
                de.unwrap().path().file_name().unwrap().to_str().unwrap().parse::<usize>().unwrap()
            })
            .min()
            .unwrap();
        trace!("[receiver | {}] attempting to open seq_num: {}", data_dir, seq_num);
        let log = data_dir.join(format!("{}", seq_num));
        let mut fp = fs::OpenOptions::new()
            .read(true)
            .open(log)
            .expect("RECEIVER could not open file");
        fp.seek(SeekFrom::End(0)).expect("could not get to end of file");

        Receiver {
            name: name.into(),
            root: data_dir.to_path_buf(),
            fp: BufReader::new(fp),
            resource_type: PhantomData,
            fs_lock: fs_lock,
        }
    }
}


impl<T> Iterator for Receiver<T>
    where T: Serialize + Deserialize
{
    type Item = T;

    fn next(&mut self) -> Option<T> {
        let mut sz_buf = [0; 4];
        let mut syn = self.fs_lock.lock().expect("Receiver fs_lock was poisoned!");
        // The receive loop
        //
        // The receiver works by regularly attempting to read a payload from its
        // current log file. In the event we hit EOF without detecting that the
        // file is read-only, we swing around and try again. If a Sender thread
        // has a bug and is unable to mark a file its finished with as read-only
        // this _will_ cause a livelock situation. If the file _is_ read-only
        // this is a signal from the senders that the file is no longer being
        // written to. It's safe for the Receiver to declare the log done by
        // deleting it and moving on to the next file.
        while (*syn).writes_to_read > 0 {
            debug!("[reciever | {}] writes to read: {}", self.name, (*syn).writes_to_read);
            match self.fp.read_exact(&mut sz_buf) {
                Ok(()) => {
                    let payload_size_in_bytes = u8tou32abe(&sz_buf);
                    trace!("[receiver | {}] payload_size_in_bytes: {}", self.name, payload_size_in_bytes);
                    let mut payload_buf = vec![0; (payload_size_in_bytes as usize)];
                    match self.fp.read_exact(&mut payload_buf) {
                        Ok(()) => {
                            match deserialize(&payload_buf) {
                                Ok(event) => {
                                    (*syn).writes_to_read -= 1;
                                    return Some(event);
                                }
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
                            // Okay, we're pretty sure that no one snuck data in
                            // on us. We check the metadata condition of the
                            // file and, if we find it read-only, switch on over
                            // to a new log file.
                            let metadata = self.fp
                                .get_ref()
                                .metadata()
                                .expect("could not get metadata at UnexpectedEof");
                            if metadata.permissions().readonly() {
                                // TODO all these unwraps are a silent death
                                let seq_num = fs::read_dir(&self.root)
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
                                    .min()
                                    .unwrap();
                                trace!("[receiver | {}] read-only seq_num: {}", self.name, seq_num);
                                let old_log = self.root.join(format!("{}", seq_num));
                                trace!("[receiver | {}] attempting to remove old log {}", self.name, old_log);
                                fs::remove_file(old_log).expect("could not remove log");
                                let lg = self.root.join(format!("{}", seq_num.wrapping_add(1)));
                                trace!("[receiver | {}] attempting to create new log at {}", self.name, lg);
                                match fs::OpenOptions::new().read(true).open(&lg) {
                                    Ok(fp) => {
                                        self.fp = BufReader::new(fp);
                                        continue;
                                    }
                                    Err(e) => panic!("[Receiver] could not open {:?}", e),
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
        None
    }
}
