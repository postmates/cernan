use std::fs;
use std::path::{Path,PathBuf};
use std::io::{Write,Read,SeekFrom,Seek};
use std::time::Duration;
use std::thread::sleep;
use metric::Event;
use bincode::serde::{serialize,deserialize};
use bincode::SizeLimit;

#[derive(Debug)]
pub struct Sender {
    data_dir: PathBuf,
    fp: fs::File,
}

#[derive(Debug)]
pub struct Receiver {
    data_dir: PathBuf,
    fp: fs::File,
}

impl Clone for Sender {
    fn clone(&self) -> Sender {
        Sender::new(&self.data_dir)
    }
}

impl Clone for Receiver {
    fn clone(&self) -> Receiver {
        Receiver::new(&self.data_dir)
    }
}

#[derive(Debug)]
pub enum SendError {
//    Unknown,
}

pub fn channel(data_dir: &Path) -> (Sender, Receiver) {
    if !data_dir.is_dir() {
        match fs::create_dir(data_dir) {
            Err(e) => panic!("Unable to create data_dir! {:?}",  e),
            Ok(_) => {},
        }
    }
    let sender = Sender::new(data_dir);
    let receiver = Receiver::new(data_dir);
    (sender, receiver)
}

impl Receiver {
    pub fn new(data_dir: &Path) -> Receiver {
        let log = data_dir.join("mpmc.log");
        let mut fp = fs::OpenOptions::new().read(true).open(log).expect("could not open file");
        fp.seek(SeekFrom::End(0)).expect("could not get to end of file");
        Receiver {
            data_dir: data_dir.to_path_buf(),
            fp: fp,
        }
    }

    // recv reads data out in chunks
    pub fn recv(&mut self) -> Event {
        let ref mut fp = self.fp;

        let mut sz_buf = [0; 4];
        let mut payload_buf = [0; 1_048_576]; // TODO 1MB hard-limit is a rough business
        loop {
            // TODO Looking up prev_offset like this kind of stinks. We have to
            // make a syscall so we know our position in the file. What would be
            // better is to just keep track of the damned thing.
            let prev_offset = fp.seek(SeekFrom::Current(0)).expect("toasted");

            match fp.read_exact(&mut sz_buf) {
                Ok(()) => {
                    let payload_size_in_bytes = u8tou32abe(&sz_buf);
                    match fp.read_exact(&mut payload_buf[..payload_size_in_bytes as usize]) {
                        Ok(()) => {
                            match deserialize(&payload_buf[..payload_size_in_bytes as usize]) {
                                Ok(event) => return event,
                                Err(e) => {
                                    debug!("Failed decoding. Skipping {:?}", e);
                                }
                            }
                        }
                        Err(e) => {
                            // TODO should we actually retry here? There's an
                            // implict state machine that is shared with
                            // server's file_server and we should extract /
                            // unify them. This will almost surely have to be
                            // done to support log rotation of the data log.
                            debug!("Error while recv'ing. Retrying {:?}", e);
                            fp.seek(SeekFrom::Start(prev_offset)).expect("couldn't rewind");
                            let duration = Duration::from_millis(10);
                            sleep(duration);
                        }
                    }
                }
                Err(_) => {
                    fp.seek(SeekFrom::Start(prev_offset)).expect("couldn't rewind");
                    let duration = Duration::from_millis(100);
                    sleep(duration);
                }
            }
        }
    }
}

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

impl Sender {
    pub fn new(data_dir: &Path) -> Sender {
        let log = data_dir.join("mpmc.log");
        let fp = fs::OpenOptions::new().append(true).create(true).open(log).expect("could not open file");
        Sender {
            data_dir: data_dir.to_path_buf(),
            fp: fp,
        }
    }

    // send writes data out in chunks, like so:
    //
    //  u32: payload_size
    //  [u8] payload
    //
    pub fn send(&mut self, event: &Event) -> Result<(), SendError> {
        let mut t = serialize(event, SizeLimit::Infinite).expect("could not serialize");
        let pyld_sz_bytes : [u8; 4] = u32tou8abe(t.len() as u32);
        t.insert(0, pyld_sz_bytes[0]);
        t.insert(0, pyld_sz_bytes[1]);
        t.insert(0, pyld_sz_bytes[2]);
        t.insert(0, pyld_sz_bytes[3]);

        let ref mut fp = self.fp;
        fp.write(&t[..]).expect("failed to write payload");
        Ok(())
    }
}
