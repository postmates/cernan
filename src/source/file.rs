use fnv::FnvHasher;
use glob::glob;
use metric;
use mpsc;
use std::collections::HashMap;
use std::hash::BuildHasherDefault;
use std::io::prelude::*;
use std::path::PathBuf;
use std::str;
use std::time::Duration;
use std::io;
use std::fs;
use time;
use std::time::{SystemTime, Instant};

use super::send;
use source::Source;

type HashMapFnv<K, V> = HashMap<K, V, BuildHasherDefault<FnvHasher>>;

pub struct FileServer {
    chans: Vec<mpsc::Sender<metric::Event>>,
    path: PathBuf,
    tags: metric::TagMap,
}

#[derive(Debug)]
pub struct FileServerConfig {
    pub path: PathBuf,
    pub tags: metric::TagMap,
}

impl FileServer {
    pub fn new(chans: Vec<mpsc::Sender<metric::Event>>, config: FileServerConfig) -> FileServer {
        FileServer {
            chans: chans,
            path: config.path,
            tags: config.tags,
        }
    }
}

pub struct FileWatcher {
    pub path: PathBuf,
    pub reader: io::BufReader<fs::File>,
    pub created: SystemTime,
}

impl FileWatcher {
    pub fn new(path: PathBuf) -> FileWatcher {
        let f = fs::File::open(&path).unwrap();
        let mut rdr = io::BufReader::new(f);
        let _ = rdr.seek(io::SeekFrom::End(0));
        let created = fs::metadata(&path).unwrap().created().unwrap();

        FileWatcher {
            path: path,
            reader: rdr,
            created: created,
        }
    }

    pub fn read_line(&mut self, mut buffer: &mut String) -> io::Result<usize> {
        let max_attempts = 5;
        loop {
            let mut attempts = 0;
            // read lines
            loop {
                match self.reader.read_line(&mut buffer) {
                    Ok(sz) => {
                        if sz == 0 {
                            time::delay(1);
                            attempts += 1;
                            if attempts > max_attempts {
                                break;
                            } else {
                                continue;
                            }
                        } else {
                            return Ok(sz);
                        }
                    }
                    Err(e) => {
                        return Err(e);
                    }
                }
            }

            let new_created = fs::metadata(&self.path).unwrap().created().unwrap();
            if new_created != self.created {
                self.created = new_created;
                let f = fs::File::open(&self.path).unwrap();
                let rdr = io::BufReader::new(f);
                self.reader = rdr;
            } else {
                return Err(io::Error::last_os_error());
            }
        }
    }
}

impl Source for FileServer {
    fn run(&mut self) {
        let mut fp_map: HashMapFnv<PathBuf, FileWatcher> = HashMapFnv::default();
        let glob_delay = Duration::from_secs(60);
        let mut buffer = String::new();

        let mut lines = Vec::new();

        loop {
            for entry in glob(&self.path.to_str().unwrap()).expect("Failed to read glob pattern") {
                match entry {
                    Ok(path) => {
                        let _ = fp_map.entry(path.clone()).or_insert(FileWatcher::new(path));
                    }
                    Err(e) => {
                        debug!("glob error: {}", e);
                    }
                }
            }
            let start = Instant::now();
            loop {
                for file in fp_map.values_mut() {
                    loop {
                        let mut lines_read = 0;
                        match file.read_line(&mut buffer) {
                            Ok(sz) => {
                                if sz > 0 {
                                    lines_read += 1;
                                    buffer.pop();
                                    trace!("{} | {}", file.path.to_str().unwrap(), buffer);
                                    lines.push(metric::LogLine::new(file.path.to_str().unwrap(),
                                                                    buffer.clone(),
                                                                    self.tags.clone()));
                                    buffer.clear();
                                    if lines_read > 10_000 {
                                        break;
                                    }
                                }
                            }
                            Err(e) => {
                                match e.kind() {
                                    io::ErrorKind::TimedOut => {}
                                    _ => trace!("read-line error: {}", e),
                                }
                                break;
                            }
                        }
                    }
                    if lines.len() > 0 {
                        send("file", &mut self.chans, &metric::Event::Log(lines));
                        lines = Vec::new();
                    }
                }
                if start.elapsed() >= glob_delay {
                    break;
                }
            }
        }
    }
}
