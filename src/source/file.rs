use fnv::FnvHasher;
use glob::glob;
use metric;
use hopper;
use std::collections::HashMap;
use std::hash::BuildHasherDefault;
use std::io::prelude::*;
use std::path::PathBuf;
use std::str;
use std::time::Duration;
use std::io;
use std::fs;
use time;
use std::time::Instant;
use std::os::unix::fs::MetadataExt;

use super::send;
use source::Source;

type HashMapFnv<K, V> = HashMap<K, V, BuildHasherDefault<FnvHasher>>;

pub struct FileServer {
    chans: Vec<hopper::Sender<metric::Event>>,
    path: PathBuf,
    tags: metric::TagMap,
}

#[derive(Debug)]
pub struct FileServerConfig {
    pub path: PathBuf,
    pub tags: metric::TagMap,
    pub forwards: Vec<String>,
    pub config_path: String,
}

impl FileServer {
    pub fn new(chans: Vec<hopper::Sender<metric::Event>>, config: FileServerConfig) -> FileServer {
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
    pub file_id: (u64, u64), // (dev, ino)
}

impl FileWatcher {
    pub fn new(path: PathBuf) -> Option<FileWatcher> {
        match fs::File::open(&path) {
            Ok(f) => {
                let mut rdr = io::BufReader::new(f);
                let _ = rdr.seek(io::SeekFrom::End(0));
                let metadata = fs::metadata(&path)
                    .expect(&format!("no metadata in FileWatcher::new : {:?}", &path));
                let dev = metadata.dev();
                let ino = metadata.ino();

                Some(FileWatcher {
                    path: path,
                    reader: rdr,
                    file_id: (dev, ino),
                })
            }
            Err(_) => None,
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

            if let Ok(metadata) = fs::metadata(&self.path) {
                let dev = metadata.dev();
                let ino = metadata.ino();

                if (dev, ino) != self.file_id {
                    if let Ok(f) = fs::File::open(&self.path) {
                        self.file_id = (dev, ino);
                        let rdr = io::BufReader::new(f);
                        self.reader = rdr;
                    }
                } else {
                    return Err(io::Error::last_os_error());
                }
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
            for entry in glob(self.path.to_str().expect("no ability to glob"))
                .expect("Failed to read glob pattern") {
                match entry {
                    Ok(path) => {
                        let entry = fp_map.entry(path.clone());
                        if let Some(fw) = FileWatcher::new(path) {
                            entry.or_insert(fw);
                        };
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
                                    let path_name =
                                        file.path.to_str().expect("could not make path_name");
                                    trace!("{} | {}", path_name, buffer);
                                    lines.push(metric::LogLine::new(path_name, &buffer)
                                        .overlay_tags_from_map(&self.tags));
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
                    if !lines.is_empty() {
                        for l in lines {
                            send("file", &mut self.chans, metric::Event::Log(l));
                        }
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
