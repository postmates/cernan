use fnv::FnvHasher;
use metric;
use mpsc;
use notify::op::{REMOVE, RENAME, WRITE};
use notify::{RecommendedWatcher, Error, Watcher};
use std::collections::HashMap;
use std::fs::File;
use std::hash::BuildHasherDefault;
use std::io::prelude::*;
use std::io::{SeekFrom, BufReader};
use std::path::PathBuf;
use std::str;
use std::sync::mpsc::channel;

use super::send;
use source::Source;

type HashMapFnv<K, V> = HashMap<K, V, BuildHasherDefault<FnvHasher>>;

pub struct FileServer {
    chans: Vec<mpsc::Sender<metric::Event>>,
    path: PathBuf,
    tags: metric::TagMap,
}

impl FileServer {
    pub fn new(chans: Vec<mpsc::Sender<metric::Event>>,
               path: PathBuf,
               tags: metric::TagMap)
               -> FileServer {
        FileServer {
            chans: chans,
            path: path,
            tags: tags,
        }
    }
}

impl Source for FileServer {
    fn run(&mut self) {
        let (tx, rx) = channel();
        // NOTE on OSX fsevent will _not_ let us watch a file we don't own
        // effectively. See
        // https://developer.apple.com/library/mac/documentation/Darwin/Conceptual/FSEvents_ProgGuide/FileSystemEventSecurity/FileSystemEventSecurity.html
        // for more details. If we must properly support _all_ files on OSX we
        // will probably need to fall back to Pollwatcher for that operating
        // system.
        let w: Result<RecommendedWatcher, Error> = Watcher::new(tx);

        let mut fp_map: HashMapFnv<PathBuf, BufReader<File>> = HashMapFnv::default();

        match w {
            Ok(mut watcher) => {
                watcher.watch(&self.path).expect("could not set up watch for path");

                while let Ok(event) = rx.recv() {
                    match event.op {
                        Ok(op) => {
                            if let Some(path) = event.path {
                                trace!("OP: {:?} | PATH: {:?} | FP_MAP: {:?}", op, path, fp_map);
                                let mut lines = Vec::new();
                                if op.contains(REMOVE) || op.contains(RENAME) {
                                    fp_map.remove(&path);
                                }
                                if op.contains(WRITE) && !fp_map.contains_key(&path) {
                                    let _ = File::open(&path).map(|fp| {
                                        let mut reader = BufReader::new(fp);
                                        reader.seek(SeekFrom::End(0))
                                            .expect("could not seek to end of file");
                                        fp_map.insert(path.clone(), reader);
                                    });
                                }
                                if op.contains(WRITE) {
                                    if let Some(rdr) = fp_map.get_mut(&path) {
                                        loop {
                                            for line in rdr.lines() {
                                                trace!("PATH: {:?} | LINE: {:?}", path, line);
                                                lines.push(metric::LogLine::new(path.to_str()
                                                                                    .unwrap(),
                                                                                line.unwrap(),
                                                                                self.tags.clone()));
                                            }
                                            if lines.is_empty() {
                                                trace!("EMPTY LINES, SEEK TO START: {:?}", path);
                                                rdr.seek(SeekFrom::Start(0))
                                                    .expect("could not seek to start of file");
                                                continue;
                                            } else {
                                                break;
                                            }
                                        }
                                    }
                                }
                                if !lines.is_empty() {
                                    send("file", &mut self.chans, &metric::Event::Log(lines));
                                }
                            }
                        }
                        Err(e) => panic!("Unknown file event error: {}", e),
                    }
                }
            }
            Err(e) => panic!("Could not create file watcher: {}", e),
        }
    }
}
