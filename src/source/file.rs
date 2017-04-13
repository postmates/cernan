use glob::glob;
use metric;
use seahash::SeaHasher;
use source::Source;
use source::internal::report_telemetry;
use std::collections::HashMap;
use std::fs;
use std::hash::BuildHasherDefault;
use std::io;
use std::io::prelude::*;
use std::os::unix::fs::MetadataExt;
use std::path::PathBuf;
use std::str;
use std::time::Duration;
use std::time::Instant;
use time;
use util;
use util::send;

type HashMapFnv<K, V> = HashMap<K, V, BuildHasherDefault<SeaHasher>>;

/// 'FileServer' is a Source which cooperatively schedules reads over files,
/// converting the lines of said files into LogLine structures. As FileServer is
/// intended to be useful across multiple operating systems with POSIX
/// filesystem semantics FileServer must poll for changes. That is, no event
/// notification is used by FileServer.
///
/// FileServer is configured on a path to watch. The files do _not_ need to
/// exist at cernan startup. FileServer will discover new files which match its
/// path in at most 60 seconds.
pub struct FileServer {
    chans: util::Channel,
    path: PathBuf,
    max_read_lines: usize,
    tags: metric::TagMap,
}

/// The configuration struct for 'FileServer'.
#[derive(Debug)]
pub struct FileServerConfig {
    /// The path that FileServer will watch. Globs are allowed and FileServer
    /// will watch multiple files.
    pub path: PathBuf,
    /// The maximum number of lines to read from a file before switching to a
    /// new file.
    pub max_read_lines: usize,
    /// The default tags to apply to each discovered LogLine.
    pub tags: metric::TagMap,
    /// The forwards which FileServer will obey.
    pub forwards: Vec<String>,
    /// The configured name of FileServer.
    pub config_path: String,
}

impl FileServer {
    /// Make a FileServer
    ///
    pub fn new(chans: util::Channel, config: FileServerConfig) -> FileServer {
        FileServer {
            chans: chans,
            path: config.path,
            tags: config.tags,
            max_read_lines: config.max_read_lines,
        }
    }
}

/// The 'FileWatcher' struct defines the polling based state machine which reads
/// from a file path, transparently updating the underlying file descriptor when
/// the file has been rolled over, as is common for logs.
///
/// The 'FileWatcher' is expected to live for the lifetime of the file
/// path. FileServer is responsible for clearing away FileWatchers which no
/// longer exist.
struct FileWatcher {
    pub path: PathBuf,
    reader: io::BufReader<fs::File>,
    offset: u64,
    file_id: (u64, u64), // (dev, ino)
}

impl FileWatcher {
    /// Create a new FileWatcher
    ///
    /// The input path will be used by FileWatcher to prime its state machine. A
    /// FileWatcher tracks _only one_ file. This function returns None if the
    /// path does not exist or is not readable by cernan.
    pub fn new(path: PathBuf) -> Option<FileWatcher> {
        match fs::File::open(&path) {
            Ok(f) => {
                let mut rdr = io::BufReader::new(f);
                assert!(rdr.seek(io::SeekFrom::End(0)).is_ok());
                let offset = rdr.get_ref().seek(io::SeekFrom::Current(0)).unwrap();
                let metadata = fs::metadata(&path).expect("no metadata");
                let dev = metadata.dev();
                let ino = metadata.ino();

                Some(FileWatcher {
                         path: path,
                         reader: rdr,
                         offset: offset,
                         file_id: (dev, ino),
                     })
            }
            Err(_) => None,
        }
    }

    fn reset_from_md(&mut self) -> bool {
        if let Ok(metadata) = fs::metadata(&self.path) {
            let dev = metadata.dev();
            let ino = metadata.ino();

            if (dev, ino) != self.file_id {
                report_telemetry(format!("cernan.sources.file.{}.switch",
                                         &self.path.to_str().expect("could not make path")),
                                 1.0);
                if let Ok(f) = fs::File::open(&self.path) {
                    self.file_id = (dev, ino);
                    self.reader = io::BufReader::new(f);
                    self.offset = 0;
                    return true;
                }
            }
        }
        false
    }

    /// Read a single line from the underlying file
    ///
    /// This function will attempt to read a new line from its file, blocking,
    /// up to some maximum but unspecified amount of time. `read_line` will open
    /// a new file handler at need, transparently to the caller.
    pub fn read_line(&mut self, mut buffer: &mut String) -> io::Result<usize> {
        // This assert doesn't check for equality. Why? The BufReader may jump
        // way ahead of our current offset but we need to track where we _know_
        // we are based on lines because when we seek back to reset the inner
        // buffer of BufReader will get dumped.
        assert!(self.offset <=
                self.reader
                    .get_ref()
                    .seek(io::SeekFrom::Current(0))
                    .unwrap());
        let mut attempts = 0;
        while attempts < 3 {
            time::delay(attempts);
            match self.reader.read_line(&mut buffer) {
                Ok(0) => {
                    // In some situations BufReader will return a read of
                    // zero. That's not success for our purposes and so we
                    // potentially reset from metadata and, if no, go ahead and
                    // back up to the last known good offset.
                    if !self.reset_from_md() {
                        let seek: bool = self.reader
                            .seek(io::SeekFrom::Start(self.offset))
                            .is_ok();
                        assert!(seek);
                    }
                }
                Ok(sz) => {
                    // Success! We've read a line out of the underlying file. We
                    // pull the newline off the end and return the size of the
                    // buffer read without the newline.
                    self.offset += sz as u64;
                    assert!(sz != 0);
                    buffer.truncate(sz - 1);
                    return Ok(sz - 1);
                }
                Err(_) => {
                    // Similar situation to Ok(0) above excepting that there's a
                    // real, honest IO error to bubble up to the glob loop.
                    if !self.reset_from_md() {
                        return Err(io::Error::last_os_error());
                    }
                }
            }
            attempts += 1;
        }
        // We've polled too many times to no good effect and have run out of
        // time. We'll signal this with TimedOut -- which might also come from
        // BufReader -- so it's hard for the caller to know where this came
        // from. Doesn't seem to be a pain in practice.
        return Err(io::Error::new(io::ErrorKind::TimedOut, "read_line hit max delay"));
    }
}

/// FileServer as Source
///
/// The 'run' of FileServer performs the cooperative scheduling of reads over
/// FileServer's configured files. Much care has been taking to make this
/// scheduling 'fair', meaning busy files do not drown out quiet files or vice
/// versa but there's no one perfect approach. Very fast files _will_ be lost if
/// your system aggressively rolls log files. FileServer will keep a file
/// handler open but should your system move so quickly that a file disappears
/// before cernan is able to open it the contents will be lost. This should be a
/// rare occurence.
///
/// Specific operating systems support evented interfaces that correct this
/// problem but your intrepid authors know of no generic solution.
impl Source for FileServer {
    fn run(&mut self) {
        let mut fp_map: HashMapFnv<PathBuf, FileWatcher> = HashMapFnv::default();
        let glob_delay = Duration::from_secs(60);
        let mut buffer = String::new();

        let mut lines = Vec::new();

        // Alright friends, how does this work?
        //
        // There's two loops, the outer one we'll call the 'glob poll' loop. The
        // inner one we'll call the 'file poll' loop. The glob poll resets at
        // least every 60 seconds, finding new files to create FileWatchers out
        // of and then enters the file poll. The file poll loops through all
        // existing FileWatchers and reads at most max_read_lines lines out of a
        // file.
        //
        // The glob poll enforces a delay between files. This is done to
        // minimize the CPU impact of this polling approach. If a file has no
        // lines to read an attempt counter will go up and we'll wait
        // time::delay(attempts). If a line is read out of the file we'll reset
        // attempts to 0.
        loop {
            // glob poll
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
            let mut attempts: u32 = 0;
            loop {
                // file poll
                if fp_map.is_empty() {
                    time::delay(9);
                    break;
                } else {
                    time::delay(attempts);
                }
                for file in fp_map.values_mut() {
                    loop {
                        let mut lines_read = 0;
                        match file.read_line(&mut buffer) {
                            Ok(sz) => {
                                attempts = (attempts + 1) % 10;
                                if sz > 0 {
                                    lines_read += 1;
                                    buffer.pop();
                                    let path_name =
                                        file.path.to_str().expect("could not make path_name");
                                    report_telemetry(format!("cernan.sources.file.{}.lines_read",
                                                             path_name),
                                                     1.0);
                                    trace!("{} | {}", path_name, buffer);
                                    lines.push(metric::LogLine::new(path_name, &buffer)
                                                   .overlay_tags_from_map(&self.tags));
                                    buffer.clear();
                                    if lines_read > self.max_read_lines {
                                        break;
                                    }
                                }
                            }
                            Err(e) => {
                                match e.kind() {
                                    io::ErrorKind::TimedOut => {}
                                    _ => trace!("read-line error: {}", e),
                                }
                                attempts = (attempts + 1) % 10;
                                break;
                            }
                        }
                    }
                    for l in lines.drain(..) {
                        send("file", &mut self.chans, metric::Event::new_log(l));
                    }
                }
                if start.elapsed() >= glob_delay {
                    break;
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    extern crate quickcheck;
    extern crate rand;
    extern crate tempdir;

    use self::quickcheck::{Arbitrary, Gen, QuickCheck, TestResult};
    use self::rand::{Rand, Rng};
    use super::*;
    use std::fs;

    // actions that apply to a single FileWatcher
    #[derive(Clone, Debug)]
    enum FWAction {
        WriteLine(String),
        RotateFile,
        DeleteFile,
        Pause(u32),
        Exit,
    }

    impl Arbitrary for FWAction {
        fn arbitrary<G: Gen>(g: &mut G) -> FWAction {
            g.gen()
        }
    }

    impl Rand for FWAction {
        fn rand<R: Rng>(rng: &mut R) -> FWAction {
            let i: usize = rng.gen_range(0, 100);
            let ln_sz = rng.gen_range(0, 256);
            let pause = rng.gen_range(1, 3);
            match i {
                0...50 => FWAction::WriteLine(rng.gen_ascii_chars().take(ln_sz).collect()),
                51...75 => FWAction::Pause(pause),
                76...85 => FWAction::RotateFile,
                86...95 => FWAction::DeleteFile,
                _ => FWAction::Exit,
            }
        }
    }

    #[test]
    fn test_file_watcher() {
        fn inner(actions: Vec<FWAction>) -> TestResult {
            let dir = tempdir::TempDir::new("file_watcher_qc").unwrap();
            let path = dir.path().join("a_file.log");
            let mut fp = fs::File::create(&path).expect("could not create");
            let mut fw = FileWatcher::new(path.clone()).expect("must be able to create");

            let mut expected_read = Vec::new();

            for action in actions.iter() {
                match *action {
                    FWAction::DeleteFile => {
                        let _ = fs::remove_file(&path);
                        assert!(!path.exists());
                        assert!(expected_read.is_empty());
                        break;
                    }
                    FWAction::Pause(ps) => time::delay(ps),
                    FWAction::Exit => break,
                    FWAction::WriteLine(ref s) => {
                        assert!(fp.write(s.as_bytes()).is_ok());
                        expected_read.push(s);
                        assert!(fp.write("\n".as_bytes()).is_ok());
                        assert!(fp.flush().is_ok());
                    }
                    FWAction::RotateFile => {
                        let mut new_path = path.clone();
                        new_path.set_extension("log.1");
                        match fs::rename(&path, &new_path) {
                            Ok(_) => {}
                            Err(e) => {
                                println!("ERROR: {:?}", e);
                                assert!(false);
                            }
                        }
                        fp = fs::File::create(&path).expect("could not create");
                    }
                }

                let mut buf = String::new();
                while !expected_read.is_empty() {
                    match fw.read_line(&mut buf) {
                        Ok(sz) => {
                            let exp = expected_read.pop().expect("must be a read here");
                            assert_eq!(buf, *exp);
                            assert_eq!(sz, buf.len());
                            buf.clear();
                        }
                        Err(_) => break,
                    }
                }

                assert!(expected_read.is_empty());
            }
            TestResult::passed()
        }
        QuickCheck::new()
            .tests(10000)
            .max_tests(100000)
            .quickcheck(inner as fn(Vec<FWAction>) -> TestResult);
    }

}
