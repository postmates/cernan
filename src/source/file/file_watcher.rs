use source::internal::report_full_telemetry;
use std::fs;
use std::io;
use std::io::BufRead;
use std::io::Seek;
use std::os::unix::fs::MetadataExt;
use std::path::PathBuf;

/// The `FileWatcher` struct defines the polling based state machine which reads
/// from a file path, transparently updating the underlying file descriptor when
/// the file has been rolled over, as is common for logs.
///
/// The `FileWatcher` is expected to live for the lifetime of the file
/// path. `FileServer` is responsible for clearing away `FileWatchers` which no
/// longer exist.
pub struct FileWatcher {
    pub path: PathBuf,
    reader: Option<io::BufReader<fs::File>>,
    file_id: Option<(u64, u64)>,
    reopen: bool,
}

impl FileWatcher {
    /// Create a new `FileWatcher`
    ///
    /// The input path will be used by `FileWatcher` to prime its state
    /// machine. A `FileWatcher` tracks _only one_ file. This function returns
    /// None if the path does not exist or is not readable by cernan.
    pub fn new(path: PathBuf) -> io::Result<FileWatcher> {
        match fs::File::open(&path) {
            Ok(f) => {
                let mut rdr = io::BufReader::new(f);
                assert!(rdr.seek(io::SeekFrom::End(0)).is_ok());
                let metadata = fs::metadata(&path).expect("no metadata");
                let dev = metadata.dev();
                let ino = metadata.ino();
                Ok(FileWatcher {
                    path: path.clone(),
                    reader: Some(rdr),
                    file_id: Some((dev, ino)),
                    reopen: false,
                })
            }
            Err(e) => match e.kind() {
                io::ErrorKind::NotFound => Ok(FileWatcher {
                    path: path.clone(),
                    reader: None,
                    file_id: None,
                    reopen: false,
                }),
                _ => Err(e),
            },
        }
    }

    fn file_id(&self) -> Option<(u64, u64)> {
        if let Ok(metadata) = fs::metadata(&self.path) {
            let dev = metadata.dev();
            let ino = metadata.ino();
            Some((dev, ino))
        } else {
            None
        }
    }

    fn open_at_start(&mut self) -> () {
        if let Ok(f) = fs::File::open(&self.path) {
            self.reader = Some(io::BufReader::new(f));
            self.file_id = self.file_id();
            assert!(self.file_id.is_some());
            report_full_telemetry(
                "cernan.sources.file.switch",
                1.0,
                Some(vec![
                    (
                        "file_path",
                        self.path.to_str().expect("could not make path"),
                    ),
                ]),
            );
        } else {
            self.reader = None;
            self.file_id = None;
        }
        self.reopen = false;
    }

    pub fn dead(&self) -> bool {
        self.reader.is_none() && self.file_id.is_none()
    }

    /// Read a single line from the underlying file
    ///
    /// This function will attempt to read a new line from its file, blocking,
    /// up to some maximum but unspecified amount of time. `read_line` will open
    /// a new file handler at need, transparently to the caller.
    pub fn read_line(&mut self, mut buffer: &mut String) -> io::Result<usize> {
        if self.reopen && self.file_id() != self.file_id {
            self.open_at_start();
        }
        if let Some(ref mut reader) = self.reader {
            // match here on error, if metadata doesn't match up open_at_start
            // new reader and let it catch on the next looparound
            match reader.read_line(&mut buffer) {
                Ok(0) => {
                    self.reopen = true;
                    Ok(0)
                }
                Ok(sz) => {
                    assert_eq!(sz, buffer.len());
                    buffer.pop();
                    Ok(buffer.len())
                }
                Err(e) => {
                    if let io::ErrorKind::NotFound = e.kind() {
                        self.reopen = true;
                    }
                    Err(e)
                }
            }
        } else {
            self.open_at_start();
            Ok(0)
        }
    }
}
