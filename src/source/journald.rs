use constants;
use metric::{Event, LogLine, TagMap};
use mio;
use source::Source;
use std::collections::BTreeMap;
use std::io::Result;
use std::sync::Arc;
use systemd::journal::{Journal, JournalFiles, JournalRecord, JournalSeek, JournalWaitResult};
use util::Channel;
use util::send;

/// Configuration for `Journald`.
#[derive(Debug, Clone)]
pub struct JournaldConfig {
    /// The journal files to read from. Defaults to using only the system log.
    pub journal_files: JournalFiles,
    /// Whether to only use runtime journal data.
    pub runtime_only: bool,
    /// Whether to also consider journals from other machines.
    pub local_only: bool,
    /// Matches to filter the logs from journald.
    pub matches: BTreeMap<String, String>,

    /// The tagmap that journald will apply to all of its created log lines.
    pub tags: TagMap,
    /// The forwards that statsd will send its telemetry on to.
    pub forwards: Vec<String>,
    /// The unique name for the source in the routing topology.
    pub config_path: Option<String>,
}

impl Default for JournaldConfig {
    fn default() -> JournaldConfig {
        JournaldConfig {
            journal_files: JournalFiles::System,
            runtime_only: false,
            local_only: true,
            matches: BTreeMap::new(),
            tags: TagMap::default(),
            forwards: Vec::new(),
            config_path: None,
        }
    }
}

/// The Journald `Source`.
///
/// Journald is systemd's service to collect and store logging data.
pub struct Journald {
    chans: Channel,
    tags: Arc<TagMap>,
    matches: BTreeMap<String, String>,

    journal_files: JournalFiles,
    runtime_only: bool,
    local_only: bool,
}

impl Journald {
    /// Create a new `Journald` source
    pub fn new(chans: Channel, config: JournaldConfig) -> Journald {
        warn!("journal files: {:?}", config);
        Journald {
            chans: chans,
            tags: Arc::new(config.tags),
            journal_files: config.journal_files,
            runtime_only: config.runtime_only,
            local_only: config.local_only,
            matches: config.matches,
        }
    }
}

impl Source for Journald {
    fn run(&mut self, poll: mio::Poll) {
        let mut journal = Journal::open(self.journal_files.clone(),
                              self.runtime_only.clone(),
                              self.local_only.clone())
            .expect("Unable to open journal");

        for (key, val) in self.matches.iter() {
            info!("journald_source: adding match {} = {}", key, val);
            journal.match_add(key, val.as_bytes())
                .expect("Unable to add match to journal");
        }

        // seek to end of journal
        match journal.seek(JournalSeek::Tail) {
            Err(err) => warn!("Unable to seek to tail of journal: {}", err),
            Ok(_) => (),
        }

        let tags = Arc::clone(&self.tags);

        const token : mio::Token = mio::Token(0);
        let fd = journal.get_fd().expect("Unable to get journald fd");
        poll.register(&mio::unix::EventedFd(&fd), token, mio::Ready::readable(), mio::PollOpt::edge()).expect("Unable to register polls");

        let gen_log = |mut rec: JournalRecord| -> Result<Event> {
            let path = rec.remove("_SYSTEMD_UNIT")
                .or_else(|| rec.remove("_SYSTEMD_USER_UNIT"))
                .map_or_else(|| {
                    info!("could not get either _SYSTEMD_UNIT nor _SYSTEMD_USER_UNIT");
                    String::from("unknown_unit")
               },
               |s| s.clone() );

            let value = rec.remove("MESSAGE")
                .map_or_else(|| String::from(""), |s| s.clone());

            let mut l = LogLine::new(path.as_ref(), value.as_ref());

            // Copy timestamp from CLOCK_REALTIME
            l = match rec.get("_SOURCE_REALTIME_TIMESTAMP").map(|s| s.parse()) {
                Some(Ok(t)) => l.time(t),
                _ => {
                    info!("Unable to get _SOURCE_REALTIME_TIMESTAMP from journald record.");
                    l
                }
            };

            // Copy JournalRecord's fields into the LogLine.
            for (k, v) in rec {
                l = l.insert_field(k, v);
            }

            // Copy tags into LogLine's fields.
            l = l.overlay_tags_from_map(&tags);

            Ok(Event::new_log(l))
        };

        loop {
            let mut events = mio::Events::with_capacity(1024);
            let mut chans = self.chans.clone();

            let mut process_record = || {
                match journal.process() {
                    Ok(JournalWaitResult::Nop) => return Ok(None),
                    Ok(_) => (),
                    Err(err) => {
                        warn!("journald_source: journal.process failed: {}", err);
                        return Err(err);
                    }
                };
                match journal.next_record()? {
                    None => Ok(None),
                    Some(rec) => match gen_log(rec) {
                        Ok(logline) => {
                            Ok(Some(logline))
                        },
                        Err(err) => {
                            warn!("cannot generate log. error: {}", err);
                            Ok(None)
                        }
                    }
                }
            };

            match poll.poll(&mut events, None) {
                Ok(_num_events) => for event in events {
                    match event.token() {
                        constants::SYSTEM => {
                            send(&mut chans, Event::Shutdown);
                            return;
                        },
                        token => {
                            match process_record() {
                                Ok(Some(logline)) => {
                                    debug!("send with chans: {:?} and logline: {:?}", chans, logline);
                                    send(&mut chans, logline);
                                }
                                Ok(None) => continue,
                                Err(err) => {
                                    warn!("Cannot process record: {}", err);
                                    continue;
                                }
                            }
                        },
                        _ => unreachable!(),
                    }
                },
                Err(err) => {
                    // Error code 74 is BADMSG
                    // Skip invalid records (due to corrupt journal)
                    if err.raw_os_error() == Some(74) {
                        info!("encountered record with error BADMSG: {}", err);
                        continue;
                    } else {
                        error!("got error from journald: {}", err);
                        return;
                    }
                }
            }
        }
    }
}
