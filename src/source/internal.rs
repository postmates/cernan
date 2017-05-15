use metric;
use source::Source;
use std::collections::VecDeque;
use std::sync;
use time;
use util;

lazy_static! {
    static ref Q: sync::Mutex<VecDeque<metric::Telemetry>> = sync::Mutex::new(VecDeque::new());
}

/// 'Internal' is a Source which is meant to allow cernan to
/// self-telemeter. This is an improvement over past methods as an explicit
/// Source gives operators the ability to define a filter topology for such
/// telemetry and makes it easier for modules to report on themeselves.
pub struct Internal {
    chans: util::Channel,
    tags: sync::Arc<metric::TagMap>,
}

/// The configuration struct for 'Internal'
#[derive(Debug, Deserialize, Clone)]
pub struct InternalConfig {
    /// The configured name of Internal.
    pub config_path: Option<String>,
    /// The forwards which Internal will obey.
    pub forwards: Vec<String>,
    /// The default tags to apply to each Telemetry that comes through the
    /// queue.
    pub tags: metric::TagMap,
}

impl Default for InternalConfig {
    fn default() -> InternalConfig {
        InternalConfig {
            tags: metric::TagMap::default(),
            forwards: Vec::new(),
            config_path: Some("sources.internal".to_string()),
        }
    }
}

impl Internal {
    pub fn new(chans: util::Channel, config: InternalConfig) -> Internal {
        Internal {
            chans: chans,
            tags: sync::Arc::new(config.tags),
        }
    }
}

/// Push telemetry into the Internal queue
///
/// Given a name and value, construct a Telemetry with Sum aggregation and push
/// into Internal's queue. This queue will then be drained into operator
/// configured forwards.
pub fn report_telemetry<S>(name: S, value: f64) -> ()
    where S: Into<String>
{
    Q.lock()
        .unwrap()
        .push_back(metric::Telemetry::new(name, value).aggr_sum());
}

/// Internal as Source
///
/// The 'run' of Internal will pull Telemetry off the internal queue, apply
/// Internal's configured tags and push said telemetry into operator configured
/// channels. If no channels are configured we toss the Telemetry onto the
/// floor.
impl Source for Internal {
    fn run(&mut self) {
        let mut attempts: u32 = 0;
        loop {
            if let Some(mut telem) = Q.lock().unwrap().pop_front() {
                attempts = attempts.saturating_sub(1);
                if !self.chans.is_empty() {
                    telem = telem.overlay_tags_from_map(&self.tags);
                    util::send("internal",
                               &mut self.chans,
                               metric::Event::new_telemetry(telem));
                } else {
                    // do nothing, intentionally
                }
            } else {
                // We mod by an arbitrary constant. We don't want to wait _too_
                // long between polls of the queue but neither do we want to
                // burn up our CPU.
                attempts = (attempts + 1) % 10;
            }
            time::delay(attempts);
        }
    }
}
