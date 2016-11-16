use filter;
use metric;
use mpsc;

use regex;

pub struct CollectdScrub {
    scrub_pattern: regex::Regex,
}

impl Default for filter::collectd_scrub::CollectdScrub {
    fn default() -> Self {
        Self::new()
    }
}

impl CollectdScrub {
    pub fn new() -> CollectdScrub {
        CollectdScrub {
            scrub_pattern: regex::Regex::new(r"^(collectd)[@|.]([[:alnum:]_-]+)(.*)").unwrap(),
        }
    }

    pub fn extract(&self, orig: &str) -> Option<String> {
        self.scrub_pattern
            .captures(orig.into())
            .map(|caps| format!("{}{}", caps.at(1).unwrap(), caps.at(3).unwrap()))
    }
}

impl filter::Filter for CollectdScrub {
    fn process<'a>(&mut self,
                   event: &'a mut metric::Event,
                   chans: &'a mut Vec<mpsc::Sender<metric::Event>>)
                   -> Vec<(&'a mut mpsc::Sender<metric::Event>, Vec<metric::Event>)> {
        trace!("received event: {:?}", event);
        let event = event.clone();
        match event {
            metric::Event::Graphite(mut m) => {
                if let Some(new_name) = self.extract(&m.name) {
                    m.name = new_name;
                }
                debug!("adjusted name: {}", m.name);
                let new_event = metric::Event::Graphite(m);
                debug!("new_event: {:?}", new_event);
                let mut emitts = Vec::new();
                for chan in chans {
                    emitts.push((chan, vec![new_event.clone()]))
                }
                emitts
            }
            other => {
                let mut emitts = Vec::new();
                for chan in chans {
                    emitts.push((chan, vec![other.clone()]))
                }
                emitts
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_collectd_ip_extraction() {
        let cs = CollectdScrub::new();

        let orig = "collectd.ip-10-1-21-239.interface-lo.if_errors.tx 0 1478751126";
        let filtered = cs.extract(orig);

        assert_eq!(Some("collectd.interface-lo.if_errors.tx 0 1478751126".into()),
                   filtered);
    }

    #[test]
    fn test_collectd_non_ip_extraction() {
        let cs = CollectdScrub::new();

        let orig = "collectd.totally_fine.interface-lo.if_errors.tx 0 1478751126";
        let filtered = cs.extract(orig);

        assert_eq!(Some("collectd.interface-lo.if_errors.tx 0 1478751126".into()),
                   filtered);
    }

    #[test]
    fn test_non_collectd_extraction() {
        let cs = CollectdScrub::new();

        let orig = "totally_fine.interface-lo.if_errors.tx 0 1478751126";
        let filtered = cs.extract(orig);

        assert_eq!(None, filtered);
    }
}
