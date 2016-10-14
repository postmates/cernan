use time;
use std::str::FromStr;

include!(concat!(env!("OUT_DIR"), "/serde_types.rs"));

use std::collections::HashMap;
use std::hash::BuildHasherDefault;
use fnv::FnvHasher;
use std::cmp::Ordering;

pub type TagMap = HashMap<String, String, BuildHasherDefault<FnvHasher>>;

impl LogLine {
    pub fn new(path: String, value: String, tags: TagMap) -> LogLine {
        LogLine {
            path: path,
            value: value,
            time: time::now(),
            tags: tags,
        }
    }
}

#[derive(PartialEq, Clone, Debug)]
pub struct MetricQOS {
    pub counter: u64,
    pub gauge: u64,
    pub timer: u64,
    pub histogram: u64,
    pub raw: u64,
}

impl Default for MetricQOS {
    fn default() -> MetricQOS {
        MetricQOS {
            counter: 1,
            gauge: 1,
            timer: 1,
            histogram: 1,
            raw: 1,
        }
    }
}

fn cmp(left: &TagMap, right: &TagMap) -> Option<Ordering> {
    if left.len() != right.len() {
        left.len().partial_cmp(&right.len())
    } else {
        let mut l: Vec<(&String,&String)> = left.iter().collect();
        l.sort();
        let mut r: Vec<(&String,&String)> = right.iter().collect();
        r.sort();
        l.partial_cmp(&r)
    }
}

impl PartialEq for MetricKind {
    fn eq(&self, other: &MetricKind) -> bool {
        match (self, other) {
            (&MetricKind::Gauge, &MetricKind::Gauge) => true,
            (&MetricKind::Gauge, &MetricKind::DeltaGauge) => true,
            (&MetricKind::DeltaGauge, &MetricKind::DeltaGauge) => true,
            (&MetricKind::DeltaGauge, &MetricKind::Gauge) => true,
            (&MetricKind::Counter(_), &MetricKind::Counter(_)) => true,
            (&MetricKind::Timer, &MetricKind::Timer) => true,
            (&MetricKind::Histogram, &MetricKind::Histogram) => true,
            (&MetricKind::Raw, &MetricKind::Raw) => true,
            _ => false,
        }
    }
}

impl PartialOrd for MetricKind {
    fn partial_cmp(&self, other: &MetricKind) -> Option<Ordering> {
        match (self, other) {
            (&MetricKind::Counter(_), &MetricKind::DeltaGauge) => Some(Ordering::Less),
            (&MetricKind::Counter(_), &MetricKind::Gauge) => Some(Ordering::Less),
            (&MetricKind::Counter(_), &MetricKind::Histogram) => Some(Ordering::Less),
            (&MetricKind::Counter(_), &MetricKind::Raw) => Some(Ordering::Less),
            (&MetricKind::Counter(_), &MetricKind::Timer) => Some(Ordering::Less),
            (&MetricKind::Counter(_), &MetricKind::Counter(_)) => Some(Ordering::Equal),
            (&MetricKind::DeltaGauge, &MetricKind::Counter(_)) => Some(Ordering::Greater),
            (&MetricKind::DeltaGauge, &MetricKind::DeltaGauge) => Some(Ordering::Equal),
            (&MetricKind::DeltaGauge, &MetricKind::Gauge) => Some(Ordering::Equal),
            (&MetricKind::DeltaGauge, &MetricKind::Histogram) => Some(Ordering::Less),
            (&MetricKind::DeltaGauge, &MetricKind::Raw) => Some(Ordering::Less),
            (&MetricKind::DeltaGauge, &MetricKind::Timer) => Some(Ordering::Less),
            (&MetricKind::Gauge, &MetricKind::Counter(_)) => Some(Ordering::Greater),
            (&MetricKind::Gauge, &MetricKind::DeltaGauge) => Some(Ordering::Equal),
            (&MetricKind::Gauge, &MetricKind::Gauge) => Some(Ordering::Equal),
            (&MetricKind::Gauge, &MetricKind::Histogram) => Some(Ordering::Less),
            (&MetricKind::Gauge, &MetricKind::Raw) => Some(Ordering::Less),
            (&MetricKind::Gauge, &MetricKind::Timer) => Some(Ordering::Less),
            (&MetricKind::Histogram, &MetricKind::Counter(_)) => Some(Ordering::Greater),
            (&MetricKind::Histogram, &MetricKind::DeltaGauge) => Some(Ordering::Greater),
            (&MetricKind::Histogram, &MetricKind::Gauge) => Some(Ordering::Greater),
            (&MetricKind::Histogram, &MetricKind::Histogram) => Some(Ordering::Equal),
            (&MetricKind::Histogram, &MetricKind::Raw) => Some(Ordering::Less),
            (&MetricKind::Histogram, &MetricKind::Timer) => Some(Ordering::Greater),
            (&MetricKind::Raw, &MetricKind::Counter(_)) => Some(Ordering::Greater),
            (&MetricKind::Raw, &MetricKind::DeltaGauge) => Some(Ordering::Greater),
            (&MetricKind::Raw, &MetricKind::Gauge) => Some(Ordering::Greater),
            (&MetricKind::Raw, &MetricKind::Histogram) => Some(Ordering::Greater),
            (&MetricKind::Raw, &MetricKind::Raw) => Some(Ordering::Equal),
            (&MetricKind::Raw, &MetricKind::Timer) => Some(Ordering::Greater),
            (&MetricKind::Timer, &MetricKind::Counter(_)) => Some(Ordering::Greater),
            (&MetricKind::Timer, &MetricKind::DeltaGauge) => Some(Ordering::Greater),
            (&MetricKind::Timer, &MetricKind::Gauge) => Some(Ordering::Greater),
            (&MetricKind::Timer, &MetricKind::Histogram) => Some(Ordering::Less),
            (&MetricKind::Timer, &MetricKind::Raw) => Some(Ordering::Less),
            (&MetricKind::Timer, &MetricKind::Timer) => Some(Ordering::Equal),
        }
    }
}

impl PartialOrd for Metric {
    fn partial_cmp(&self, other: &Metric) -> Option<Ordering> {
        match self.kind.partial_cmp(&other.kind) {
            Some(Ordering::Equal) => match self.name.partial_cmp(&other.name) {
                Some(Ordering::Equal) => match self.time.partial_cmp(&other.time) {
                    Some(Ordering::Equal) => cmp(&self.tags, &other.tags),
                    other => { println!("TIME ORDERING"); other },
                },
                other => { println!("NAME ORDERING"); other },
            },
            other => { println!("KIND ORDERING"); other },
        }
    }
}

impl Metric {
    /// Make a builder for metrics
    ///
    /// This function returns a MetricBuidler with a name set. A metric must
    /// have _at least_ a name and a value but values may be delayed behind
    /// names.
    ///
    /// # Examples
    ///
    /// ```
    /// use cernan::metric::{Metric,MetricKind};
    ///
    /// let m = Metric::new("foo", 1.1);
    ///
    /// assert_eq!(m.kind, MetricKind::Raw);
    /// assert_eq!(m.name, "foo");
    /// assert_eq!(m.value, 1.1);
    /// ```
    pub fn new<S>(name: S, value: f64) -> Metric
        where S: Into<String>
    {
        Metric {
            kind: MetricKind::Raw,
            name: name.into(),
            tags: TagMap::default(),
            time: time::now(),
            value: value,
        }
    }

    /// Overlay a specific key / value pair in self's tags
    ///
    /// This insert a key / value pair into the metric's tag map. If the key was
    /// already present in the tag map the value will be replaced, else it will
    /// be inserted.
    ///
    /// # Examples
    ///
    /// ```
    /// use cernan::metric::Metric;
    ///
    /// let mut m = Metric::new("foo", 1.1);
    ///
    /// assert!(m.tags.is_empty());
    ///
    /// m = m.overlay_tag("foo", "bar");
    /// assert_eq!(Some(&"bar".into()), m.tags.get("foo".into()));
    ///
    /// m = m.overlay_tag("foo", "22");
    /// assert_eq!(Some(&"22".into()), m.tags.get("foo".into()));
    /// ```
    pub fn overlay_tag<S>(mut self, key: S, val: S) -> Metric
        where S: Into<String> {
        self.tags.insert(key.into(), val.into());
        self
    }

    /// Overlay self's tags with a TagMap
    ///
    /// This inserts a map of key / value pairs over the top of metric's
    /// existing tag map. Any new keys will be inserted while existing keys will
    /// be overwritten.
    ///
    /// # Examples
    ///
    /// ```
    /// use cernan::metric::{Metric,TagMap};
    ///
    /// let mut m = Metric::new("foo", 1.1);
    ///
    /// assert!(m.tags.is_empty());
    ///
    /// m = m.overlay_tag("foo", "22");
    /// assert_eq!(Some(&"22".into()), m.tags.get("foo".into()));
    ///
    /// let mut tag_map = TagMap::default();
    /// tag_map.insert("foo".into(), "bar".into());
    /// tag_map.insert("oof".into(), "rab".into());
    ///
    /// m = m.overlay_tags_from_map(&tag_map);
    /// assert_eq!(Some(&"bar".into()), m.tags.get("foo".into()));
    /// assert_eq!(Some(&"rab".into()), m.tags.get("oof".into()));
    /// ```
    pub fn overlay_tags_from_map(mut self, map: &TagMap) -> Metric {
        for (k,v) in map.iter() {
            self.tags.insert(k.clone(),v.clone());
        }
        self
    }

    /// Merge a TagMap into self's tags
    ///
    /// This inserts a map of key / values pairs into metric's existing map,
    /// inserting keys if and only if the key does not already exist
    /// in-map. This is the information-preserving partner to
    /// overlay_tags_from_map.
    ///
    /// # Examples
    ///
    /// ```
    /// use cernan::metric::{Metric,TagMap};
    ///
    /// let mut m = Metric::new("foo", 1.1);
    ///
    /// assert!(m.tags.is_empty());
    ///
    /// m = m.overlay_tag("foo", "22");
    /// assert_eq!(Some(&"22".into()), m.tags.get("foo".into()));
    ///
    /// let mut tag_map = TagMap::default();
    /// tag_map.insert("foo".into(), "bar".into());
    /// tag_map.insert("oof".into(), "rab".into());
    ///
    /// m = m.merge_tags_from_map(&tag_map);
    /// assert_eq!(Some(&"22".into()), m.tags.get("foo".into()));
    /// assert_eq!(Some(&"rab".into()), m.tags.get("oof".into()));
    /// ```
    pub fn merge_tags_from_map(mut self, map: &TagMap) -> Metric {
        for (k, v) in map.iter() {
            self.tags.entry(k.clone()).or_insert(v.clone());
        }
        self
    }

    /// Replace a Metric's value
    ///
    /// While it is sometimes still necessary to fiddle with the value of a
    /// Metric directly--I'm looking at you, buckets.rs--ideally we'd use a safe
    /// API to do that fiddling.
    ///
    /// This function merely replaces the value. We could get fancy in the
    /// future and do the Kind appropriate computation...
    ///
    /// # Examples
    ///
    /// ```
    /// use cernan::metric::Metric;
    ///
    /// let m = Metric::new("foo", 1.1).value(10.10);
    ///
    /// assert_eq!(m.value, 10.10);
    /// ```
    pub fn value(mut self, value: f64) -> Metric {
        self.value = value;
        self
    }

    /// Adjust MetricKind to Counter
    ///
    /// # Examples
    ///
    /// ```
    /// use cernan::metric::{Metric, MetricKind};
    ///
    /// let m = Metric::new("foo", 1.1).counter(1.0);
    ///
    /// assert_eq!(MetricKind::Counter(1.0), m.kind);
    /// ```
    pub fn counter(mut self, sample: f64) -> Metric {
        self.kind = MetricKind::Counter(sample);
        self
    }

    /// Adjust MetricKind to Gauge
    ///
    /// If the kind has previously been set to DeltaGauge the kind will not be
    /// reset to Gauge.
    ///
    /// # Examples
    ///
    /// ```
    /// use cernan::metric::{Metric, MetricKind};
    ///
    /// let m = Metric::new("foo", 1.1).gauge();
    ///
    /// assert_eq!(MetricKind::Gauge, m.kind);
    /// ```
    pub fn gauge(mut self) -> Metric {
        self.kind = match self.kind {
            MetricKind::DeltaGauge => MetricKind::DeltaGauge,
            _ => MetricKind::Gauge,
        };
        self
    }

    /// Adjust MetricKind to DeltaGauge
    ///
    /// # Examples
    ///
    /// ```
    /// use cernan::metric::{Metric, MetricKind};
    ///
    /// let m = Metric::new("foo", 1.1).delta_gauge();
    ///
    /// assert_eq!(MetricKind::DeltaGauge, m.kind);
    /// ```
    pub fn delta_gauge(mut self) -> Metric {
        self.kind = MetricKind::DeltaGauge;
        self
    }

    /// Adjust MetricKind to Timer
    ///
    /// # Examples
    ///
    /// ```
    /// use cernan::metric::{Metric, MetricKind};
    ///
    /// let m = Metric::new("foo", 1.1).delta_gauge();
    ///
    /// assert_eq!(MetricKind::DeltaGauge, m.kind);
    /// ```
    pub fn timer(mut self) -> Metric {
        self.kind = MetricKind::Timer;
        self
    }

    /// Adjust MetricKind to Histogram
    ///
    /// # Examples
    ///
    /// ```
    /// use cernan::metric::{Metric, MetricKind};
    ///
    /// let m = Metric::new("foo", 1.1).histogram();
    ///
    /// assert_eq!(MetricKind::Histogram, m.kind);
    /// ```
    pub fn histogram(mut self) -> Metric {
        self.kind = MetricKind::Histogram;
        self
    }

    /// Adjust Metric time
    ///
    /// This sets the metric time to the specified value, taken to be UTC
    /// seconds since the Unix Epoch. If this is not set the metric will default
    /// to `cernan::time::now()`.
    ///
    /// # Examples
    ///
    /// ```
    /// use cernan::metric::Metric;
    ///
    /// let m = Metric::new("foo", 1.1).time(10101);
    ///
    /// assert_eq!(10101, m.time);
    /// ```
    pub fn time(mut self, time: i64) -> Metric {
        self.time = time;
        self
    }

    /// Valid message formats are:
    ///
    /// - `<str:metric_name>:<f64:value>|<str:type>`
    /// - `<str:metric_name>:<f64:value>|c|@<f64:sample_rate>`
    ///
    /// Multiple metrics can be sent in a single UDP packet
    /// separated by newlines.
    pub fn parse_statsd(source: &str) -> Option<Vec<Metric>> {
        let mut res = Vec::new();
        let mut iter = source.lines();
        loop {
            let mut offset = 0;
            match iter.next() {
                Some(src) => {
                    let len = src.len();
                    match (&src[offset..]).find(':') {
                        Some(colon_idx) => {
                            let name = &src[offset..(offset + colon_idx)];
                            if name.is_empty() {
                                return None;
                            };
                            offset += colon_idx + 1;
                            if offset >= len {
                                return None;
                            };
                            match (&src[offset..]).find('|') {
                                Some(pipe_idx) => {
                                    let val = match f64::from_str(&src[offset..(offset +
                                                                                pipe_idx)]) {
                                        Ok(f) => f,
                                        Err(_) => return None,
                                    };
                                    let mut metric = Metric::new(name, val);
                                    metric = match &src[offset..(offset + 1)] {
                                        "+" | "-" => metric.delta_gauge(),
                                        _ => metric,
                                    };
                                    offset += pipe_idx + 1;
                                    if offset >= len {
                                        return None;
                                    };
                                    metric = match (&src[offset..]).find('@') {
                                        Some(sample_idx) => {
                                            match &src[offset..(offset + sample_idx)] {
                                                "g" => metric.gauge(),
                                                "ms" => metric.timer(),
                                                "h" => metric.histogram(),
                                                "c" => {
                                                    let sample =
                                                        match f64::from_str(&src[(offset +
                                                                                  sample_idx +
                                                                                  1)..]) {
                                                            Ok(f) => f,
                                                            Err(_) => return None,
                                                        };
                                                    metric.counter(sample)
                                                }
                                                _ => return None,
                                            }
                                        }
                                        None => {
                                            match &src[offset..] {
                                                "g" | "g\n" => metric.gauge(),
                                                "ms" | "ms\n" => metric.timer(),
                                                "h" | "h\n" => metric.histogram(),
                                                "c" | "c\n" => metric.counter(1.0),
                                                _ => return None,
                                            }
                                        }
                                    };

                                    res.push(metric);
                                }
                                None => return None,
                            }
                        }
                        None => return None,
                    }
                }
                None => break,
            }
        }
        if res.is_empty() {
            None
        } else {
            Some(res)
        }
    }

    pub fn parse_graphite(source: &str) -> Option<Vec<Metric>> {
        let mut res = Vec::new();
        let mut iter = source.split_whitespace();
        loop {
            match iter.next() {
                Some(name) => {
                    match iter.next() {
                        Some(val) => {
                            match iter.next() {
                                Some(time) => {
                                    let parsed_val = match f64::from_str(val) {
                                        Ok(f) => f,
                                        Err(_) => return None,
                                    };
                                    let parsed_time = match i64::from_str(time) {
                                        Ok(t) => t,
                                        Err(_) => return None,
                                    };
                                    res.push(Metric::new(name, parsed_val).time(parsed_time));
                                }
                                None => return None,
                            }
                        }
                        None => return None,
                    }
                }
                None => break,
            }
        }
        if res.is_empty() {
            return None;
        }
        Some(res)
    }
}

#[cfg(test)]
mod tests {
    extern crate rand;
    extern crate quickcheck;

    use metric::{Metric, MetricKind, MetricQOS, Event};
    use self::quickcheck::{Arbitrary, Gen};
    use chrono::{UTC, TimeZone};
    use self::rand::{Rand, Rng};
    use std::cmp::Ordering;

    #[test]
    fn partial_ord_gauges() {
        let mdg = Metric::new("l6", 0.7913855).delta_gauge().time(47);
        let mg = Metric::new("l6", 0.9683).gauge().time(47);

        assert_eq!(Some(Ordering::Equal), mg.partial_cmp(&mdg));
        assert_eq!(Some(Ordering::Equal), mdg.partial_cmp(&mg));
    }

    impl Rand for MetricKind {
        fn rand<R: Rng>(rng: &mut R) -> MetricKind {
            let i: usize = rng.gen();
            match i % 6 {
                0 => MetricKind::Counter(rng.gen_range(-2.0, 2.0)),
                1 => MetricKind::Gauge,
                2 => MetricKind::DeltaGauge,
                3 => MetricKind::Timer,
                4 => MetricKind::Histogram,
                _ => MetricKind::Raw,
            }
        }
    }

    impl Rand for Metric {
        fn rand<R: Rng>(rng: &mut R) -> Metric {
            let name: String = rng.gen_ascii_chars().take(2).collect();
            let val: f64 = rng.gen();
            let kind: MetricKind = rng.gen();
            let time: i64 = rng.gen_range(0, 100);
            let mut mb = Metric::new(name, val).time(time);
            mb = match kind {
                MetricKind::Gauge => mb.gauge(),
                MetricKind::Timer => mb.timer(),
                MetricKind::Histogram => mb.histogram(),
                MetricKind::Counter(smpl) => mb.counter(smpl),
                MetricKind::DeltaGauge => mb.delta_gauge(),
                MetricKind::Raw => mb,
            };
            mb
        }
    }

    impl Rand for MetricQOS {
        fn rand<R: Rng>(rng: &mut R) -> MetricQOS {
            MetricQOS {
                counter: rng.gen_range(1, 60),
                gauge: rng.gen_range(1, 60),
                timer: rng.gen_range(1, 60),
                histogram: rng.gen_range(1, 60),
                raw: rng.gen_range(1, 60),
            }
        }
    }

    impl Rand for Event {
        fn rand<R: Rng>(rng: &mut R) -> Event {
            let i: usize = rng.gen();
            match i % 4 {
                0 => Event::TimerFlush,
                _ => Event::Statsd(rng.gen()),
            }
        }
    }

    impl Arbitrary for MetricQOS {
        fn arbitrary<G: Gen>(g: &mut G) -> MetricQOS {
            g.gen()
        }
    }

    impl Arbitrary for Metric {
        fn arbitrary<G: Gen>(g: &mut G) -> Metric {
            g.gen()
        }
    }

    impl Arbitrary for Event {
        fn arbitrary<G: Gen>(g: &mut G) -> Event {
            g.gen()
        }
    }

    #[test]
    fn test_parse_graphite() {
        let pyld = "fst 1 101\nsnd -2.0 202\nthr 3 303\nfth@fth 4 404\nfv%fv 5 505\ns-th 6 606\n";
        let prs = Metric::parse_graphite(pyld);

        assert!(prs.is_some());
        let prs_pyld = prs.unwrap();

        assert_eq!(prs_pyld[0].kind, MetricKind::Raw);
        assert_eq!(prs_pyld[0].name, "fst");
        assert_eq!(prs_pyld[0].value, 1.0);
        assert_eq!(prs_pyld[0].time, UTC.timestamp(101, 0).timestamp());

        assert_eq!(prs_pyld[1].kind, MetricKind::Raw);
        assert_eq!(prs_pyld[1].name, "snd");
        assert_eq!(prs_pyld[1].value, -2.0);
        assert_eq!(prs_pyld[1].time, UTC.timestamp(202, 0).timestamp());

        assert_eq!(prs_pyld[2].kind, MetricKind::Raw);
        assert_eq!(prs_pyld[2].name, "thr");
        assert_eq!(prs_pyld[2].value, 3.0);
        assert_eq!(prs_pyld[2].time, UTC.timestamp(303, 0).timestamp());

        assert_eq!(prs_pyld[3].kind, MetricKind::Raw);
        assert_eq!(prs_pyld[3].name, "fth@fth");
        assert_eq!(prs_pyld[3].value, 4.0);
        assert_eq!(prs_pyld[3].time, UTC.timestamp(404, 0).timestamp());

        assert_eq!(prs_pyld[4].kind, MetricKind::Raw);
        assert_eq!(prs_pyld[4].name, "fv%fv");
        assert_eq!(prs_pyld[4].value, 5.0);
        assert_eq!(prs_pyld[4].time, UTC.timestamp(505, 0).timestamp());

        assert_eq!(prs_pyld[5].kind, MetricKind::Raw);
        assert_eq!(prs_pyld[5].name, "s-th");
        assert_eq!(prs_pyld[5].value, 6.0);
        assert_eq!(prs_pyld[5].time, UTC.timestamp(606, 0).timestamp());
    }

    #[test]
    fn test_negative_timer() {
        let m = Metric::new("timer", -1.0).timer();

        assert_eq!(m.kind, MetricKind::Timer);
        assert_eq!(m.value, -1.0);
        assert_eq!(m.name, "timer");
    }

    #[test]
    fn test_parse_negative_timer() {
        let prs = Metric::parse_statsd("fst:-1.1|ms\n");

        assert!(prs.is_some());
        let prs_pyld = prs.unwrap();

        assert_eq!(prs_pyld[0].kind, MetricKind::Timer);
        assert_eq!(prs_pyld[0].name, "fst");
        assert_eq!(prs_pyld[0].value, -1.1);
    }

    #[test]
    fn test_postive_delta_gauge() {
        let m = Metric::new("dgauge", 1.0).delta_gauge();

        assert_eq!(m.kind, MetricKind::DeltaGauge);
        assert_eq!(m.value, 1.0);
        assert_eq!(m.name, "dgauge");
    }

    #[test]
    fn test_parse_metric_via_api() {
        let pyld = "zrth:0|g\nfst:-1.1|ms\nsnd:+2.2|g\nthd:3.3|h\nfth:4|c\nfvth:5.5|c@2\nsxth:-6.\
                    6|g\nsvth:+7.77|g\n";
        let prs = Metric::parse_statsd(pyld);

        assert!(prs.is_some());
        let prs_pyld = prs.unwrap();

        assert_eq!(prs_pyld[0].kind, MetricKind::Gauge);
        assert_eq!(prs_pyld[0].name, "zrth");
        assert_eq!(prs_pyld[0].value, 0.0);

        assert_eq!(prs_pyld[1].kind, MetricKind::Timer);
        assert_eq!(prs_pyld[1].name, "fst");
        assert_eq!(prs_pyld[1].value, -1.1);

        assert_eq!(prs_pyld[2].kind, MetricKind::DeltaGauge);
        assert_eq!(prs_pyld[2].name, "snd");
        assert_eq!(prs_pyld[2].value, 2.2);

        assert_eq!(prs_pyld[3].kind, MetricKind::Histogram);
        assert_eq!(prs_pyld[3].name, "thd");
        assert_eq!(prs_pyld[3].value, 3.3);

        assert_eq!(prs_pyld[4].kind, MetricKind::Counter(1.0));
        assert_eq!(prs_pyld[4].name, "fth");
        assert_eq!(prs_pyld[4].value, 4.0);

        assert_eq!(prs_pyld[5].kind, MetricKind::Counter(2.0));
        assert_eq!(prs_pyld[5].name, "fvth");
        assert_eq!(prs_pyld[5].value, 5.5);

        assert_eq!(prs_pyld[6].kind, MetricKind::DeltaGauge);
        assert_eq!(prs_pyld[6].name, "sxth");
        assert_eq!(prs_pyld[6].value, -6.6);

        assert_eq!(prs_pyld[7].kind, MetricKind::DeltaGauge);
        assert_eq!(prs_pyld[7].name, "svth");
        assert_eq!(prs_pyld[7].value, 7.77);
    }

    #[test]
    fn test_metric_equal_in_name() {
        let res = Metric::parse_statsd("A=:1|ms\n").unwrap();

        assert_eq!("A=", res[0].name);
        assert_eq!(1.0, res[0].value);
        assert_eq!(MetricKind::Timer, res[0].kind);
    }

    #[test]
    fn test_metric_slash_in_name() {
        let res = Metric::parse_statsd("A/:1|ms\n").unwrap();

        assert_eq!("A/", res[0].name);
        assert_eq!(1.0, res[0].value);
        assert_eq!(MetricKind::Timer, res[0].kind);
    }

    #[test]
    fn test_metric_sample_gauge() {
        let res = Metric::parse_statsd("foo:1|g@0.22\nbar:101|g@2\n").unwrap();
        //                              0         A     F
        assert_eq!("foo", res[0].name);
        assert_eq!(1.0, res[0].value);
        assert_eq!(MetricKind::Gauge, res[0].kind);

        assert_eq!("bar", res[1].name);
        assert_eq!(101.0, res[1].value);
        assert_eq!(MetricKind::Gauge, res[1].kind);
    }

    #[test]
    fn test_metric_parse_invalid_no_name() {
        assert_eq!(None, Metric::parse_statsd(""));
    }

    #[test]
    fn test_metric_parse_invalid_no_value() {
        assert_eq!(None, Metric::parse_statsd("foo:"));
    }

    #[test]
    fn test_metric_multiple() {
        let res = Metric::parse_statsd("a.b:12.1|g\nb_c:13.2|c\n").unwrap();
        assert_eq!(2, res.len());

        assert_eq!("a.b", res[0].name);
        assert_eq!(12.1, res[0].value);

        assert_eq!("b_c", res[1].name);
        assert_eq!(13.2, res[1].value);
    }

    #[test]
    fn test_metric_optional_final_newline() {
        let res = Metric::parse_statsd("a.b:12.1|g\nb_c:13.2|c").unwrap();
        assert_eq!(2, res.len());

        assert_eq!("a.b", res[0].name);
        assert_eq!(12.1, res[0].value);

        assert_eq!("b_c", res[1].name);
        assert_eq!(13.2, res[1].value);
    }

    #[test]
    fn test_metric_invalid() {
        let invalid = vec!["", "metric", "metric|11:", "metric|12", "metric:13|", ":|@", ":1.0|c"];
        for input in invalid.iter() {
            let result = Metric::parse_statsd(*input);
            assert!(result.is_none());
        }
    }
}
