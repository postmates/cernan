//! Wavefront is a proprietary aggregation and alerting product

use buckets;
use metric::{AggregationMethod, TagIter, TagMap, Telemetry};
use sink::{Sink, Valve};
use source::flushes_per_second;
use std::cmp;
use std::collections::{HashMap, HashSet};
use std::io::Write as IoWrite;
use std::mem;
use std::net::TcpStream;
use std::net::ToSocketAddrs;
use std::string;
use std::sync::atomic::{AtomicUsize, Ordering};
use time;

/// Total number of connection attempts made to wavefront proxy
pub static WAVEFRONT_CONNECT_ATTEMPTS: AtomicUsize = AtomicUsize::new(0);
/// Total number of stored aggregations
pub static WAVEFRONT_AGGR_STORED_VALUES: AtomicUsize = AtomicUsize::new(0);
/// Total histograms emitted
pub static WAVEFRONT_AGGR_HISTO: AtomicUsize = AtomicUsize::new(0);
/// Total sums emitted
pub static WAVEFRONT_AGGR_SUM: AtomicUsize = AtomicUsize::new(0);
/// Total sets emitted
pub static WAVEFRONT_AGGR_SET: AtomicUsize = AtomicUsize::new(0);
/// Total summarize emitted
pub static WAVEFRONT_AGGR_SUMMARIZE: AtomicUsize = AtomicUsize::new(0);
/// Total percentiles for summarize emitted
pub static WAVEFRONT_AGGR_TOT_PERCENT: AtomicUsize = AtomicUsize::new(0);
/// Total delivery successes
pub static WAVEFRONT_DELIVERY_SUCCESS: AtomicUsize = AtomicUsize::new(0);
/// Total delivery failures
pub static WAVEFRONT_DELIVERY_FAILURE: AtomicUsize = AtomicUsize::new(0);
/// Total valve closed
pub static WAVEFRONT_VALVE_CLOSED: AtomicUsize = AtomicUsize::new(0);
/// Total valve open
pub static WAVEFRONT_VALVE_OPEN: AtomicUsize = AtomicUsize::new(0);

#[derive(Copy, Clone, Debug, Default, Serialize, Deserialize)]
/// Controls which aggregrations will be padded
pub struct PadControl {
    /// DO pad SET Telemetry if the value is true, DO NOT pad if the value is
    /// false.
    pub set: bool,
    /// DO pad SUM Telemetry if the value is true, DO NOT pad if the value is
    /// false.
    pub sum: bool,
    /// DO pad SUMMARIZE Telemetry if the value is true, DO NOT pad if the
    /// value is
    /// false.
    pub summarize: bool,
    /// DO pad HISTOGRAM Telemetry if the value is true, DO NOT pad if the
    /// value is
    /// false.
    pub histogram: bool,
}

/// The `wavefront` sink emits into [Wavefront](http://wavefront.com), a
/// proprietary metric aggregation and alerting product.
pub struct Wavefront {
    host: String,
    port: u16,
    bin_width: i64,
    aggrs: buckets::Buckets,
    delivery_attempts: u32,
    percentiles: Vec<(String, f64)>,
    stats: String,
    flush_interval: u64,
    age_threshold: Option<u64>,
    stream: Option<TcpStream>,
    last_seen: HashMap<u64, i64>,
    pad_control: PadControl,
    tags: TagMap,
}

/// Configuration for `wavefront`. The challenge of Wavefront is controlling
/// point spreads emission and accuracy of aggregation. The knobs in this struct
/// reflect that.
#[derive(Clone, Debug, Deserialize)]
pub struct WavefrontConfig {
    /// The width of aggregation bins. A `bin_width` of N will consider points
    /// with timestamps N seconds appart to have occured at the 'same time'.
    pub bin_width: i64,
    /// The wavefront proxy to communicate with. May be an IP address or DNS
    /// hostname.
    pub host: String,
    /// The port of the wavefront proxy.
    pub port: u16,
    /// The sink's unique name in the routing topology.
    pub config_path: Option<String>,
    /// The percentiles that quantile aggregations will report. The quantile
    /// method used by cernan supports arbitrary queries. See
    /// `metric::AggregationMethod` for more details.
    pub percentiles: Vec<(String, f64)>,
    /// The tags to be applied to all `metric::Event`s streaming through this
    /// sink. These tags will overwrite any tags carried by the `metric::Event`
    /// itself.
    pub tags: TagMap,
    /// The sink specific `flush_interval`.
    pub flush_interval: u64,
    /// Determine if we will or will not pad an aggregation, disregard ephemeral
    /// status
    pub pad_control: PadControl,
    /// Determine the age at which a Telemetry point will be ejected. If the
    /// value is None no points will ever be rejected. Units are seconds.
    pub age_threshold: Option<u64>,
}

impl Default for WavefrontConfig {
    fn default() -> WavefrontConfig {
        let percentiles = vec![
            ("min".to_string(), 0.0),
            ("max".to_string(), 1.0),
            ("2".to_string(), 0.02),
            ("9".to_string(), 0.09),
            ("25".to_string(), 0.25),
            ("50".to_string(), 0.5),
            ("75".to_string(), 0.75),
            ("90".to_string(), 0.90),
            ("91".to_string(), 0.91),
            ("95".to_string(), 0.95),
            ("98".to_string(), 0.98),
            ("99".to_string(), 0.99),
            ("999".to_string(), 0.999),
        ];
        WavefrontConfig {
            bin_width: 1,
            host: "localhost".to_string(),
            port: 2878,
            config_path: Some("sinks.wavefront".to_string()),
            percentiles,
            tags: TagMap::default(),
            flush_interval: 60 * flushes_per_second(),
            pad_control: PadControl::default(),
            age_threshold: None,
        }
    }
}

#[inline]
fn fmt_tags(mut iter: TagIter, s: &mut String) -> () {
    if let Some((fk, fv)) = iter.next() {
        s.push_str(fk);
        s.push_str("=");
        s.push_str(fv);
        for (k, v) in iter {
            s.push_str(" ");
            s.push_str(k);
            s.push_str("=");
            s.push_str(v);
        }
    }
}

#[derive(Clone, Debug)]
enum Pad<'a> {
    Zero(&'a Telemetry, i64),
    Telem(&'a Telemetry),
}

impl<'a> Pad<'a> {
    pub fn hash(&self) -> u64 {
        match *self {
            Pad::Zero(x, _) | Pad::Telem(x) => x.hash(),
        }
    }

    pub fn zero_at(self, ts: i64) -> Pad<'a> {
        match self {
            Pad::Zero(x, _) | Pad::Telem(x) => Pad::Zero(x, ts),
        }
    }

    #[cfg(test)]
    pub fn is_zeroed(&self) -> bool {
        match *self {
            Pad::Zero(_, _) => true,
            Pad::Telem(x) => x.is_zeroed(),
        }
    }

    pub fn skip_pad(&self, pad_control: &PadControl) -> bool {
        match *self {
            Pad::Zero(_, _) => true,
            Pad::Telem(x) => match x.kind() {
                AggregationMethod::Histogram => !pad_control.histogram,
                AggregationMethod::Set => !pad_control.set,
                AggregationMethod::Sum => !pad_control.sum,
                AggregationMethod::Summarize => !pad_control.summarize,
            },
        }
    }

    pub fn timestamp(&self) -> i64 {
        match *self {
            Pad::Zero(_, ts) => ts,
            Pad::Telem(x) => x.timestamp,
        }
    }
}

fn padding<'a>(
    xs: buckets::Iter<'a>,
    span: i64,
    last_seen: &'a HashMap<u64, i64>,
    pad_control: PadControl,
) -> Padding<'a> {
    Padding {
        span,
        orig: xs,
        last_seen,
        emit_q: Vec::new(),
        last_hash: 0,
        flush_padded: HashSet::default(),
        pad_control,
    }
}

struct Padding<'a> {
    span: i64,
    orig: buckets::Iter<'a>,
    last_seen: &'a HashMap<u64, i64>,
    emit_q: Vec<Pad<'a>>,
    last_hash: u64,
    flush_padded: HashSet<u64>,
    pad_control: PadControl,
}

impl<'a> Iterator for Padding<'a> {
    type Item = Pad<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        // To figure out what to do we have to know if there's a 'gap' in the
        // original iterator to be filled. This is complicated by emit_q which
        // we use to buffer points that we've read out of the initial iterator
        // _and_ zero points that need to be emitted. We preferentially pull
        // points from the emission queue. In the event that there are not
        // enough, we go to the original iterator.
        let next_x = match self.emit_q.pop() {
            Some(x) => Some(x),
            None => self.orig.next().map(|x| Pad::Telem(x)),
        };
        let next_y = match self.emit_q.pop() {
            Some(x) => Some(x),
            None => self.orig.next().map(|x| Pad::Telem(x)),
        };
        match (next_x, next_y) {
            (Some(x), Some(y)) => {
                // Telemetry hashes by considering name, timestamp and
                // aggregation. If these three are different then the next point
                // is not part of our current sequence and it requires no
                // padding.
                if x.hash() == y.hash() {
                    if x.skip_pad(&self.pad_control) {
                        self.emit_q.push(y);
                        return Some(x);
                    }
                    let flush_padded = self.flush_padded.contains(&x.hash());
                    if !flush_padded {
                        if let Some(ts) = self.last_seen.get(&x.hash()) {
                            if x.timestamp() > *ts {
                                self.flush_padded.insert(x.hash());
                                if (x.timestamp() - ts) / self.span > 1 {
                                    let sub_x = x.clone()
                                        .zero_at(x.timestamp().saturating_sub(1));
                                    let post_x =
                                        x.clone().zero_at(ts.saturating_add(1));
                                    self.emit_q.push(y);
                                    self.emit_q.push(x);
                                    self.emit_q.push(sub_x);
                                    return Some(post_x);
                                }
                            } else {
                                self.emit_q.push(y);
                                return Some(x);
                            }
                        }
                    }
                    match (x.timestamp() - y.timestamp()).abs() / self.span {
                        0 | 1 => {
                            // In this case the next point, y, is within the
                            // span configured by the user. We stash it into
                            // emit_q and will pull it on the next iterative
                            // go-around.
                            self.emit_q.push(y);
                            Some(x)
                        }
                        _ => {
                            // This case is tricky. Here we've found that the
                            // span between our current point, x, and the next
                            // point, y, is larger than the configured
                            // limit. But! If the current point is zero we don't
                            // want to make any more zero points to pad the gap.
                            //
                            // If the value of x is zero we stash the next
                            // point. Else, we make our pad, stashing those
                            // points plus y.
                            let sub_y =
                                y.clone().zero_at(y.timestamp().saturating_sub(1));
                            let post_x =
                                x.clone().zero_at(x.timestamp().saturating_add(1));
                            self.emit_q.push(y);
                            self.emit_q.push(sub_y);
                            self.emit_q.push(post_x);
                            Some(x)
                        }
                    }
                } else {
                    self.emit_q.push(y);
                    Some(x)
                }
            }
            (Some(x), None) => {
                self.last_hash = x.hash();
                // end of sequence
                if x.skip_pad(&self.pad_control) {
                    return Some(x);
                }
                let flush_padded = self.flush_padded.contains(&x.hash());
                if !flush_padded {
                    if let Some(ts) = self.last_seen.get(&x.hash()) {
                        if x.timestamp() > *ts {
                            self.flush_padded.insert(x.hash());
                            if (x.timestamp() - ts) / self.span > 1 {
                                let sub_x =
                                    x.clone().zero_at(x.timestamp().saturating_sub(1));
                                let post_x = x.clone().zero_at(ts.saturating_add(1));
                                self.emit_q.push(x);
                                self.emit_q.push(sub_x);
                                return Some(post_x);
                            }
                        }
                    }
                }
                Some(x)
            }
            (None, _) => None,
        }
    }
}

#[inline]
fn get_from_cache<T>(cache: &mut Vec<(T, String)>, val: T) -> &str
where
    T: cmp::PartialOrd + string::ToString + Copy,
{
    match cache.binary_search_by(|probe| probe.0.partial_cmp(&val).unwrap()) {
        Ok(idx) => &cache[idx].1,
        Err(idx) => {
            let str_val = val.to_string();
            cache.insert(idx, (val, str_val));
            get_from_cache(cache, val)
        }
    }
}

fn connect(host: &str, port: u16) -> Option<TcpStream> {
    let addrs = (host, port).to_socket_addrs();
    match addrs {
        Ok(srv) => {
            let ips: Vec<_> = srv.collect();
            for ip in ips {
                match TcpStream::connect(ip) {
                    Ok(stream) => return Some(stream),
                    Err(e) => info!(
                        "Unable to connect to proxy at {} using addr {} with error \
                         {}",
                        host, ip, e
                    ),
                }
            }
            None
        }
        Err(e) => {
            info!(
                "Unable to perform DNS lookup on host {} with error {}",
                host, e
            );
            None
        }
    }
}

impl Wavefront {
    /// Convert the buckets into a String that
    /// can be sent to the the wavefront proxy
    pub fn format_stats(&mut self) -> () {
        let mut time_cache: Vec<(i64, String)> = Vec::with_capacity(128);
        let mut count_cache: Vec<(usize, String)> = Vec::with_capacity(128);
        let mut value_cache: Vec<(f64, String)> = Vec::with_capacity(128);

        let mut tmp_last_seen = HashMap::new();
        let mut aggrs =
            mem::replace(&mut self.aggrs, buckets::Buckets::new(self.bin_width));
        let mut last_seen = mem::replace(&mut self.last_seen, Default::default());

        for pad in padding(aggrs.iter(), self.bin_width, &last_seen, self.pad_control)
        {
            // When we update the last_seen map we have to be sure that if the new
            // telem has a point with a timestamp greater than the one we have
            // stored we replace the timestamp. Else, we ignore.
            tmp_last_seen.insert(pad.hash(), pad.timestamp());

            match pad {
                Pad::Zero(value, ts) => {
                    let zero = value
                        .clone()
                        .thaw()
                        .value(0.0)
                        .harden()
                        .unwrap()
                        .timestamp(ts);
                    self.fmt_val(
                        &zero,
                        &mut time_cache,
                        &mut count_cache,
                        &mut value_cache,
                    );
                }
                Pad::Telem(value) => {
                    if value.persist {
                        let new_val = value.clone();
                        self.aggrs.add(new_val.timestamp(value.timestamp + 1));
                    }

                    match value.kind() {
                        AggregationMethod::Histogram => {
                            WAVEFRONT_AGGR_HISTO.fetch_add(1, Ordering::Relaxed)
                        }
                        AggregationMethod::Sum | AggregationMethod::Set => {
                            WAVEFRONT_AGGR_SUM.fetch_add(1, Ordering::Relaxed)
                        }
                        AggregationMethod::Summarize => {
                            WAVEFRONT_AGGR_SUMMARIZE.fetch_add(1, Ordering::Relaxed);
                            WAVEFRONT_AGGR_TOT_PERCENT
                                .fetch_add(self.percentiles.len(), Ordering::Relaxed)
                        }
                    };

                    self.fmt_val(
                        value,
                        &mut time_cache,
                        &mut count_cache,
                        &mut value_cache,
                    );
                }
            }
        }
        for (k, v) in tmp_last_seen {
            last_seen.insert(k, v);
        }
        self.aggrs = aggrs;
        self.aggrs.reset();
        self.last_seen = last_seen;
    }

    fn fmt_val(
        &mut self,
        value: &Telemetry,
        mut time_cache: &mut Vec<(i64, String)>,
        mut count_cache: &mut Vec<(usize, String)>,
        mut value_cache: &mut Vec<(f64, String)>,
    ) -> () {
        let mut tag_buf = String::with_capacity(1_024);
        match value.kind() {
            AggregationMethod::Histogram => if let Some(bins) = value.bins() {
                use quantiles::histogram::Bound;
                fmt_tags(value.tags(&self.tags), &mut tag_buf);
                for &(bound, count) in bins {
                    self.stats.push_str(&value.name);
                    self.stats.push_str("_");
                    match bound {
                        Bound::Finite(bnd) => {
                            self.stats.push_str(get_from_cache(&mut value_cache, bnd));
                        }
                        Bound::PosInf => {
                            self.stats.push_str("pos_inf");
                        }
                    };
                    self.stats.push_str(" ");
                    self.stats.push_str(get_from_cache(&mut count_cache, count));
                    self.stats.push_str(" ");
                    self.stats
                        .push_str(get_from_cache(&mut time_cache, value.timestamp));
                    self.stats.push_str(" ");
                    self.stats.push_str(&tag_buf);
                    self.stats.push_str("\n");
                }

                tag_buf.clear();
            },
            AggregationMethod::Sum => if let Some(v) = value.sum() {
                self.stats.push_str(&value.name);
                self.stats.push_str(" ");
                self.stats.push_str(get_from_cache(&mut value_cache, v));
                self.stats.push_str(" ");
                self.stats
                    .push_str(get_from_cache(&mut time_cache, value.timestamp));
                self.stats.push_str(" ");
                fmt_tags(value.tags(&self.tags), &mut tag_buf);
                self.stats.push_str(&tag_buf);
                self.stats.push_str("\n");

                tag_buf.clear();
            },
            AggregationMethod::Set => if let Some(v) = value.set() {
                self.stats.push_str(&value.name);
                self.stats.push_str(" ");
                self.stats.push_str(get_from_cache(&mut value_cache, v));
                self.stats.push_str(" ");
                self.stats
                    .push_str(get_from_cache(&mut time_cache, value.timestamp));
                self.stats.push_str(" ");
                fmt_tags(value.tags(&self.tags), &mut tag_buf);
                self.stats.push_str(&tag_buf);
                self.stats.push_str("\n");

                tag_buf.clear();
            },
            AggregationMethod::Summarize => {
                fmt_tags(value.tags(&self.tags), &mut tag_buf);
                for tup in &self.percentiles {
                    let stat: &String = &tup.0;
                    let quant: f64 = tup.1;
                    self.stats.push_str(&value.name);
                    self.stats.push_str(".");
                    self.stats.push_str(stat);
                    self.stats.push_str(" ");
                    self.stats.push_str(get_from_cache(
                        &mut value_cache,
                        value.query(quant).unwrap(),
                    ));
                    self.stats.push_str(" ");
                    self.stats
                        .push_str(get_from_cache(&mut time_cache, value.timestamp));
                    self.stats.push_str(" ");
                    self.stats.push_str(&tag_buf);
                    self.stats.push_str("\n");
                }
                let count = value.count();
                self.stats.push_str(&value.name);
                self.stats.push_str(".count");
                self.stats.push_str(" ");
                self.stats.push_str(get_from_cache(&mut count_cache, count));
                self.stats.push_str(" ");
                self.stats
                    .push_str(get_from_cache(&mut time_cache, value.timestamp));
                self.stats.push_str(" ");
                self.stats.push_str(&tag_buf);
                self.stats.push_str("\n");

                let mean = value.mean();
                self.stats.push_str(&value.name);
                self.stats.push_str(".mean");
                self.stats.push_str(" ");
                self.stats.push_str(get_from_cache(&mut value_cache, mean));
                self.stats.push_str(" ");
                self.stats
                    .push_str(get_from_cache(&mut time_cache, value.timestamp));
                self.stats.push_str(" ");
                self.stats.push_str(&tag_buf);
                self.stats.push_str("\n");

                tag_buf.clear();
            }
        }
    }
}

impl Sink<WavefrontConfig> for Wavefront {
    fn init(config: WavefrontConfig) -> Self {
        if config.host == "" {
            panic!("Host can not be empty".to_string());
        }
        let stream = connect(&config.host, config.port);
        Wavefront {
            host: config.host,
            port: config.port,
            bin_width: config.bin_width,
            aggrs: buckets::Buckets::new(config.bin_width),
            delivery_attempts: 0,
            percentiles: config.percentiles,
            stats: String::with_capacity(0x2000),
            stream: stream,
            flush_interval: config.flush_interval,
            age_threshold: config.age_threshold,
            last_seen: HashMap::default(),
            pad_control: config.pad_control,
            tags: config.tags,
        }
    }

    fn flush_interval(&self) -> Option<u64> {
        Some(self.flush_interval)
    }

    fn flush(&mut self) {
        self.format_stats();
        loop {
            if self.delivery_attempts > 0 {
                debug!("delivery attempts: {}", self.delivery_attempts);
            }
            let mut delivery_failure = false;
            if let Some(ref mut stream) = self.stream {
                let res = stream.write_all(self.stats.as_bytes());
                if res.is_ok() {
                    self.aggrs.reset();
                    self.stats.clear();
                    self.delivery_attempts = 0;
                    WAVEFRONT_DELIVERY_SUCCESS.fetch_add(1, Ordering::Relaxed);
                    return;
                } else {
                    WAVEFRONT_DELIVERY_FAILURE.fetch_add(1, Ordering::Relaxed);
                    self.delivery_attempts = self.delivery_attempts.saturating_add(1);
                    delivery_failure = true;
                }
            } else {
                time::delay(self.delivery_attempts);
                WAVEFRONT_CONNECT_ATTEMPTS.fetch_add(1, Ordering::Relaxed);
                self.stream = connect(&self.host, self.port);
            }
            if delivery_failure {
                self.stream = None
            }
        }
    }

    fn shutdown(mut self) -> () {
        self.flush();
    }

    fn deliver(&mut self, telem: Telemetry) -> () {
        if let Some(age_threshold) = self.age_threshold {
            if (telem.timestamp - time::now()).abs() <= (age_threshold as i64) {
                self.aggrs.add(telem);
            }
        } else {
            self.aggrs.add(telem);
        }
    }

    fn valve_state(&self) -> Valve {
        let total_values = self.aggrs.count();
        WAVEFRONT_AGGR_STORED_VALUES.store(total_values, Ordering::Relaxed);
        if total_values > 10_000 {
            WAVEFRONT_VALVE_CLOSED.fetch_add(1, Ordering::Relaxed);
            Valve::Closed
        } else {
            WAVEFRONT_VALVE_OPEN.fetch_add(1, Ordering::Relaxed);
            Valve::Open
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use buckets::Buckets;
    use chrono::{TimeZone, Utc};
    use metric::{AggregationMethod, TagMap, Telemetry};
    use quickcheck::{QuickCheck, TestResult};
    use sink::Sink;

    #[test]
    fn test_pad_across_flush() {
        let bin_width = 1;
        let mut bucket = buckets::Buckets::new(bin_width);

        let m0 = Telemetry::new()
            .value(0.5)
            .kind(AggregationMethod::Set)
            .name("")
            .harden()
            .unwrap()
            .timestamp(100);
        let mut last_seen = HashMap::new();
        last_seen.insert(m0.hash(), 10);

        let pad_control = PadControl {
            set: true,
            sum: true,
            summarize: true,
            histogram: true,
        };

        bucket.add(m0);

        let mut telems = padding(bucket.iter(), bin_width, &last_seen, pad_control);
        assert!(telems.next().unwrap().is_zeroed()); // 11 at 0
        assert!(telems.next().unwrap().is_zeroed()); // 99 at 0
        assert!(!telems.next().unwrap().is_zeroed()); // 100 at 0.5
        assert!(telems.next().is_none());
    }

    #[test]
    fn test_no_pad_across_flush() {
        let bin_width = 1;
        let mut bucket = buckets::Buckets::new(bin_width);

        let m0 = Telemetry::new()
            .value(0.5)
            .kind(AggregationMethod::Set)
            .name("")
            .harden()
            .unwrap()
            .timestamp(100);
        let mut last_seen = HashMap::new();
        last_seen.insert(m0.hash(), 10);

        let pad_control = PadControl {
            set: false,
            sum: false,
            summarize: false,
            histogram: false,
        };

        bucket.add(m0);

        let mut telems = padding(bucket.iter(), bin_width, &last_seen, pad_control);
        assert!(!telems.next().unwrap().is_zeroed()); // 100 at 0.5
        assert!(telems.next().is_none());
    }

    #[test]
    fn test_pad_across_multiple_flush() {
        let bin_width = 1;
        let mut bucket = buckets::Buckets::new(bin_width);

        let m0 = Telemetry::new()
            .value(0.5)
            .kind(AggregationMethod::Set)
            .name("")
            .harden()
            .unwrap()
            .timestamp(0);
        let m1 = Telemetry::new()
            .value(0.5)
            .kind(AggregationMethod::Set)
            .name("")
            .harden()
            .unwrap()
            .timestamp(10);
        let m2 = Telemetry::new()
            .value(0.5)
            .kind(AggregationMethod::Set)
            .name("")
            .harden()
            .unwrap()
            .timestamp(11);
        let m3 = Telemetry::new()
            .value(0.5)
            .kind(AggregationMethod::Set)
            .name("")
            .harden()
            .unwrap()
            .timestamp(100);
        let mut last_seen = HashMap::new();
        last_seen.insert(m0.hash(), 10);

        bucket.add(m0);
        bucket.add(m1);
        bucket.add(m2);
        bucket.add(m3);

        let pad_control = PadControl {
            set: true,
            sum: true,
            summarize: true,
            histogram: true,
        };

        let mut telems = padding(bucket.iter(), bin_width, &last_seen, pad_control);

        assert!(!telems.next().unwrap().is_zeroed()); // m0
        assert!(!telems.next().unwrap().is_zeroed()); // m1
        assert!(!telems.next().unwrap().is_zeroed()); // m2
        assert!(telems.next().unwrap().is_zeroed()); // m2_0
        assert!(telems.next().unwrap().is_zeroed()); // m3_0
        assert!(!telems.next().unwrap().is_zeroed()); // m3
        assert!(telems.next().is_none());
    }

    #[test]
    fn test_pad_is_identity_with_no_pads() {
        // The iteration order is not specified. As a result what we'll do here
        // is make sure that the bucket iterator and padding iterator both
        // return the same number of items.
        fn inner(bin_width: u8, ms: Vec<Telemetry>) -> TestResult {
            if bin_width == 0 {
                return TestResult::discard();
            }
            let mut bucket = Buckets::new(bin_width as i64);
            for m in ms.clone() {
                bucket.add(m);
            }
            let last_seen = Default::default();
            let pad_control = PadControl {
                set: false,
                sum: false,
                summarize: false,
                histogram: false,
            };
            let expected = bucket.count();
            let mut padding =
                padding(bucket.iter(), bin_width as i64, &last_seen, pad_control);

            let mut found = 0;
            while let Some(_) = padding.next() {
                found += 1;
            }
            assert_eq!(expected, found);

            TestResult::passed()
        }
        QuickCheck::new().quickcheck(inner as fn(u8, Vec<Telemetry>) -> TestResult);
    }

    #[test]
    fn test_no_unpadded_gaps() {
        fn inner(bin_width: u8, ms: Vec<Telemetry>) -> TestResult {
            if bin_width == 0 {
                return TestResult::discard();
            }
            let mut bucket = Buckets::new(bin_width as i64);
            for m in ms.clone() {
                bucket.add(m);
            }
            let last_seen = Default::default();
            let pad_control = PadControl {
                set: true,
                sum: true,
                summarize: true,
                histogram: true,
            };
            let mut padding =
                padding(bucket.iter(), bin_width as i64, &last_seen, pad_control)
                    .peekable();

            while let Some(t) = padding.next() {
                if let Some(next_t) = padding.peek() {
                    // When we examine the next point in a series there are
                    // three possibilities:
                    //
                    //  1. the points don't hash the same, so we move on
                    //  2. the points do hash the same:
                    //     a. if their timestamps are greater than one span
                    //        apart then they are both zero
                    //     b. if both points are non-zero they must not be
                    //        more than one span apart
                    if t.hash() == next_t.hash() {
                        let span = (t.timestamp() - next_t.timestamp()).abs()
                            / (bin_width as i64);
                        if span > 1 {
                            assert!(t.is_zeroed());
                            assert!(next_t.is_zeroed());
                        }
                        if !t.is_zeroed() && !next_t.is_zeroed() {
                            assert!(span <= 1);
                        }
                    } else {
                        continue;
                    }
                } else {
                    break;
                }
            }

            TestResult::passed()
        }
        QuickCheck::new().quickcheck(inner as fn(u8, Vec<Telemetry>) -> TestResult);
    }

    #[test]
    fn test_no_zero_runs() {
        // We want to elide excess zeros. This means that if we examine the
        // stream of values out of a padded stream then we should never
        // encounter more than two zero-valued Telemetry in a row.
        fn inner(bin_width: u8, ms: Vec<Telemetry>) -> TestResult {
            if bin_width == 0 {
                return TestResult::discard();
            }

            let mut bucket = Buckets::new(bin_width as i64);
            for m in ms.clone() {
                bucket.add(m);
            }

            let mut total_zero_run = 0;
            let last_seen = Default::default();
            let pad_control = PadControl {
                set: true,
                sum: true,
                summarize: true,
                histogram: true,
            };
            let padding =
                padding(bucket.iter(), bin_width as i64, &last_seen, pad_control);
            for val in padding {
                match val {
                    Pad::Zero(_, _) => total_zero_run += 1,
                    Pad::Telem(_) => total_zero_run = 0,
                }
                if total_zero_run > 2 {
                    return TestResult::failed();
                }
            }
            TestResult::passed()
        }
        QuickCheck::new().quickcheck(inner as fn(u8, Vec<Telemetry>) -> TestResult);
    }

    #[test]
    fn test_never_fewer_non_zero() {
        fn inner(bin_width: u8, ms: Vec<Telemetry>) -> TestResult {
            if bin_width == 0 {
                return TestResult::discard();
            }

            let mut bucket = Buckets::new(bin_width as i64);
            for m in ms.clone() {
                bucket.add(m);
            }

            let mut total_non_zero = 0;
            for val in bucket.clone().iter() {
                if !val.is_zeroed() {
                    total_non_zero += 1;
                }
            }

            let last_seen = Default::default();
            let pad_control = PadControl {
                set: true,
                sum: true,
                summarize: true,
                histogram: true,
            };
            let padding =
                padding(bucket.iter(), bin_width as i64, &last_seen, pad_control);
            let mut total = 0;
            for val in padding {
                if !val.is_zeroed() {
                    total += 1;
                }
            }

            assert_eq!(total_non_zero, total);
            TestResult::passed()
        }
        QuickCheck::new().quickcheck(inner as fn(u8, Vec<Telemetry>) -> TestResult);
    }

    #[test]
    fn format_retain_bin_width() {
        let mut tags = TagMap::default();
        tags.insert("source".into(), "test-src".into());
        let percentiles = vec![
            ("min".to_string(), 0.0),
            ("max".to_string(), 1.0),
            ("2".to_string(), 0.02),
            ("9".to_string(), 0.09),
            ("25".to_string(), 0.25),
            ("50".to_string(), 0.5),
            ("75".to_string(), 0.75),
            ("90".to_string(), 0.90),
            ("91".to_string(), 0.91),
            ("95".to_string(), 0.95),
            ("98".to_string(), 0.98),
            ("99".to_string(), 0.99),
            ("999".to_string(), 0.999),
        ];
        let pad_control = PadControl {
            set: true,
            sum: true,
            summarize: true,
            histogram: true,
        };
        let config = WavefrontConfig {
            bin_width: 1024,
            host: "127.0.0.1".to_string(),
            port: 1987,
            config_path: Some("sinks.wavefront".to_string()),
            tags: tags.clone(),
            percentiles: percentiles,
            flush_interval: 60,
            pad_control: pad_control,
            age_threshold: None,
        };
        let mut wavefront = Wavefront::init(config);
        wavefront.format_stats();

        assert_eq!(wavefront.bin_width, wavefront.aggrs.bin_width());
    }

    #[test]
    fn test_format_wavefront() {
        let mut tags = TagMap::default();
        tags.insert("source".into(), "test-src".into());
        let mut custom_tags = TagMap::default();
        custom_tags.insert("filter".into(), "test-filter-mod".into());
        let percentiles = vec![
            ("min".to_string(), 0.0),
            ("max".to_string(), 1.0),
            ("2".to_string(), 0.02),
            ("9".to_string(), 0.09),
            ("25".to_string(), 0.25),
            ("50".to_string(), 0.5),
            ("75".to_string(), 0.75),
            ("90".to_string(), 0.90),
            ("91".to_string(), 0.91),
            ("95".to_string(), 0.95),
            ("98".to_string(), 0.98),
            ("99".to_string(), 0.99),
            ("999".to_string(), 0.999),
        ];
        let pad_control = PadControl {
            set: true,
            sum: true,
            summarize: true,
            histogram: true,
        };
        let config = WavefrontConfig {
            bin_width: 1,
            host: "127.0.0.1".to_string(),
            port: 1987,
            config_path: Some("sinks.wavefront".to_string()),
            tags: tags,
            percentiles: percentiles,
            flush_interval: 60,
            pad_control: pad_control,
            age_threshold: None,
        };
        let mut wavefront = Wavefront::init(config);
        let dt_0 = Utc.ymd(1990, 6, 12)
            .and_hms_milli(9, 10, 11, 00)
            .timestamp();
        let dt_1 = Utc.ymd(1990, 6, 12)
            .and_hms_milli(9, 10, 12, 00)
            .timestamp();
        let dt_2 = Utc.ymd(1990, 6, 12)
            .and_hms_milli(9, 10, 13, 00)
            .timestamp();
        wavefront.deliver(
            Telemetry::new()
                .name("test.counter")
                .value(-1.0)
                .timestamp(dt_0)
                .kind(AggregationMethod::Sum)
                .harden()
                .unwrap()
                .overlay_tags_from_map(&custom_tags),
        );
        wavefront.deliver(
            Telemetry::new()
                .name("test.counter")
                .value(2.0)
                .timestamp(dt_0)
                .kind(AggregationMethod::Sum)
                .harden()
                .unwrap(),
        );
        wavefront.deliver(
            Telemetry::new()
                .name("test.counter")
                .value(3.0)
                .timestamp(dt_1)
                .kind(AggregationMethod::Sum)
                .harden()
                .unwrap(),
        );
        wavefront.deliver(
            Telemetry::new()
                .name("test.gauge")
                .value(3.211)
                .timestamp(dt_0)
                .kind(AggregationMethod::Set)
                .harden()
                .unwrap(),
        );
        wavefront.deliver(
            Telemetry::new()
                .name("test.gauge")
                .value(4.322)
                .timestamp(dt_1)
                .kind(AggregationMethod::Set)
                .harden()
                .unwrap(),
        );
        wavefront.deliver(
            Telemetry::new()
                .name("test.gauge")
                .value(5.433)
                .timestamp(dt_2)
                .kind(AggregationMethod::Set)
                .harden()
                .unwrap(),
        );
        wavefront.deliver(
            Telemetry::new()
                .name("test.timer")
                .value(12.101)
                .timestamp(dt_0)
                .kind(AggregationMethod::Summarize)
                .harden()
                .unwrap(),
        );
        wavefront.deliver(
            Telemetry::new()
                .name("test.timer")
                .value(1.101)
                .timestamp(dt_0)
                .kind(AggregationMethod::Summarize)
                .harden()
                .unwrap(),
        );
        wavefront.deliver(
            Telemetry::new()
                .name("test.timer")
                .value(3.101)
                .timestamp(dt_0)
                .kind(AggregationMethod::Summarize)
                .harden()
                .unwrap(),
        );
        wavefront.deliver(
            Telemetry::new()
                .name("test.raw")
                .value(1.0)
                .timestamp(dt_0)
                .kind(AggregationMethod::Set)
                .harden()
                .unwrap(),
        );
        wavefront.deliver(
            Telemetry::new()
                .name("test.raw")
                .value(2.0)
                .timestamp(dt_1)
                .kind(AggregationMethod::Set)
                .harden()
                .unwrap(),
        );
        wavefront.format_stats();
        let lines: Vec<&str> = wavefront.stats.lines().collect();

        println!("{:?}", lines);
        assert!(lines.contains(
            &"test.counter -1 645181811 filter=test-filter-mod source=test-src"
        ));
        assert!(lines.contains(&"test.counter 2 645181811 source=test-src"));
        assert!(lines.contains(&"test.counter 3 645181812 source=test-src"));
        assert!(lines.contains(&"test.gauge 3.211 645181811 source=test-src"));
        assert!(lines.contains(&"test.gauge 4.322 645181812 source=test-src"));
        assert!(lines.contains(&"test.gauge 5.433 645181813 source=test-src"));
        assert!(lines.contains(&"test.timer.min 1.101 645181811 source=test-src"));
        assert!(lines.contains(&"test.timer.max 12.101 645181811 source=test-src"));
        assert!(lines.contains(&"test.timer.2 1.101 645181811 source=test-src"));
        assert!(lines.contains(&"test.timer.9 1.101 645181811 source=test-src"));
        assert!(lines.contains(&"test.timer.25 1.101 645181811 source=test-src"));
        assert!(lines.contains(&"test.timer.50 3.101 645181811 source=test-src"));
        assert!(lines.contains(&"test.timer.75 3.101 645181811 source=test-src"));
        assert!(lines.contains(&"test.timer.90 12.101 645181811 source=test-src"));
        assert!(lines.contains(&"test.timer.91 12.101 645181811 source=test-src"));
        assert!(lines.contains(&"test.timer.95 12.101 645181811 source=test-src"));
        assert!(lines.contains(&"test.timer.98 12.101 645181811 source=test-src"));
        assert!(lines.contains(&"test.timer.99 12.101 645181811 source=test-src"));
        assert!(lines.contains(&"test.timer.999 12.101 645181811 source=test-src"));
        assert!(lines.contains(&"test.timer.count 3 645181811 source=test-src"));
        assert!(
            lines.contains(
                &"test.timer.mean 5.434333333333334 645181811 source=test-src"
            )
        );
        assert!(lines.contains(&"test.raw 1 645181811 source=test-src"));
    }
}
