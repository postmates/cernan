#[derive(PartialEq, Debug, Serialize, Deserialize)]
pub enum Event {
    Statsd(Metric),
    Graphite(Metric),
    TimerFlush,
    Snapshot,
}

#[derive(PartialEq, Debug, Serialize, Deserialize)]
pub enum MetricKind {
    Counter(f64),
    Gauge,
    Timer,
    Histogram,
    Raw,
}

#[derive(PartialEq, Debug, Serialize, Deserialize)]
pub struct Metric {
    pub kind: MetricKind,
    pub name: Atom,
    pub value: f64,
    pub time: i64,
}
