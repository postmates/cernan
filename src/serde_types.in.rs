#[derive(PartialEq, Debug, Serialize, Deserialize, Clone)]
pub enum Event {
    Statsd(Metric),
    Graphite(Metric),
    TimerFlush,
    Snapshot,
}

#[derive(PartialEq, Debug, Serialize, Deserialize, Clone)]
pub enum MetricKind {
    Counter(f64),
    Gauge,
    DeltaGauge,
    Timer,
    Histogram,
    Raw,
}

#[derive(PartialEq, Debug, Serialize, Deserialize, Clone)]
pub struct Metric {
    pub kind: MetricKind,
    pub name: Atom,
    pub value: f64,
    pub time: i64,
}
