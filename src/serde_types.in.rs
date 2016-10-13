#[derive(PartialEq, Debug, Serialize, Deserialize, Clone)]
pub struct LogLine {
    pub time: i64,
    pub path: String,
    pub value: String,
    pub tags: TagMap,
}

#[derive(PartialEq, Debug, Serialize, Deserialize, Clone)]
pub struct Metric {
    pub kind: MetricKind,
    pub name: String,
    pub tags: TagMap,
    pub time: i64,
    pub value: f64,
}

#[derive(PartialEq, Debug, Serialize, Deserialize, Clone)]
pub enum Event {
    Statsd(Metric),
    Graphite(Metric),
    Log(Vec<LogLine>),
    TimerFlush,
}

#[derive(PartialEq, PartialOrd, Debug, Serialize, Deserialize, Clone)]
pub enum MetricKind {
    Counter(f64),
    Gauge,
    DeltaGauge,
    Timer,
    Histogram,
    Raw,
}
