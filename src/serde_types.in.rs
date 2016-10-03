#[derive(PartialEq, Debug, Serialize, Deserialize, Clone)]
pub struct LogLine {
    pub path: String,
    pub value: String,
    pub time: i64,
}

#[derive(PartialEq, Debug, Serialize, Deserialize, Clone)]
pub enum Event {
    Statsd(Metric),
    Graphite(Metric),
    Log(Vec<LogLine>),
    TimerFlush,
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
    pub name: String,
    pub value: f64,
    pub time: i64,
}
