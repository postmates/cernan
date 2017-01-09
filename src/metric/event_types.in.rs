#[derive(PartialEq, Debug, Serialize, Deserialize, Clone)]
pub enum Event {
    Telemetry(sync::Arc<Option<Telemetry>>),
    Log(sync::Arc<Option<LogLine>>),
    TimerFlush,
}
