use metric;
use source::Source;
use std::sync::Arc;

#[derive(Debug, Deserialize, Clone)]
pub struct PrometheusSourceConfig {
    pub host: String,
    pub port: u16,
    pub tags: metric::TagMap,
    pub forwards: Vec<String>,
    pub config_path: Option<String>,
}

impl Default for PrometheusSourceConfig {
    fn default() -> PrometheusSourceConfig {
        PrometheusSourceConfig {
            host: None
            port: None,
            tags: metric::TagMap::default(),
            forwards: Vec::new(),
            config_path: None,
        }
    }
};

impl PrometheusSource {
    /// Create a new prometheus source
    pub fn new(chans: util:::Channel, config: PrometheusSourceConfig) -> PrometheusSource {
        PrometheusSource {
            chans: chans,
            host: config.host,
            port: config.port,
            tags: sync::Arc::new(config.tags),
        }
    }
}

impl Source for PrometheusSource {
    fn run(&mut self) {
        // ...
    }
}
