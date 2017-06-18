use metric;
use protocols::statsd::parse_statsd;
use source::Source;
use source::internal::report_telemetry;
use std::net::{ToSocketAddrs, UdpSocket};
use std::str;
use std::sync;
use std::thread;
use util;
use util::send;

pub struct Statsd {
    chans: util::Channel,
    host: String,
    port: u16,
    tags: sync::Arc<metric::TagMap>,
    telemetry_error: f64,
}

#[derive(Debug, Deserialize, Clone)]
pub struct StatsdConfig {
    pub host: String,
    pub port: u16,
    pub tags: metric::TagMap,
    pub forwards: Vec<String>,
    pub config_path: Option<String>,
    pub delete_gauges: bool,
    pub telemetry_error: f64,
}

impl Default for StatsdConfig {
    fn default() -> StatsdConfig {
        StatsdConfig {
            host: "localhost".to_string(),
            port: 8125,
            tags: metric::TagMap::default(),
            forwards: Vec::new(),
            config_path: None,
            delete_gauges: false,
            telemetry_error: 0.001,
        }
    }
}

impl Statsd {
    pub fn new(chans: util::Channel, config: StatsdConfig) -> Statsd {
        Statsd {
            chans: chans,
            host: config.host,
            port: config.port,
            tags: sync::Arc::new(config.tags),
            telemetry_error: config.telemetry_error,
        }
    }
}

fn handle_udp(mut chans: util::Channel,
              tags: sync::Arc<metric::TagMap>,
              socket: &UdpSocket,
              telemetry_error: f64) {
    let mut buf = [0; 8192];
    let mut metrics = Vec::new();
    let basic_metric = sync::Arc::new(Some(metric::Telemetry::default()
                                               .overlay_tags_from_map(&tags)));
    loop {
        let (len, _) = match socket.recv_from(&mut buf) {
            Ok(r) => r,
            Err(_) => panic!("Could not read UDP socket."),
        };
        match str::from_utf8(&buf[..len]) {
            Ok(val) => {
                if parse_statsd(val, &mut metrics, basic_metric.clone()) {
                    for m in metrics.drain(..) {
                        send(&mut chans, metric::Event::new_telemetry(m));
                    }
                    report_telemetry("cernan.statsd.packet", 1.0, telemetry_error);
                } else {
                    report_telemetry("cernan.statsd.bad_packet", 1.0, telemetry_error);
                    error!("BAD PACKET: {:?}", val);
                }
            }
            Err(e) => {
                error!("Payload not valid UTF-8: {:?}", e);
            }
        }
    }
}

impl Source for Statsd {
    fn run(&mut self) {
        let mut joins = Vec::new();

        let addrs = (self.host.as_str(), self.port).to_socket_addrs();
        match addrs {
            Ok(ips) => {
                let ips: Vec<_> = ips.collect();
                for addr in ips {
                    let listener =
                        UdpSocket::bind(addr).expect("Unable to bind to TCP socket");
                    let chans = self.chans.clone();
                    let tags = self.tags.clone();
                    info!("server started on {:?} {}", addr, self.port);
                    let error = self.telemetry_error;
                    joins.push(thread::spawn(move || {
                                                 handle_udp(chans, tags, &listener, error)
                                             }));
                }
            }
            Err(e) => {
                info!("Unable to perform DNS lookup on host {} with error {}",
                      self.host,
                      e);
            }
        }

        for jh in joins {
            // TODO Having sub-threads panic will not cause a bubble-up if that
            // thread is not the currently examined one. We're going to have to have
            // some manner of sub-thread communication going on.
            jh.join().expect("Uh oh, child thread panicked!");
        }
    }
}
