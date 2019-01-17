use crate::constants;
use crate::metric;
use mio;
use crate::protocols::statsd::parse_statsd;
use regex::Regex;
use crate::source;
use std::io::ErrorKind;
use std::net::ToSocketAddrs;
use std::str;
use std::sync;
use std::sync::atomic::{AtomicUsize, Ordering};
use crate::util;
use crate::util::send;

pub static STATSD_GOOD_PACKET: AtomicUsize = AtomicUsize::new(0);
pub static STATSD_BAD_PACKET: AtomicUsize = AtomicUsize::new(0);

/// The statsd source
///
/// Statsd is a collection of protocols, originally spawned by the telemetering
/// work done out of Etsy. Cernan tries to support a cow-path subset of the
/// statsd protocol family.
pub struct Statsd {
    conns: util::TokenSlab<mio::net::UdpSocket>,
    parse_config: sync::Arc<StatsdParseConfig>,
}

/// The mask type for metrics in `StatsdParseConfig`.
pub type Mask = Regex;

/// The bound type for metrics in `StatsdParseConfig`.
pub type Bounds = Vec<f64>;

/// Configuration for the statsd parser
#[derive(Debug, Clone)]
pub struct StatsdParseConfig {
    /// Set specific bin masks for timeseries according to their name. The name
    /// may be a [regex](https://crates.io/crates/regex) match, such like
    /// 'foo.*'. In this case all metrics prefixed by 'foo.' which are timer or
    /// histogram will be interpreted as a histogram.
    pub histogram_masks: Vec<(Mask, Bounds)>,
    /// Configure the error bound for a statsd timer or histogram. Cernan does
    /// not compute precise quantiles but approximations with a guaranteed upper
    /// bound on the error of approximation. This allows the end-user to set
    /// that.
    pub summarize_error_bound: f64,
}

impl Default for StatsdParseConfig {
    fn default() -> StatsdParseConfig {
        StatsdParseConfig {
            histogram_masks: vec![],
            summarize_error_bound: 0.01,
        }
    }
}

/// Configuration for the statsd source.
#[derive(Debug, Clone)]
pub struct StatsdConfig {
    /// The host for the statsd protocol to bind to.
    pub host: String,
    /// The port for the statsd source to listen on.
    pub port: u16,
    /// The forwards that statsd will send its telemetry on to.
    pub forwards: Vec<String>,
    /// The unique name for the source in the routing topology.
    pub config_path: Option<String>,
    /// Configuration for the parsing of statsd lines
    pub parse_config: StatsdParseConfig,
}

impl Default for StatsdConfig {
    fn default() -> StatsdConfig {
        StatsdConfig {
            host: "localhost".to_string(),
            port: 8125,
            forwards: Vec::new(),
            config_path: None,
            parse_config: StatsdParseConfig::default(),
        }
    }
}

enum StatsdHandlerErr {
    Fatal,
}

impl Statsd {
    fn handle_datagrams(
        &self,
        mut chans: &mut util::Channel,
        socket: &mio::net::UdpSocket,
        mut buf: &mut Vec<u8>,
    ) -> Result<(), StatsdHandlerErr> {
        let mut metrics = Vec::new();
        let basic_metric = sync::Arc::new(Some(metric::Telemetry::default()));
        loop {
            match socket.recv_from(&mut buf) {
                Ok((len, _)) => match str::from_utf8(&buf[..len]) {
                    Ok(val) => if parse_statsd(
                        val,
                        &mut metrics,
                        &basic_metric,
                        &self.parse_config,
                    ) {
                        for m in metrics.drain(..) {
                            send(&mut chans, metric::Event::new_telemetry(m));
                        }
                        STATSD_GOOD_PACKET.fetch_add(1, Ordering::Relaxed);
                    } else {
                        STATSD_BAD_PACKET.fetch_add(1, Ordering::Relaxed);
                        error!("BAD PACKET: {:?}", val);
                    },
                    Err(e) => {
                        error!("Payload not valid UTF-8: {:?}", e);
                    }
                },
                Err(e) => match e.kind() {
                    ErrorKind::WouldBlock => {
                        break;
                    }
                    _ => {
                        error!("Could not read UDP socket with error {:?}", e);
                        return Err(StatsdHandlerErr::Fatal);
                    }
                },
            }
        }
        Ok(())
    }
}

impl source::Source<StatsdConfig> for Statsd {
    /// Create and spawn a new statsd source
    fn init(config: StatsdConfig) -> Self {
        let mut conns = util::TokenSlab::<mio::net::UdpSocket>::new();
        let addrs = (config.host.as_str(), config.port).to_socket_addrs();
        match addrs {
            Ok(ips) => for addr in ips {
                let socket = mio::net::UdpSocket::bind(&addr)
                    .expect("Unable to bind to UDP socket");
                conns.insert(socket);
            },
            Err(e) => {
                info!(
                    "Unable to perform DNS lookup on host {} with error {}",
                    config.host, e
                );
            }
        };

        Statsd {
            conns: conns,
            parse_config: sync::Arc::new(config.parse_config),
        }
    }

    fn run(self, mut chans: util::Channel, poller: mio::Poll) -> () {
        for (idx, socket) in self.conns.iter() {
            if let Err(e) = poller.register(
                socket,
                mio::Token::from(idx),
                mio::Ready::readable(),
                mio::PollOpt::edge(),
            ) {
                error!("Failed to register {:?} - {:?}!", socket, e);
            }
        }

        let mut buf = vec![0; 16_250];
        loop {
            let mut events = mio::Events::with_capacity(1024);
            match poller.poll(&mut events, None) {
                Ok(_num_events) => {
                    for event in events {
                        match event.token() {
                            constants::SYSTEM => {
                                send(&mut chans, metric::Event::Shutdown);
                                return;
                            }

                            token => {
                                let mut socket = &self.conns[token];
                                if let Err(_e) =
                                    self.handle_datagrams(&mut chans, socket, &mut buf)
                                {
                                    error!("Deregistering {:?} due to unrecoverable error!", *socket);
                                }
                            }
                        }
                    }
                }
                Err(e) => panic!(format!("Failed during poll {:?}", e)),
            }
        } // loop
    } // run
}
