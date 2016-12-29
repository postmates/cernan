use metric;
use protocols::statsd::parse_statsd;
use source::Source;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddrV4, SocketAddrV6, UdpSocket};
use std::str;
use std::sync;
use std::thread;
use util;
use util::send;

pub struct Statsd {
    chans: util::Channel,
    port: u16,
    tags: sync::Arc<metric::TagMap>,
}

#[derive(Debug,Clone)]
pub struct StatsdConfig {
    pub ip: String,
    pub port: u16,
    pub tags: metric::TagMap,
    pub forwards: Vec<String>,
    pub config_path: String,
}

impl Default for StatsdConfig {
    fn default() -> StatsdConfig {
        StatsdConfig {
            ip: String::from(""),
            port: 8125,
            tags: metric::TagMap::default(),
            forwards: Vec::new(),
            config_path: "sources.statsd".to_string(),
        }
    }
}

impl Statsd {
    pub fn new(chans: util::Channel, config: StatsdConfig) -> Statsd {
        Statsd {
            chans: chans,
            port: config.port,
            tags: sync::Arc::new(config.tags),
        }
    }
}

fn handle_udp(mut chans: util::Channel, tags: sync::Arc<metric::TagMap>, socket: UdpSocket) {
    let mut buf = [0; 8192];
    let mut metrics = Vec::new();
    let basic_metric = sync::Arc::new(Some(metric::Metric::default().overlay_tags_from_map(&tags)));
    loop {
        let (len, _) = match socket.recv_from(&mut buf) {
            Ok(r) => r,
            Err(_) => panic!("Could not read UDP socket."),
        };
        match str::from_utf8(&buf[..len]) {
            Ok(val) => {
                if parse_statsd(val, &mut metrics, basic_metric.clone()) {
                    for m in metrics.drain(..) {
                        send("statsd", &mut chans, metric::Event::new_telemetry(m));
                    }
                    let mut metric = metric::Metric::new("cernan.statsd.packet", 1.0).counter();
                    metric = metric.overlay_tags_from_map(&tags);
                    send("statsd", &mut chans, metric::Event::new_telemetry(metric));
                } else {
                    let mut metric = metric::Metric::new("cernan.statsd.bad_packet", 1.0).counter();
                    metric = metric.overlay_tags_from_map(&tags);
                    send("statsd", &mut chans, metric::Event::new_telemetry(metric));
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

        let addr_v6 = SocketAddrV6::new(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1), self.port, 0, 0);
        let socket_v6 = UdpSocket::bind(addr_v6).expect("Unable to bind to UDP V6 socket");
        let chans_v6 = self.chans.clone();
        let tags_v6 = self.tags.clone();
        info!("server started on ::1 {}", self.port);
        joins.push(thread::spawn(move || handle_udp(chans_v6, tags_v6, socket_v6)));

        let addr_v4 = SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), self.port);
        let socket_v4 = UdpSocket::bind(addr_v4).expect("Unable to bind to UDP socket");
        let chans_v4 = self.chans.clone();
        let tags_v4 = self.tags.clone();
        info!("server started on 127.0.0.1:{}", self.port);
        joins.push(thread::spawn(move || handle_udp(chans_v4, tags_v4, socket_v4)));

        for jh in joins {
            // TODO Having sub-threads panic will not cause a bubble-up if that
            // thread is not the currently examined one. We're going to have to have
            // some manner of sub-thread communication going on.
            jh.join().expect("Uh oh, child thread paniced!");
        }
    }
}

#[cfg(test)]
mod tests {
    use metric::{Metric, MetricKind};
    use std::sync;
    use super::*;

    #[test]
    fn test_parse_negative_timer() {
        let metric = sync::Arc::new(Some(Metric::default()));
        let mut res = Vec::new();
        assert!(parse_statsd("fst:-1.1|ms\n", &mut res, metric));

        assert_eq!(res[0].kind, MetricKind::Timer);
        assert_eq!(res[0].name, "fst");
        assert_eq!(res[0].query(1.0), Some(-1.1));
    }

    #[test]
    fn test_metric_equal_in_name() {
        let metric = sync::Arc::new(Some(Metric::default()));
        let mut res = Vec::new();
        assert!(parse_statsd("A=:1|ms\n", &mut res, metric));

        assert_eq!("A=", res[0].name);
        assert_eq!(Some(1.0), res[0].query(1.0));
        assert_eq!(MetricKind::Timer, res[0].kind);
    }

    #[test]
    fn test_metric_slash_in_name() {
        let metric = sync::Arc::new(Some(Metric::default()));
        let mut res = Vec::new();
        assert!(parse_statsd("A/:1|ms\n", &mut res, metric));

        assert_eq!("A/", res[0].name);
        assert_eq!(Some(1.0), res[0].query(1.0));
        assert_eq!(MetricKind::Timer, res[0].kind);
    }

    #[test]
    fn test_metric_sample_gauge() {
        let metric = sync::Arc::new(Some(Metric::default()));
        let mut res = Vec::new();
        assert!(parse_statsd("foo:1|g@0.22\nbar:101|g@2\n", &mut res, metric));
        //                              0         A     F
        assert_eq!("foo", res[0].name);
        assert_eq!(Some(1.0), res[0].query(1.0));
        assert_eq!(MetricKind::Gauge, res[0].kind);

        assert_eq!("bar", res[1].name);
        assert_eq!(Some(101.0), res[1].query(1.0));
        assert_eq!(MetricKind::Gauge, res[1].kind);
    }

    #[test]
    fn test_metric_parse_invalid_no_name() {
        let metric = sync::Arc::new(Some(Metric::default()));
        let mut res = Vec::new();
        assert!(!parse_statsd("", &mut res, metric));
    }


    #[test]
    fn test_metric_parse_invalid_no_value() {
        let metric = sync::Arc::new(Some(Metric::default()));
        let mut res = Vec::new();
        assert!(!parse_statsd("foo:", &mut res, metric));
    }

    #[test]
    fn test_metric_multiple() {
        let metric = sync::Arc::new(Some(Metric::default()));
        let mut res = Vec::new();
        assert!(parse_statsd("a.b:12.1|g\nb_c:13.2|c\n", &mut res, metric));
        assert_eq!(2, res.len());

        assert_eq!("a.b", res[0].name);
        assert_eq!(Some(12.1), res[0].value());

        assert_eq!("b_c", res[1].name);
        assert_eq!(Some(13.2), res[1].value());
    }

    #[test]
    fn test_metric_optional_final_newline() {
        let metric = sync::Arc::new(Some(Metric::default()));
        let mut res = Vec::new();
        assert!(parse_statsd("a.b:12.1|g\nb_c:13.2|c", &mut res, metric));
        assert_eq!(2, res.len());

        assert_eq!("a.b", res[0].name);
        assert_eq!(Some(12.1), res[0].value());

        assert_eq!("b_c", res[1].name);
        assert_eq!(Some(13.2), res[1].value());
    }

    #[test]
    fn test_metric_invalid() {
        let invalid = vec!["", "metric", "metric|11:", "metric|12", "metric:13|", ":|@", ":1.0|c"];
        let metric = sync::Arc::new(Some(Metric::default()));
        for input in invalid.iter() {
            assert!(!parse_statsd(*input, &mut Vec::new(), metric.clone()));
        }
    }

    #[test]
    fn test_parse_metric_via_api() {
        let pyld = "zrth:0|g\nfst:-1.1|ms\nsnd:+2.2|g\nthd:3.3|h\nfth:4|c\nfvth:5.5|c@0.1\nsxth:\
                    -6.6|g\nsvth:+7.77|g\n";
        let metric = sync::Arc::new(Some(Metric::default()));
        let mut res = Vec::new();
        assert!(parse_statsd(pyld, &mut res, metric));

        assert_eq!(res[0].kind, MetricKind::Gauge);
        assert_eq!(res[0].name, "zrth");
        assert_eq!(res[0].value(), Some(0.0));

        assert_eq!(res[1].kind, MetricKind::Timer);
        assert_eq!(res[1].name, "fst");
        assert_eq!(res[1].query(1.0), Some(-1.1));

        assert_eq!(res[2].kind, MetricKind::DeltaGauge);
        assert_eq!(res[2].name, "snd");
        assert_eq!(res[2].value(), Some(2.2));

        assert_eq!(res[3].kind, MetricKind::Histogram);
        assert_eq!(res[3].name, "thd");
        assert_eq!(res[3].query(1.0), Some(3.3));

        assert_eq!(res[4].kind, MetricKind::Counter);
        assert_eq!(res[4].name, "fth");
        assert_eq!(res[4].value(), Some(4.0));

        assert_eq!(res[5].kind, MetricKind::Counter);
        assert_eq!(res[5].name, "fvth");
        assert_eq!(res[5].value(), Some(55.0));

        assert_eq!(res[6].kind, MetricKind::DeltaGauge);
        assert_eq!(res[6].name, "sxth");
        assert_eq!(res[6].value(), Some(-6.6));

        assert_eq!(res[7].kind, MetricKind::DeltaGauge);
        assert_eq!(res[7].name, "svth");
        assert_eq!(res[7].value(), Some(7.77));
    }
}
