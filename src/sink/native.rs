use byteorder::{BigEndian, ByteOrder};
use hopper;
use metric;
use protobuf::Message;
use protobuf::repeated::RepeatedField;
use protobuf::stream::CodedOutputStream;
use protocols::native::{AggregationMethod, LogLine, Payload, Telemetry};
use sink::{Sink, Valve};
use std::collections::HashMap;
use std::io::BufWriter;
use std::mem::replace;
use std::net::{TcpStream, ToSocketAddrs};
use std::sync;
use time;

pub struct Native {
    port: u16,
    host: String,
    buffer: Vec<metric::Event>,
    flush_interval: u64,
    delivery_attempts: u32,
    stream: Option<TcpStream>,
}

#[derive(Debug, Deserialize)]
pub struct NativeConfig {
    pub port: u16,
    pub host: String,
    pub config_path: Option<String>,
    pub flush_interval: u64,
}

impl Default for NativeConfig {
    fn default() -> Self {
        NativeConfig {
            port: 1972,
            host: "localhost".to_string(),
            config_path: None,
            flush_interval: 60,
        }
    }
}

impl Native {
    pub fn new(config: NativeConfig) -> Native {
        let stream = connect(&config.host, config.port);
        Native {
            port: config.port,
            host: config.host,
            buffer: Vec::new(),
            flush_interval: config.flush_interval,
            delivery_attempts: 0,
            stream: stream,
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
                    Err(e) => {
                        info!(
                            "Unable to connect to proxy at {} using addr {} with error \
                             {}",
                            host,
                            ip,
                            e
                        )
                    }
                }
            }
            None
        }
        Err(e) => {
            info!(
                "Unable to perform DNS lookup on host {} with error {}",
                host,
                e
            );
            None
        }
    }
}

impl Sink for Native {
    fn valve_state(&self) -> Valve {
        Valve::Open
    }

    fn deliver(&mut self, _: sync::Arc<Option<metric::Telemetry>>) -> () {
        // discard point
    }

    fn deliver_line(&mut self, _: sync::Arc<Option<metric::LogLine>>) -> () {
        // discard point
    }

    fn run(&mut self, recv: hopper::Receiver<metric::Event>) {
        let mut attempts = 0;
        let mut recv = recv.into_iter();
        let mut last_flush_idx = 0;
        loop {
            time::delay(attempts);
            if self.buffer.len() > 10_000 {
                attempts += 1;
                continue;
            }
            match recv.next() {
                None => attempts += 1,
                Some(event) => {
                    attempts = 0;
                    match event {
                        metric::Event::TimerFlush(idx) => {
                            if idx > last_flush_idx {
                                if let Some(flush_interval) = self.flush_interval() {
                                    if idx % flush_interval == 0 {
                                        self.flush();
                                    }
                                }
                                last_flush_idx = idx;
                            }
                        }
                        _ => self.buffer.push(event),
                    }
                }
            }
        }
    }

    fn flush_interval(&self) -> Option<u64> {
        Some(self.flush_interval)
    }

    fn flush(&mut self) {
        let mut points = Vec::with_capacity(1024);
        let mut lines = Vec::with_capacity(1024);

        for ev in self.buffer.drain(..) {
            match ev {
                metric::Event::Telemetry(mut m) => {
                    let mut m = sync::Arc::make_mut(&mut m).take().unwrap();
                    let mut telem = Telemetry::new();
                    telem.set_name(replace(&mut m.name, Default::default()));
                    let method = match m.aggregation() {
                        metric::AggregationMethod::Histogram => AggregationMethod::BIN,
                        metric::AggregationMethod::Sum => AggregationMethod::SUM,
                        metric::AggregationMethod::Set => AggregationMethod::SET,
                        metric::AggregationMethod::Summarize => {
                            AggregationMethod::SUMMARIZE
                        }
                    };
                    let persist = m.persist;
                    telem.set_persisted(persist);
                    telem.set_method(method);
                    let mut meta = HashMap::new();
                    // TODO
                    //
                    // Learn how to consume bits of the metric without having to
                    // clone like crazy
                    for (k, v) in m.tags.into_iter() {
                        meta.insert(k.clone(), v.clone());
                    }
                    telem.set_metadata(meta);
                    telem.set_timestamp_ms(m.timestamp * 1000); // FIXME #166
                    telem.set_samples(m.samples());
                    // TODO set bin_bounds. What we do is set the counts for the
                    // bins as set_samples above, then bin_bounds comes from
                    // elsewhere
                    points.push(telem);
                }
                metric::Event::Log(mut l) => {
                    let l = sync::Arc::make_mut(&mut l).take().unwrap();
                    let mut ll = LogLine::new();
                    ll.set_path(l.path);
                    ll.set_value(l.value);
                    let mut meta = HashMap::new();
                    // TODO
                    //
                    // Learn how to consume bits of the metric without having to
                    // clone like crazy
                    for (k, v) in &l.tags {
                        meta.insert(k.clone(), v.clone());
                    }
                    ll.set_metadata(meta);
                    ll.set_timestamp_ms(l.time * 1000); // FIXME #166

                    lines.push(ll);
                }
                _ => {}
            }
        }

        let mut pyld = Payload::new();
        pyld.set_points(RepeatedField::from_vec(points));
        pyld.set_lines(RepeatedField::from_vec(lines));

        loop {
            let mut delivery_failure = false;
            if let Some(ref mut stream) = self.stream {
                let mut bufwrite = BufWriter::new(stream);
                let mut stream = CodedOutputStream::new(&mut bufwrite);
                let mut sz_buf = [0; 4];
                let pyld_len = pyld.compute_size();
                BigEndian::write_u32(&mut sz_buf, pyld_len);
                stream.write_raw_bytes(&sz_buf).unwrap();
                let res = pyld.write_to_with_cached_sizes(&mut stream);
                if res.is_ok() {
                    self.buffer.clear();
                    return;
                } else {
                    self.delivery_attempts = self.delivery_attempts.saturating_add(1);
                    delivery_failure = true;
                }
            } else {
                time::delay(self.delivery_attempts);
                self.stream = connect(&self.host, self.port);
            }
            if delivery_failure {
                self.stream = None
            }
        }
    }
}
