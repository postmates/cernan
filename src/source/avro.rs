use byteorder::{BigEndian, ReadBytesExt};
use constants;
use metric;
use mio;
use serde_avro;
use source;
use std::{io, sync};
use std::io::Read;
use thread;
use util;

/// Avro source state
pub struct Avro {
    server: source::TCP,
}

/// Monitors for inbound system and TCP events.
fn stream_handler(
    chans: util::Channel,
    tags: &sync::Arc<metric::TagMap>,
    poller: &mio::Poll,
    stream: mio::net::TcpStream,
) {
    let mut reader = io::BufReader::new(stream);
    loop {
        let mut events = mio::Events::with_capacity(1024);
        match poller.poll(&mut events, None) {
            Err(e) => panic!(format!("Failed during poll {:?}", e)),
            Ok(_num_events) => for event in events {
                match event.token() {
                    constants::SYSTEM => return,
                    _stream_token => {
                        handle_avro_payload(chans.clone(), tags, &mut reader);
                    }
                }
            },
        };
    }
}

/// Pulls length prefixed (4 bytes, BE), avro encoded
/// binaries off the wire and populates them in the configured
/// channel.
fn handle_avro_payload(
    mut chans: util::Channel,
    _tags: &sync::Arc<metric::TagMap>,
    reader: &mut io::BufReader<mio::net::TcpStream>,
) {
    let mut buf = Vec::with_capacity(4000);
    let payload_size_in_bytes = match reader.read_u32::<BigEndian>() {
        Ok(i) => i as usize,
        Err(_) => return,
    };
    buf.resize(payload_size_in_bytes, 0);
    if reader.read_exact(&mut buf).is_err() {
        return;
    }

    match serde_avro::de::Deserializer::from_container(&buf[..]) {
        Ok(_avro_de) => {
            trace!("Successfully deserialized container.");
            // TODO - Enforce configurable type naming requirements.
        }

        Err(e) => {
            trace!("Failed to deserialize container: {:?}", e.description());
            return;
        }
    }

    util::send(&mut chans, metric::Event::Raw(metric::Encoding::Avro, buf));
}

impl source::Source<Avro, source::TCPConfig> for Avro {
    /// Creates and starts an Avro source witht the given config.
    fn new(chans: util::Channel, config: source::TCPConfig) -> Self {
        Avro {
            server: source::TCP::new(chans, config),
        }
    }

    fn run(self) -> thread::ThreadHandle {
        self.server.run(stream_handler)
    }
}
