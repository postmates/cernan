use byteorder::{BigEndian, ReadBytesExt};
use constants;
use metric;
use mio;
use serde_avro;
use source::{TCP, TCPStreamHandler};
use std::{io, sync};
use std::io::Read;
use util;

#[derive(Default, Debug, Clone, Deserialize)]
pub struct AvroStreamHandler ;

impl TCPStreamHandler for AvroStreamHandler {

    /// Receives and buffers Avro events from the given stream.
    ///
    /// The stream handler exits gracefully whhen a shutdown event is received.
    fn handle_stream(&mut self, chans: util::Channel, tags: &sync::Arc<metric::TagMap>, poller: &mio::Poll, stream: mio::net::TcpStream)-> () {
        let mut reader = io::BufReader::new(stream);
        loop {
            let mut events = mio::Events::with_capacity(1024);
            match poller.poll(&mut events, None) {
                Err(e) => panic!(format!("Failed during poll {:?}", e)),
                Ok(_num_events) => for event in events {
                    match event.token() {
                        constants::SYSTEM => return,
                        _stream_token => {
                            self.handle_avro_payload(chans.clone(), tags, &mut reader);
                        }
                    }
                },
            };
        }
    }
}

impl AvroStreamHandler {

    /// Pulls length prefixed (4 bytes, BE), avro encoded
    /// binaries off the wire and populates them in the configured
    /// channel.
    fn handle_avro_payload(
        &mut self,
        mut chans: util::Channel,
        _tags: &sync::Arc<metric::TagMap>,
        reader: &mut io::BufReader<mio::net::TcpStream>,
    ) {
        let payload_size_in_bytes = match reader.read_u32::<BigEndian>() {
            Ok(i) => i as usize,
            Err(_) => return,
        };

        let mut buf = Vec::with_capacity(payload_size_in_bytes);
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

        util::send(&mut chans, metric::Event::Raw{encoding:metric::Encoding::Avro, bytes: buf});
    }
}

/// Source for Avro events.
pub type Avro = TCP<AvroStreamHandler>;
