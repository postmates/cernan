use byteorder::{BigEndian, ReadBytesExt};
use constants;
use metric;
use mio;
use serde_avro;
use source::{TCPStreamHandler, TCP};
use std::{io, mem, sync};
use std::io::{Cursor, Read};
use util;

#[derive(Default, Debug, Clone, Deserialize)]
pub struct AvroStreamHandler;

#[repr(C, packed)]
pub struct Header {
    pub version: u32,
    pub order_by: u64,
}

impl From<Vec<u8>> for Header {
    /// Parses payload headers from raw bytes.
    /// All values assumed to be big endian.
    ///
    /// | version |    order_by     |
    /// | 0 - 31  |    32 - 95      |
    fn from(item: Vec<u8>) -> Self {
        let mut cursor = Cursor::new(item);
        let version = cursor.read_u32::<BigEndian>().unwrap();
        let order_by = cursor.read_u64::<BigEndian>().unwrap();
        Header {
            version: version,
            order_by: order_by,
        }
    }
}

impl TCPStreamHandler for AvroStreamHandler {
    /// Receives and buffers Avro events from the given stream.
    ///
    /// The stream handler exits gracefully whhen a shutdown event is received.
    fn handle_stream(
        &mut self,
        chans: util::Channel,
        tags: &sync::Arc<metric::TagMap>,
        poller: &mio::Poll,
        stream: mio::net::TcpStream,
    ) -> () {
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
    ///
    /// Payloads are assumed to be of the following form:
    /// | Length: 4 u32, BigEndian | Version: 4 u32, BigEndian  |
    /// |               OrderBy: u64, BigEndian                 |
    /// |                    Avro Blob                          |
    ///
    /// While not all sinks for Raw events will make use of OrderBy,
    /// it should be the publisher that decides when they are interested
    /// in their events being ordered.  As such, the burden of providing
    /// this value is on the publishing client.
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

        let header_size_in_bytes = mem::size_of::<Header>();
        if payload_size_in_bytes <= header_size_in_bytes {
            warn!("Invalid payload detected!  Discarding!");
            return;
        }

        let mut header_raw = Vec::with_capacity(header_size_in_bytes);
        if reader.read_exact(&mut header_raw).is_err() {
            warn!("Failed to read header from stream!");
            return;
        }
        let header : Header = header_raw.into();

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

        util::send(
            &mut chans,
            metric::Event::Raw {
                order_by: header.order_by,
                encoding: metric::Encoding::Avro,
                bytes: buf,
            },
        );
    }
}

/// Source for Avro events.
pub type Avro = TCP<AvroStreamHandler>;
