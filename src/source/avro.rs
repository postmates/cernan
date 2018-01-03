use byteorder::{BigEndian, ReadBytesExt};
use constants;
use metric;
use mio;
use serde_avro;
use source::{TCPStreamHandler, TCP};
use std::{io, mem, sync};
use std::io::{Cursor, Read};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use util;

lazy_static! {
    /// Total payloads processed.
    pub static ref AVRO_PAYLOAD_SUCCESS_SUM: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
    /// Total fatal failures to parse.
    pub static ref AVRO_PAYLOAD_FATAL_SUM: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
}

#[derive(Default, Debug, Clone, Deserialize)]
pub struct AvroStreamHandler;

#[derive(Clone, Copy, Debug, PartialEq)]
#[repr(C, packed)]
pub struct Header {
    pub version: u32,
    pub order_by: u64,
}

impl<'a> From<&'a mut Cursor<Vec<u8>>> for Header {
    /// Parses payload headers from raw bytes.
    /// All values assumed to be big endian.
    ///
    /// | version |    order_by     |
    /// | 0 - 31  |    32 - 95      |
    fn from(cursor: &'a mut Cursor<Vec<u8>>) -> Self {
        let version = cursor.read_u32::<BigEndian>().unwrap();
        let order_by = cursor.read_u64::<BigEndian>().unwrap();
        Header {
            version: version,
            order_by: order_by,
        }
    }
}

#[derive(Debug, PartialEq)]
enum Payload {
    Invalid(String),
    Valid { header: Header, avro_blob: Vec<u8> },
}

impl From<Cursor<Vec<u8>>> for Payload {
    /// Transforms cursors of payloads into Payload objects.
    fn from(mut cursor: Cursor<Vec<u8>>) -> Payload {
        let header: Header = (&mut cursor).into();

        // Read the avro payload off the wire
        let mut avro_blob = Vec::new();
        if cursor.read_to_end(&mut avro_blob).is_err() {
            return Payload::Invalid(format!("Failed to read avro payload!"));
        }

        // TODO - Enforce configurable type naming requirements.
        // Check that the blob provided is valid Avro.
        if let Err(e) = serde_avro::de::Deserializer::from_container(&avro_blob[..]) {
            return Payload::Invalid(format!(
                "Failed to deserialize container - {:?}.",
                e
            ));
        };

        trace!("Successfully deserialized container.");
        Payload::Valid {
            header: header,
            avro_blob: avro_blob,
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
                            match self.handle_avro_payload(chans.clone(), tags, &mut reader) {
                                Ok(_) => {
                                    trace!("Avro payloads processed successfully.");
                                    AVRO_PAYLOAD_SUCCESS_SUM.fetch_add(1, Ordering::Relaxed);
                                }
                                Err(e) => {
                                    error!("Failed to process avro payload: {:?}", e);
                                    AVRO_PAYLOAD_FATAL_SUM.fetch_add(1, Ordering::Relaxed);
                                    return;
                                }
                            }
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
    ) -> Result<(), String>{
        let header_size_in_bytes = mem::size_of::<Header>();
        let payload_size_in_bytes = match reader.read_u32::<BigEndian>() {
            Ok(i) => i as usize,
            Err(_) => return Err(format!("Failed to parse payload length from the wire!"))
        };

        if payload_size_in_bytes <= header_size_in_bytes {
            return Err(format!("Received payload is too small!"))
        }

        let mut buf = Vec::with_capacity(payload_size_in_bytes);
        if reader.read_exact(&mut buf).is_err() {
            return Err(format!("Failed to read payload from the wire!"))
        }

        match Cursor::new(buf).into() {
            Payload::Valid { header, avro_blob } => {
                util::send(
                    &mut chans,
                    metric::Event::Raw {
                        order_by: header.order_by,
                        encoding: metric::Encoding::Avro,
                        bytes: avro_blob,
                    },
                );
                Ok(())
            }

            Payload::Invalid(e) => {
                return Err(format!("Failed while parsing payload contents: {:?}", e))
            }
        }
    }
}

/// Source for Avro events.
pub type Avro = TCP<AvroStreamHandler>;

#[cfg(test)]
mod test {
    use super::*;
    use byteorder::WriteBytesExt;
    use glob;
    use std::fs::File;
    use std::io::Write;

    #[test]
    fn parse_payload_happy_path() {
        let test_data_path =
            format!("{}/resources/tests/data/*.avro", env!("CARGO_MANIFEST_DIR"));
        for test_path in
            glob::glob(&test_data_path).expect("Failed to glob test data!")
        {
            match test_path {
                Err(e) => {
                    warn!("Failed to load avro test file {:?}", e);
                    assert!(false);
                }

                Ok(test_file) => {
                    println!("Testing {:?}", test_file);
                    let mut test_file_data = File::open(test_file).unwrap();
                    let mut avro_blob = Vec::new();
                    test_file_data
                        .read_to_end(&mut avro_blob)
                        .expect("Failed to read testdata!");

                    let buf = Vec::new();
                    let mut write_cursor = Cursor::new(buf);
                    assert!(write_cursor.write_u32::<BigEndian>(0).is_ok());
                    assert!(write_cursor.write_u64::<BigEndian>(0).is_ok());
                    assert!(write_cursor.write_all(&avro_blob[..]).is_ok());

                    let read_cursor = Cursor::new(write_cursor.into_inner());
                    let payload: Payload = read_cursor.into();

                    let expected = Payload::Valid {
                        header: Header {
                            version: 0,
                            order_by: 0,
                        },
                        avro_blob: Vec::from(avro_blob),
                    };
                    assert!(payload == expected);
                }
            }
        }
    }
}
