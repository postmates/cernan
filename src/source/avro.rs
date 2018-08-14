use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use constants;
use metric;
use mio;
use serde_avro;
use source::{TCPStreamHandler, TCP};
use source::nonblocking::{write_all, BufferedPayload, PayloadErr};
use std::io::{Cursor, Read};
use std::net;
use std::sync::atomic::{AtomicUsize, Ordering};
use util;
use uuid::Uuid;

/// Total payloads processed.
pub static AVRO_PAYLOAD_SUCCESS_SUM: AtomicUsize = AtomicUsize::new(0);
/// Total fatal parse failures.
pub static AVRO_PAYLOAD_PARSE_FAILURE_SUM: AtomicUsize = AtomicUsize::new(0);
/// Total fatal IO related errors.
pub static AVRO_PAYLOAD_IO_FAILURE_SUM: AtomicUsize = AtomicUsize::new(0);

#[derive(Default, Debug, Clone, Deserialize)]
pub struct AvroStreamHandler;

#[derive(Clone, Copy, Debug, PartialEq)]
#[repr(C, packed)]
pub struct Header {
    pub version: u32,
    pub control: u32,
    pub id: u64,
    pub order_by: u64,
}

impl<'a> From<&'a mut Cursor<Vec<u8>>> for Header {
    /// Parses payload headers from raw bytes.
    /// All values assumed to be big endian.
    ///
    /// | version - 4 bytes   |   control - 4 bytes  |
    /// |               id - 8 bytes                 |
    /// |           order_by - 8 bytes               |
    ///
    /// The above fields have the following semantics:
    ///
    /// * version   -   Version of Avro source wire protocol.
    /// * control   -   Metadata governing how the payload is to be published.
    /// * id        -   Client assigned id for the payload.  Sent in reply on
    ///                 successful publication when the control field indicates
    ///                 a sync publish.
    /// * order_by  -   Value used by some sinks to order payloads within buckets.
    fn from(cursor: &'a mut Cursor<Vec<u8>>) -> Self {
        let version = cursor.read_u32::<BigEndian>().unwrap();
        let control = cursor.read_u32::<BigEndian>().unwrap();
        let id = cursor.read_u64::<BigEndian>().unwrap();
        let order_by = cursor.read_u64::<BigEndian>().unwrap();
        Header {
            version: version,
            control: control,
            id: id,
            order_by: order_by,
        }
    }
}

impl Header {
    /// Client expects an acknowledgement after publish to Hopper.
    const CONTROL_SYNC: u32 = 1;

    /// Does the given header indicate the payload as a sync. publish?
    pub fn sync(self) -> bool {
        (self.control & Header::CONTROL_SYNC) > 0
    }
}

#[derive(Debug, PartialEq)]
enum Payload {
    Invalid(String),
    Valid { header: Header, avro_blob: Vec<u8> },
}

impl From<Vec<u8>> for Payload {
    /// Transforms cursors of payloads into Payload objects.
    fn from(vec: Vec<u8>) -> Payload {
        let mut cursor = Cursor::new(vec);
        let header: Header = (&mut cursor).into();

        // Read the avro payload off the wire
        let mut avro_blob = Vec::new();
        if cursor.read_to_end(&mut avro_blob).is_err() {
            return Payload::Invalid("Failed to read avro payload!".to_string());
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
    /// The stream handler exits gracefully when a shutdown event is received.
    fn handle_stream(
        &mut self,
        chans: util::Channel,
        poller: &mio::Poll,
        stream: mio::net::TcpStream,
    ) -> () {
        let connection_id = Uuid::new_v4();
        let mut streaming = true;
        let mut reader = BufferedPayload::new(stream.try_clone().unwrap(), 1_048_576);
        while streaming {
            let mut events = mio::Events::with_capacity(1024);
            match poller.poll(&mut events, None) {
                Err(e) => panic!(format!("Failed during poll {:?}", e)),
                Ok(_num_events) => for event in events {
                    match event.token() {
                        constants::SYSTEM => {
                            streaming = false;
                            break;
                        }
                        _stream_token => {
                            while streaming {
                                match reader.read() {
                                    Ok(payload) => {
                                        let handle_res = self.handle_avro_payload(
                                            chans.clone(),
                                            payload,
                                            connection_id,
                                        );
                                        if handle_res.is_err() {
                                            AVRO_PAYLOAD_PARSE_FAILURE_SUM
                                                .fetch_add(1, Ordering::Relaxed);
                                            streaming = false;
                                            break;
                                        }

                                        trace!(
                                            "Avro payloads processed successfully."
                                        );
                                        AVRO_PAYLOAD_SUCCESS_SUM
                                            .fetch_add(1, Ordering::Relaxed);

                                        let maybe_id = handle_res.unwrap();
                                        if let Some(id) = maybe_id {
                                            let mut resp = Cursor::new(Vec::new());
                                            resp.write_u64::<BigEndian>(id).expect(
                                                "Failed to write response id!",
                                            );
                                            write_all(&stream, resp.get_ref())
                                                .expect("Failed to write response!");
                                            trace!("Acked {:?}", id);
                                        }
                                    }

                                    Err(PayloadErr::WouldBlock) => {
                                        // Not enough data on the wire.  Try again
                                        // later.
                                        break;
                                    }

                                    Err(PayloadErr::EOF) => {
                                        // Client went away.  Shut it down
                                        // (gracefully).
                                        trace!("TCP stream closed.");
                                        streaming = false;
                                        break;
                                    }

                                    Err(e) => {
                                        // Something unexpected / fatal.
                                        error!(
                                            "Failed to process avro payload: {:?}",
                                            e
                                        );
                                        AVRO_PAYLOAD_IO_FAILURE_SUM
                                            .fetch_add(1, Ordering::Relaxed);
                                        streaming = false;
                                        break;
                                    }
                                }
                            } // WouldBlock loop
                        }
                    }
                },
            }
        } // while streaming

        // On some systems shutting down an already closed connection (client or
        // otherwise) results in an Err.  See -
        // https://doc.rust-lang.org/beta/std/net/struct.TcpStream.html#platform-specific-behavior
        let _shutdown_result = stream.shutdown(net::Shutdown::Both);
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
        payload: Vec<u8>,
        connection_id: Uuid
    ) -> Result<Option<u64>, PayloadErr> {
        match payload.into() {
            Payload::Valid { header, avro_blob } => {
                let ackbag = metric::global_ack_bag();
                if header.sync() {
                    ackbag.prepare_wait(connection_id)
                }

                util::send(
                    &mut chans,
                    metric::Event::Raw {
                        order_by: header.order_by,
                        encoding: metric::Encoding::Avro,
                        bytes: avro_blob,
                        connection_id: Some(connection_id)
                    },
                );

                if header.sync() {
                    ackbag.wait_for(connection_id);
                    ackbag.remove(connection_id);
                    Ok(Some(header.id))
                } else {
                    Ok(None)
                }
            }

            Payload::Invalid(e) => Err(PayloadErr::Protocol(format!(
                "Failed while parsing payload contents: {:?}!",
                e
            ))),
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
    fn header_async() {
        let header = Header {
            version: 0,
            control: 0,
            id: 0,
            order_by: 0,
        };

        assert!(!header.sync());
    }

    #[test]
    fn header_sync() {
        let header = Header {
            version: 0,
            control: Header::CONTROL_SYNC,
            id: 0,
            order_by: 0,
        };

        assert!(header.sync());
    }

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
                    assert!(write_cursor.write_u32::<BigEndian>(0).is_ok());
                    assert!(write_cursor.write_u64::<BigEndian>(0).is_ok());
                    assert!(write_cursor.write_u64::<BigEndian>(0).is_ok());
                    assert!(write_cursor.write_all(&avro_blob[..]).is_ok());

                    let payload: Payload = write_cursor.into_inner().into();

                    let expected = Payload::Valid {
                        header: Header {
                            version: 0,
                            control: 0,
                            id: 0,
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
