use crate::constants;
use crate::metric;
use crate::metric::Metadata;
use crate::source::nonblocking::{write_all, BufferedPayload, PayloadErr};
use crate::source::{TCPStreamHandler, TCP};
use crate::util;
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use mio;
use serde_avro;
use std::io::{Cursor, Read};
use std::net;
use std::sync::atomic::{AtomicUsize, Ordering};
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
    // Parses payload headers from raw bytes.
    // All values assumed to be big endian.
    // Diagram below describes packet layout.
    //
    // 0                   1                   2                   3
    // 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
    // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    // |                             Length                            |
    // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    // |                            Version                            |
    // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    // |                            Control                            |
    // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    // |                                                               |
    // +                               ID                              +
    // |                                                               |
    // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    // |                                                               |
    // +                            ShardBy                            +
    // |                                                               |
    // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    // |   #KV Pairs   |   Key Length  |    Key (up to 255 bytes)      |
    // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    // |          Value Length         |   Value (up to 65535 bytes)   |
    // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    // |                          Avro N Bytes                         |
    // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    //
    //
    //    The fields of the payload have the following semantic meaning:
    //
    //    * Length  - Length of full packet
    //    * Version - Version of the wire protocol used.  In this case, 2.
    //    * Control - Metadata governing on the payload is to be published. Version
    //      1 & 2 of the protocol only support a bit indicating whether or not the
    //      client expects an ack after publication.
    //    * ID - Explained in kwargs.
    //    * ShardBy - Explained in kwargs.
    //    * #KV Pairs - Number of KV pairs (max 255)
    //    * Key Length - Length of Key in bytes (max 255)
    //    * Key - N Bytes of Key data encoded as UTF-8 (max 255 bytes after
    //      encoding)
    //    * Value Length - Length of Value data (max 65535)
    //    * Value - N Bytes of Value data, no encoding enforced (max 65535 bytes)

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
    Valid {
        header: Header,
        metadata: Option<Metadata>,
        avro_blob: Vec<u8>,
    },
}

impl From<Vec<u8>> for Payload {
    /// Transforms cursors of payloads into Payload objects.
    fn from(vec: Vec<u8>) -> Payload {
        let mut cursor = Cursor::new(vec);
        let header: Header = (&mut cursor).into();

        let metadata = if header.version < 2 {
            None
        } else {
            let num_kv = cursor.read_u8().unwrap();
            if num_kv < 1 {
                None
            } else {
                let mut m: Metadata = Metadata::default();
                for _ in 0..num_kv {
                    let key_len = cursor.read_u8().unwrap();
                    let mut key = vec![0; key_len as usize];
                    cursor.read_exact(&mut key).unwrap();
                    let val_len = cursor.read_u16::<BigEndian>().unwrap();
                    let mut val = vec![0; val_len as usize];
                    cursor.read_exact(&mut val).unwrap();
                    m.insert(key, val);
                }
                Some(m)
            }
        };

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
            metadata: metadata,
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
                Ok(_num_events) => {
                    for event in events {
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

                                            trace!("Avro payloads processed successfully.");
                                            AVRO_PAYLOAD_SUCCESS_SUM
                                                .fetch_add(1, Ordering::Relaxed);

                                            let maybe_id = handle_res.unwrap();
                                            if let Some(id) = maybe_id {
                                                let mut resp = Cursor::new(Vec::new());
                                                resp.write_u64::<BigEndian>(id)
                                                    .expect(
                                                        "Failed to write response id!",
                                                    );
                                                write_all(&stream, resp.get_ref())
                                                    .expect(
                                                        "Failed to write response!",
                                                    );
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
                    }
                }
            }
        } // while streaming

        // On some systems shutting down an already closed connection (client or
        // otherwise) results in an Err.  See -
        // https://doc.rust-lang.org/beta/std/net/struct.TcpStream.html#platform-specific-behavior
        let _shutdown_result = stream.shutdown(net::Shutdown::Both);
    }
}

impl AvroStreamHandler {
    // Pulls length prefixed (4 bytes, BE), avro encoded
    // binaries off the wire and populates them in the configured
    // channel.
    //
    // Payloads are assumed to be of the following form:
    //
    // 0                   1                   2                   3
    // 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
    // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    // |                             Length                            |
    // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    // |                            Version                            |
    // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    // |                            Control                            |
    // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    // |                                                               |
    // +                               ID                              +
    // |                                                               |
    // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    // |                                                               |
    // +                            ShardBy                            +
    // |                                                               |
    // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    // |   #KV Pairs   |   Key Length  |    Key (up to 255 bytes)      |
    // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    // |          Value Length         |   Value (up to 65535 bytes)   |
    // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    // |                          Avro N Bytes                         |
    // +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
    //
    //
    // While not all sinks for Raw events will make use of OrderBy,
    // it should be the publisher that decides when they are interested
    // in their events being ordered.  As such, the burden of providing
    // this value is on the publishing client.

    fn handle_avro_payload(
        &mut self,
        mut chans: util::Channel,
        payload: Vec<u8>,
        connection_id: Uuid,
    ) -> Result<Option<u64>, PayloadErr> {
        match payload.into() {
            Payload::Valid {
                header,
                metadata,
                avro_blob,
            } => {
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
                        metadata: metadata,
                        connection_id: Some(connection_id),
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
    use std::fs::File;
    use std::io::Write;

    use byteorder::WriteBytesExt;
    use glob;

    use super::*;

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
    fn parse_payload_happy_path_v1() {
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
                    assert!(write_cursor.write_u32::<BigEndian>(1).is_ok());
                    assert!(write_cursor.write_u32::<BigEndian>(0).is_ok());
                    assert!(write_cursor.write_u64::<BigEndian>(0).is_ok());
                    assert!(write_cursor.write_u64::<BigEndian>(0).is_ok());
                    assert!(write_cursor.write_all(&avro_blob[..]).is_ok());

                    let payload: Payload = write_cursor.into_inner().into();

                    let expected = Payload::Valid {
                        header: Header {
                            version: 1,
                            control: 0,
                            id: 0,
                            order_by: 0,
                        },
                        metadata: None,
                        avro_blob: Vec::from(avro_blob),
                    };
                    assert_eq!(payload, expected);
                }
            }
        }
    }

    #[test]
    fn parse_payload_happy_path_v2() {
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

                    // Mocked header data
                    assert!(write_cursor.write_u32::<BigEndian>(2).is_ok());
                    assert!(write_cursor.write_u32::<BigEndian>(0).is_ok());
                    assert!(write_cursor.write_u64::<BigEndian>(0).is_ok());
                    assert!(write_cursor.write_u64::<BigEndian>(0).is_ok());

                    // Mocked kv data
                    // Write number of kv, 1 in this case
                    assert!(write_cursor.write_u8(1).is_ok());

                    // Write key
                    let key = "testkey".as_bytes();
                    assert!(write_cursor.write_u8(key.len() as u8).is_ok());
                    assert!(write_cursor.write_all(&key).is_ok());

                    // Write value
                    let value = "testvalue".as_bytes();
                    assert!(write_cursor
                        .write_u16::<BigEndian>(value.len() as u16)
                        .is_ok());
                    assert!(write_cursor.write_all(&value).is_ok());

                    // Mocked Avro data
                    assert!(write_cursor.write_all(&avro_blob[..]).is_ok());

                    let payload: Payload = write_cursor.into_inner().into();

                    let mut expected_metadata: Metadata = Metadata::default();
                    expected_metadata.insert(key.to_vec(), value.to_vec());

                    let expected = Payload::Valid {
                        header: Header {
                            version: 2,
                            control: 0,
                            id: 0,
                            order_by: 0,
                        },
                        metadata: Some(expected_metadata),
                        avro_blob: Vec::from(avro_blob),
                    };
                    assert_eq!(payload, expected);
                }
            }
        }
    }
}
