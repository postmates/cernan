//! Handy interfaces for nonblocking streams.

use byteorder::{BigEndian, ReadBytesExt};
use mio;
use std::io;
use std::io::Read;

/// Handler error types returned by handle_avro_payload.
#[derive(Debug)]
pub enum PayloadErr {
    /// Not enough data present to construct the payload.
    /// Try again later.
    WouldBlock,
    /// An IO error occured.
    IO(io::Error),
    /// Payload parsing failure.
    Protocol(String),
}

impl From<io::Error> for PayloadErr {
    fn from(e: io::Error) -> PayloadErr {
        if e.kind() == io::ErrorKind::WouldBlock {
            PayloadErr::WouldBlock
        } else {
            PayloadErr::IO(e)
        }
    }
}

impl From<String> for PayloadErr {
    fn from(s: String) -> PayloadErr {
        PayloadErr::Protocol(s)
    }
}

/// Buffered length-prefixed payload.
///
/// For use on blocking or non-blocking streams.
pub struct BufferedPayload {
    /// Size of the expected payload in bytes.
    /// When None, this value is read off the underlying
    /// stream as a big-endian u32.
    remaining_bytes: Option<usize>,

    /// Inner buffer where bytes from the underlying
    /// stream are staged.
    buffer: io::BufReader<mio::net::TcpStream>,
}

impl BufferedPayload {
    /// Constructs a new BufferedPayload.
    pub fn new(stream: mio::net::TcpStream) -> Self {
        BufferedPayload {
            remaining_bytes: None,
            buffer: io::BufReader::new(stream),
        }
    }

    /// Reads existing buffer from the underlying data
    /// stream.  If enough data is present, a single payload
    /// is constructed and returned.
    ///
    /// On non-blocking streams, it is up to the user to call
    /// this method repeatedly until PayloadErr::WouldBlock
    /// is returned.
    pub fn read(&mut self) -> Result<Vec<u8>, PayloadErr> {
        // Are we actively reading a payload already?
        if self.remaining_bytes.is_none() {
            self.read_length()?;
        }

        // At this point we can assume that we have successfully
        // read the length off the wire.
        let remaining_bytes = self.remaining_bytes.clone().unwrap();
        let mut buf = vec![0u8; remaining_bytes];
        self.read_payload(&mut buf)?;

        // By this point we assert that we have read exactly
        // 1 payload off the buffer.  We may have have read partial
        // or entire other payloads off the wire. Additional bytes
        // will persist in buffer for later parsing.
        Ok(buf)
    }

    /// Reads the payload's length from the wire, caching the result.
    ///
    /// If a cached value already exists, this function noops.
    fn read_length(&mut self) -> Result<(), PayloadErr> {
        if self.remaining_bytes.is_none() {
            self.remaining_bytes = Some(self.buffer.read_u32::<BigEndian>()? as usize);
        };
        Ok(())
    }

    /// Attempts to read at least one payload worth of data.  If there
    /// isn't enough data between the inner buffer and the underlying stream, then
    /// PayloadErr::WouldBlock is returned.
    fn read_payload(&mut self, mut buf: &mut Vec<u8>) -> Result<(), PayloadErr> {
        match self.buffer.read_exact(&mut buf) {
            Ok(_) => {
                // We successfully pulled a payload off the wire.
                // Reset bytes remaining for the next payload.
                self.remaining_bytes = None;
                Ok(())
            }

            Err(e) => Err(e.into()),
        }
    }
}
