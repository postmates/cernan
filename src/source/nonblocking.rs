//! Handy interfaces for nonblocking streams.

use byteorder::{BigEndian, ReadBytesExt};
use mio;
use std::{io, mem};
use std::io::{Read, Write};

/// Like std::net::TcpStream::write_all, except it handles WouldBlock too.
pub fn write_all(mut stream: &mio::net::TcpStream, bytes: &Vec<u8>) -> Result<(), io::Error> {
    let mut written = 0;

    while written < bytes.len() {
        match stream.write(&bytes[written..]) {
            Ok(bytes_written) => {
                written += bytes_written;
            }

            Err(e) => {
                match e.kind() {
                    io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted => {
                        continue;
                    }

                    _ => {
                        error!("Failed to write bytes onto stream! {:?}", e);
                        return Err(e)
                    }
                }
            }
        }
    }
    Ok(())
}

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
    payload_size: Option<usize>,

    /// Position in the payload byte vector receiving.
    payload_pos: usize,

    ///Bytes comprising the payload.
    payload: Vec<u8>,

    /// Inner buffer where bytes from the underlying
    /// stream are staged.
    buffer: io::BufReader<mio::net::TcpStream>,
}

impl BufferedPayload {
    /// Constructs a new BufferedPayload.
    pub fn new(stream: mio::net::TcpStream) -> Self {
        BufferedPayload {
            payload_size: None,
            payload_pos: 0,
            payload: Vec::new(),
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
        if self.payload_size.is_none() {
            self.read_length()?;
        }

        self.read_payload()?;

        // By this point we assert that we have read exactly
        // 1 payload off the buffer.  We may have have read partial
        // or entire other payloads off the wire. Additional bytes
        // will persist in buffer for later parsing.
        Ok(mem::replace(&mut self.payload, Vec::new()))
    }

    /// Reads the payload's length from the wire, caching the result.
    ///
    /// If a cached value already exists, this function noops.
    fn read_length(&mut self) -> Result<(), PayloadErr> {
        if self.payload_size.is_none() {
            self.payload_size = Some(self.buffer.read_u32::<BigEndian>()? as usize);
        };
        Ok(())
    }

    /// Attempts to read at least one payload worth of data.  If there
    /// isn't enough data between the inner buffer and the underlying stream, then
    /// PayloadErr::WouldBlock is returned.
    fn read_payload(&mut self) -> Result<(), PayloadErr> {
        // At this point we can assume that we have successfully
        // read the length off the wire.
        let payload_pos = self.payload_pos;
        let payload_size = self.payload_size.clone().unwrap();

        if self.payload.len() != payload_size {
            trace!("Resizing internal buffer to {:?}", payload_size);
            self.payload.resize(payload_size, 0);
        }

        match self.buffer.read(&mut self.payload[self.payload_pos..payload_size]) {
            Ok(bytes_read) if (payload_pos + bytes_read) == payload_size => {
                // We successfully pulled a payload off the wire.
                // Reset bytes remaining for the next payload.
                self.payload_size = None;
                self.payload_pos = 0;
                Ok(())
            }

            Ok(bytes_read) => {
                // We read some data, but not yet enough.
                // Store the difference and try again later.
                self.payload_pos += bytes_read;
                Err(PayloadErr::WouldBlock)
            }

            Err(e) => {
                Err(e.into())
            }
        }
    }
}
