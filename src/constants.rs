//! Library level constants
use mio;

/// MIO token used to distinguish system events
/// from other event sources.
///
/// Note - It is assumed that sources will not hold
/// more than 2048 addressable streams, 0 indexed.
pub const SYSTEM: mio::Token = mio::Token(2048);
