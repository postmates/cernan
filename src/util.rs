//! Utility module, a grab-bag of functionality
use constants;
use hopper;
use metric;
use mio;
use seahash::SeaHasher;
use slab;
use std::collections;
use std::hash;
use std::ops::{Index, IndexMut};

/// Cernan hashmap
///
/// In most cases where cernan needs a hashmap we've got smallish inputs as keys
/// and, more, have a smallish number of total elements (< 100k) to store in the
/// map. This hashmap is specialized to address that common use-case.
pub type HashMap<K, V> =
    collections::HashMap<K, V, hash::BuildHasherDefault<SeaHasher>>;

/// A vector of `hopper::Sender`s.
pub type Channel = Vec<hopper::Sender<metric::Event>>;

/// Send a `metric::Event` into a `Channel`.
pub fn send(chans: &mut Channel, event: metric::Event) {
    if chans.is_empty() {
        // Nothing to send to.
        return;
    }

    for chan in &mut chans[..] {
        let mut snd_event = event.clone();
        loop {
            if let Err(res) = chan.send(snd_event) {
                // The are a variety of errors that hopper will signal back up
                // when we do a send. The only one we care about is
                // `Error::Full`, meaning that all disk and memory buffer space
                // is consumed. We drop the event on the floor in that case.
                match res.1 {
                    hopper::Error::Full => {
                        break;
                    }
                    _ => {
                        snd_event = res.0;
                        continue;
                    }
                }
            } else {
                break;
            }
        }
    }
}

/// Determine the state of a buffering queue, whether open or closed.
///
/// Cernan is architected to be a push-based system. It copes with demand rushes
/// by buffering to disk -- via the hopper queues -- and rejecting memory-based
/// storage with overload signals. This signal, in particular, limits the amount
/// of information delivered to a filter / sink by declaring that said filter /
/// sink's input 'valve' is closed. Exactly how and why a filter / sink declares
/// its valve state is left to the implementation.
#[derive(Debug, PartialEq)]
pub enum Valve {
    /// In the `Open` state a filter / sink will accept new inputs
    Open,
    /// In the `Closed` state a filter / sink will reject new inputs, backing
    /// them up in the communication queue.
    Closed,
}

#[inline]
fn token_to_idx(token: &mio::Token) -> usize {
    match *token {
        mio::Token(idx) => idx,
    }
}

/// Wrapper around Slab
pub struct TokenSlab<E: mio::Evented> {
    tokens: slab::Slab<E>,
}

impl<E: mio::Evented> Default for TokenSlab<E> {
    fn default() -> Self {
        Self::new()
    }
}

impl<E: mio::Evented> Index<mio::Token> for TokenSlab<E> {
    type Output = E;

    /// Returns Evented object corresponding to Token.
    fn index(&self, token: mio::Token) -> &E {
        &self.tokens[token_to_idx(&token)]
    }
}

impl<E: mio::Evented> IndexMut<mio::Token> for TokenSlab<E> {
    fn index_mut(&mut self, token: mio::Token) -> &mut E {
        &mut self.tokens[token_to_idx(&token)]
    }
}

/// Interface wrapping a subset of Slab such
/// that we can magically translate indices to
/// `mio::token`.
impl<E: mio::Evented> TokenSlab<E> {
    /// Constructs a new TokenSlab with a capacity derived from the value
    /// of constants::SYSTEM.
    pub fn new() -> TokenSlab<E> {
        TokenSlab {
            tokens: slab::Slab::with_capacity(token_to_idx(&constants::SYSTEM)),
        }
    }

    /// Iterates over the underlying slab mapping index to mio::Evented.
    pub fn iter(&self) -> slab::Iter<E> {
        self.tokens.iter()
    }

    /// Inserts a new Evented into the slab, returning a mio::Token
    /// corresponding to the index of the newly inserted type.
    pub fn insert(&mut self, thing: E) -> mio::Token {
        let idx = self.tokens.insert(thing);
        mio::Token::from(idx)
    }
}
