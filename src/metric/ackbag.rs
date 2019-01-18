use crate::util;
use std::cmp::min;
use std::sync::{Arc, Mutex};
use std::{thread, time};
use uuid::Uuid;

#[derive(Clone)]
pub struct SyncAckProps {
    acks_received: usize,
}

impl Default for SyncAckProps {
    fn default() -> SyncAckProps {
        SyncAckProps { acks_received: 0 }
    }
}

impl SyncAckProps {
    pub fn ack(&mut self) {
        self.acks_received = self.acks_received.saturating_add(1);
    }

    pub fn acks_received(&self) -> usize {
        self.acks_received
    }
}

type SyncAckBagMap = util::HashMap<Uuid, SyncAckProps>;

pub struct SyncAckBag {
    waiting_syncs: Arc<Mutex<SyncAckBagMap>>,
}

impl Default for SyncAckBag {
    fn default() -> SyncAckBag {
        SyncAckBag {
            waiting_syncs: Arc::new(Mutex::new(SyncAckBagMap::default())),
        }
    }
}

impl SyncAckBag {
    /// Return a clone of the SyncAckProps for the given key
    pub fn props_for(&self, key: Uuid) -> Option<SyncAckProps> {
        let bag = self.waiting_syncs.lock().unwrap();
        match bag.get(&key) {
            Some(v) => Some((*v).clone()),
            None => None,
        }
    }

    /// Insert an empty-initialized SyncAckProps for the given key
    pub fn prepare_wait(&self, key: Uuid) {
        let mut bag = self.waiting_syncs.lock().unwrap();
        bag.insert(key, SyncAckProps::default());
    }

    /// Remove the given key from the internal bag
    pub fn remove(&self, key: Uuid) {
        let mut bag = self.waiting_syncs.lock().unwrap();
        bag.remove(&key);
    }

    /// Retrieve a mutable reference to the SyncAckProps for the key and invoke
    /// a callback if it exists. If the key is not present, then the
    /// callback is not called.
    pub fn with_props<F: FnOnce(&mut SyncAckProps)>(&self, key: Uuid, f: F) {
        let mut bag = self.waiting_syncs.lock().unwrap();
        match bag.get_mut(&key) {
            Some(v) => Some(f(v)),
            None => None,
        };
    }

    /// Wait until the number of acks in the specified key becomes non-zero.
    pub fn wait_for(&self, key: Uuid) {
        self.wait_for_callback(key, thread::sleep);
    }

    /// Testability driver for wait_for
    pub fn wait_for_callback<F: Fn(time::Duration)>(&self, key: Uuid, cb: F) {
        let max_nap_time = time::Duration::from_millis(250);
        let mut current_nap_time = time::Duration::from_millis(5);
        let mut wait = true;
        while wait {
            match self.props_for(key) {
                Some(props) => wait = props.acks_received() == 0,
                _ => wait = false,
            }
            if wait {
                cb(current_nap_time);
                current_nap_time = min(max_nap_time, current_nap_time * 2);
            }
        }
    }
}

lazy_static! {
    static ref SINGLETON: SyncAckBag = SyncAckBag::default();
}

/// Returns the singleton ack bag.
/// The ack bag is necessary because we need to protect concurrent access of raw
/// events' data, but we can't Arc that enum due to serialization needs.
/// Instead, we keep (and Arc) a global bag of properties and only serialize a
/// key into the bag.
pub fn global_ack_bag() -> &'static SyncAckBag {
    &SINGLETON
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;

    #[test]
    fn test_prepare_wait_adds_default_entry() {
        let ack_bag = SyncAckBag::default();
        let key = Uuid::new_v4();
        ack_bag.prepare_wait(key);

        let value = ack_bag.props_for(key).unwrap();
        assert_eq!(value.acks_received(), 0);
    }

    #[test]
    fn test_remove_works() {
        let ack_bag = SyncAckBag::default();
        let key = Uuid::new_v4();
        ack_bag.prepare_wait(key);
        ack_bag.remove(key);

        assert_eq!(ack_bag.props_for(key).is_some(), false);

        // Removing a non-existent key is fine.
        let other_key = Uuid::new_v4();
        ack_bag.remove(other_key);
    }

    #[test]
    fn test_ack_adds_one_to_tally() {
        let ack_bag = SyncAckBag::default();
        let key = Uuid::new_v4();
        ack_bag.prepare_wait(key);
        ack_bag.with_props(key, |props| {
            props.ack();
        });

        assert_eq!(ack_bag.props_for(key).unwrap().acks_received(), 1);
    }

    #[test]
    fn test_wait_for_returns_when_there_are_acks() {
        let ack_bag = SyncAckBag::default();
        let key = Uuid::new_v4();
        ack_bag.prepare_wait(key);
        ack_bag.with_props(key, |props| {
            props.ack();
        });

        ack_bag.wait_for(key);
    }

    #[test]
    fn test_wait_for_sleeps_when_there_are_no_acks() {
        let callback_count = RefCell::new(0);
        let ack_bag = SyncAckBag::default();
        let key = Uuid::new_v4();
        ack_bag.prepare_wait(key);
        ack_bag.wait_for_callback(key, |_| {
            *callback_count.borrow_mut() += 1;
            ack_bag.with_props(key, |props| {
                props.ack();
            })
        });
        assert_eq!(*callback_count.borrow(), 1);
    }

    #[test]
    fn test_waiting_for_nonexistent_key_returns() {
        let ack_bag = SyncAckBag::default();
        ack_bag.wait_for(Uuid::new_v4());
    }
}
