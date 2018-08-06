use std::sync::{Arc, Mutex};
use std::{thread, time};
use util;
use uuid::Uuid;

#[derive(Clone)]
pub struct SyncAckProps {
    acks_received: usize
}

impl SyncAckProps {
    pub fn new() -> Self {
        SyncAckProps {
            acks_received: 0,
        }
    }

    pub fn ack(&mut self) {
        self.acks_received.saturating_add(1);
    }

    pub fn acks_received(&self) -> usize {
        self.acks_received
    }
}

type SyncAckBagMap = util::HashMap<Uuid, SyncAckProps>;

pub struct SyncAckBag {
    waiting_syncs: Arc<Mutex<SyncAckBagMap>>
}


impl SyncAckBag {
    /// Creates a SyncAckBag
    pub fn new() -> Self {
        SyncAckBag {
            waiting_syncs: Arc::new(Mutex::new(SyncAckBagMap::default()))
        }
    }

    /// Return a clone of the SyncAckProps for the given key
    pub fn props_for(&self, key: Uuid) -> Option<SyncAckProps> {
        let bag = self.waiting_syncs.lock().unwrap();
        match bag.get(&key) {
            Some(v) => Some((*v).clone()),
            None => None
        }
    }

    /// Insert an empty-initialized SyncAckProps for the given key
    pub fn prepare_wait(&self, key: Uuid) {
        let mut bag = self.waiting_syncs.lock().unwrap();
        bag.insert(key, SyncAckProps::new());
    }

    /// Remove the given key from the internal bag
    pub fn remove(&self, key: Uuid) {
        let mut bag = self.waiting_syncs.lock().unwrap();
        bag.remove(&key);
    }

    /// Retrieve a mutable reference to the SyncAckProps for the key and invoke a callback
    /// if it exists. If the key is not present, then the callback is not called.
    pub fn with_props<F: FnOnce(&mut SyncAckProps)>(&self, key: Uuid, f: F) {
        let mut bag = self.waiting_syncs.lock().unwrap();
        match bag.get_mut(&key) {
            Some(v) => Some(f(v)),
            None => None
        };
    }

    /// Wait until the number of acks in the specified key becomes non-zero.
    pub fn wait_for(&self, key: Uuid) {
        let max_nap_time = time::Duration::from_millis(250);
        let mut current_nap_time = time::Duration::from_millis(5);
        let mut wait = true;
        while wait {
            match self.props_for(key) {
                Some(props) => wait = props.acks_received() == 0,
                _ => wait = false
            }
            if wait {
                thread::sleep(current_nap_time);
                current_nap_time = current_nap_time * 2;
                if current_nap_time > max_nap_time {
                    current_nap_time = max_nap_time;
                }
            }
        }
    }
}

lazy_static! {
    static ref SINGLETON: SyncAckBag = SyncAckBag::new();
}

/// Returns the singleton ack bag.
/// The ack bag is necessary because we need to protect concurrent access of raw events' data,
/// but we can't Arc that enum due to serialization needs. Instead, we keep (and Arc) a global bag
/// of properties and only serialize a key into the bag.
pub fn global_ack_bag() -> &'static SyncAckBag {
    &SINGLETON
}
