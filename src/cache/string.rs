//! Cache for strings
//!
//! This implementation is pretty bare-bones. We don't do any fancy tricks with
//! automatic deref or the like. Clients are responsible for maintaining the ID
//! we hand out from `store` and giving it back to us for lookups.

use seahash::SeaHasher;
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::hash::BuildHasherDefault;
use std::sync::{Arc, RwLock};

type HashMapFnv<K, V> = HashMap<K, V, BuildHasherDefault<SeaHasher>>;

lazy_static! {
    static ref STRING: Arc<RwLock<HashMapFnv<u64, Arc<String>>>> = Arc::new(RwLock::default());
}

/// Store a str into the string cache
///
/// A new String will be allocated from the str if the map does not already
/// contain str. Otherwise, this operation pays the cost of a lookup and not
/// much more.
pub fn store(value: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    let id = hasher.finish();

    let mut w = STRING.write().unwrap();
    w.entry(id).or_insert_with(|| Arc::new(value.to_string()));
    id
}

/// Get a str from the string cache
///
/// If no such ID exists in the cache None will be returned
#[inline]
pub fn get(id: u64) -> Option<Arc<String>> {
    let r = STRING.read().unwrap();
    r.get(&id).map(|x| (*x).clone())
}
