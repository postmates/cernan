//! tagmap is the map of key, value pairs that sits inside each non-flush
//! `metric::Event`. Think of it as a specialized hashmap. The purpose of event
//! associated metadata is to distinguish identially named events that come from
//! other sources or have been otherwise manipulated by the user in a filter.

use cache::string::store;
use std::cmp;
use std::hash::{Hash, Hasher};
use std::slice::Iter;

/// The tagmap key, value collection. Behaves similarly to
/// `std::collections::HashMap` but with a specialize implementation for fast
/// searching over a small collection.
#[derive(Clone, Debug, Eq, Serialize, Deserialize)]
pub struct TagMap {
    inner: Vec<(u64, u64)>,
}

impl Hash for TagMap {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for &(ref k, ref v) in self.iter() {
            k.hash(state);
            v.hash(state);
        }
    }
}

impl PartialEq for TagMap {
    fn eq(&self, other: &TagMap) -> bool {
        if self.inner.len() != other.inner.len() {
            false
        } else {
            for (&(ref k, ref v), &(ref o_k, ref o_v)) in
                self.inner.iter().zip(other.inner.iter())
            {
                if (k != o_k) || (v != o_v) {
                    return false;
                }
            }
            true
        }
    }
}

/// Iteration over a `TagMap`. Behaves as you'd expect a key, value map to
/// behave.
pub struct TagMapIterator<'a> {
    tagmap: &'a TagMap,
    index: usize,
}

impl<'a> IntoIterator for &'a TagMap {
    type Item = (&'a u64, &'a u64);
    type IntoIter = TagMapIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        TagMapIterator {
            tagmap: self,
            index: 0,
        }
    }
}

impl<'a> Iterator for TagMapIterator<'a> {
    type Item = (&'a u64, &'a u64);
    fn next(&mut self) -> Option<(&'a u64, &'a u64)> {
        let result = if self.index < self.tagmap.inner.len() {
            let (ref k, ref v) = self.tagmap.inner[self.index];
            Some((k, v))
        } else {
            None
        };
        self.index += 1;
        result
    }
}

impl TagMap {
    /// Create a `tagmap::Iter`.
    pub fn iter(&self) -> Iter<(u64, u64)> {
        self.inner.iter()
    }

    /// Get a value from the tagmap, if it exists.
    pub fn get(&self, key: &u64) -> Option<&u64> {
        match self.inner.binary_search_by_key(&key, |&(ref k, _)| k) {
            Ok(idx) => Some(&self.inner[idx].1),
            Err(_) => None,
        }
    }

    /// Remove a value from the tagmap. The value will be returned if it
    /// existed.
    pub fn remove(&mut self, key: &u64) -> Option<u64> {
        match self.inner.binary_search_by_key(&key, |&(ref k, _)| k) {
            Ok(idx) => {
                let id = self.inner.remove(idx).1;
                Some(id)
            }
            Err(_) => None,
        }
    }

    /// Determine if the tagmap is empty.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Merge two tagmaps
    ///
    /// This method will overwrite values in the map with values from
    /// `other`. If the key / value in `other` does not exist in self it will be
    /// created in self.
    pub fn merge(&mut self, other: &TagMap) {
        for &(ref key, ref val) in &other.inner {
            match self.inner.binary_search_by(|probe| probe.0.cmp(key)) {
                Ok(_) => {}
                Err(idx) => {
                    self.inner.insert(idx, (*key, *val));
                }
            }
        }
    }

    /// Insert a key / value into self
    ///
    /// This method will return the value previously stored under the given key,
    /// if there was such a value.
    pub fn insert(&mut self, key: &str, val: &str) -> Option<u64> {
        let hsh_k = store(key);
        let hsh_v = store(val);
        match self.inner.binary_search_by(|probe| probe.0.cmp(&hsh_k)) {
            Ok(idx) => {
                self.inner.push((hsh_k, hsh_v));
                let old = self.inner.swap_remove(idx);
                Some(old.1)
            }
            Err(idx) => {
                self.inner.insert(idx, (hsh_k, hsh_v));
                None
            }
        }
    }

    /// Return the length of the tagmap. This is the total number of key /
    /// values stored in the map.
    pub fn len(&self) -> usize {
        self.inner.len()
    }
}

impl Default for TagMap {
    fn default() -> TagMap {
        TagMap {
            inner: Vec::with_capacity(15),
        }
    }
}

/// Compare two tagmaps
///
/// This function is used to define an ordering over `TagMap`. This is done by
/// comparing the key / value pairs of each map. The exist method of comparison
/// is undefined.
pub fn cmp(left: &TagMap, right: &TagMap) -> Option<cmp::Ordering> {
    if left.len() != right.len() {
        left.len().partial_cmp(&right.len())
    } else {
        left.inner.partial_cmp(&right.inner)
    }
}
