//! tagmap is the map of key, value pairs that sits inside each non-flush
//! `metric::Event`. Think of it as a specialized hashmap. The purpose of event
//! associated metadata is to distinguish identially named events that come from
//! other sources or have been otherwise manipulated by the user in a filter.

use std::cmp;
use std::hash::{Hash, Hasher};
use std::slice::Iter;

/// The tagmap key, value collection. Behaves similarly to
/// `std::collections::HashMap` but with a specialize implementation for fast
/// searching over a small collection.
#[derive(Clone, Debug, Eq, Serialize, Deserialize)]
pub struct TagMap<K, V>
where
    K: Hash,
    V: Hash,
{
    inner: Vec<(K, V)>,
}

impl<K, V> Hash for TagMap<K, V>
where
    K: Hash + cmp::Ord,
    V: Hash + cmp::Ord,
{
    fn hash<H: Hasher>(&self, state: &mut H) {
        for &(ref k, ref v) in self.iter() {
            k.hash(state);
            v.hash(state);
        }
    }
}

impl<K, V> PartialEq for TagMap<K, V>
where
    K: Hash + PartialEq,
    V: Hash + PartialEq,
{
    fn eq(&self, other: &TagMap<K, V>) -> bool {
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
pub struct TagMapIterator<'a, K, V>
where
    V: 'a + Hash,
    K: 'a + Hash,
{
    tagmap: &'a TagMap<K, V>,
    index: usize,
}

impl<'a, K, V> IntoIterator for &'a TagMap<K, V>
where
    K: Hash,
    V: Hash,
{
    type Item = (&'a K, &'a V);
    type IntoIter = TagMapIterator<'a, K, V>;

    fn into_iter(self) -> Self::IntoIter {
        TagMapIterator {
            tagmap: self,
            index: 0,
        }
    }
}

impl<'a, K, V> Iterator for TagMapIterator<'a, K, V>
where
    K: Hash,
    V: Hash,
{
    type Item = (&'a K, &'a V);
    fn next(&mut self) -> Option<(&'a K, &'a V)> {
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

impl<K, V> TagMap<K, V>
where
    K: cmp::Ord + Hash,
    V: Hash,
{
    /// Create a `tagmap::Iter`.
    pub fn iter(&self) -> Iter<(K, V)> {
        self.inner.iter()
    }

    /// Get a value from the tagmap, if it exists.
    pub fn get(&self, key: &K) -> Option<&V> {
        match self.inner.binary_search_by_key(&key, |&(ref k, _)| k) {
            Ok(idx) => Some(&self.inner[idx].1),
            Err(_) => None,
        }
    }

    /// Remove a value from the tagmap. The value will be returned if it
    /// existed.
    pub fn remove(&mut self, key: &K) -> Option<V> {
        match self.inner.binary_search_by_key(&key, |&(ref k, _)| k) {
            Ok(idx) => Some(self.inner.remove(idx).1),
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
    pub fn merge(&mut self, other: &TagMap<K, V>)
    where
        K: Clone,
        V: Clone,
    {
        for &(ref key, ref val) in &other.inner {
            match self.inner.binary_search_by(|probe| probe.0.cmp(key)) {
                Ok(_) => {}
                Err(idx) => {
                    self.inner.insert(idx, (key.clone(), val.clone()));
                }
            }
        }
    }

    /// Insert a key / value into self
    ///
    /// This method will return the value previously stored under the given key,
    /// if there was such a value.
    pub fn insert(&mut self, key: K, val: V) -> Option<V> {
        match self.inner.binary_search_by(|probe| probe.0.cmp(&key)) {
            Ok(idx) => {
                self.inner.push((key, val));
                let old = self.inner.swap_remove(idx);
                Some(old.1)
            }
            Err(idx) => {
                self.inner.insert(idx, (key, val));
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

impl<K, V> Default for TagMap<K, V>
where
    K: Hash,
    V: Hash,
{
    fn default() -> TagMap<K, V> {
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
pub fn cmp<K, V>(left: &TagMap<K, V>, right: &TagMap<K, V>) -> Option<cmp::Ordering>
where
    K: cmp::Ord + Hash,
    V: cmp::Ord + Hash,
{
    if left.len() != right.len() {
        left.len().partial_cmp(&right.len())
    } else {
        left.inner.partial_cmp(&right.inner)
    }
}
