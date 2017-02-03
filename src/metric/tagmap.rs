use std::cmp;
use std::slice::Iter;

#[derive(Clone,Debug,PartialEq,Serialize,Deserialize)]
pub struct TagMap<K, V> {
    inner: Vec<(K, V)>,
}

pub struct TagMapIterator<'a, K, V>
    where V: 'a,
          K: 'a
{
    tagmap: &'a TagMap<K, V>,
    index: usize,
}

impl<'a, K, V> IntoIterator for &'a TagMap<K, V> {
    type Item = (&'a K, &'a V);
    type IntoIter = TagMapIterator<'a, K, V>;

    fn into_iter(self) -> Self::IntoIter {
        TagMapIterator {
            tagmap: self,
            index: 0,
        }
    }
}

impl<'a, K, V> Iterator for TagMapIterator<'a, K, V> {
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
    where K: cmp::Ord
{
    pub fn iter(&self) -> Iter<(K, V)> {
        self.inner.iter()
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        match self.inner.binary_search_by_key(&key, |&(ref k, _)| k) {
            Ok(idx) => Some(&self.inner[idx].1),
            Err(_) => None,
        }
    }

    pub fn remove(&mut self, key: &K) -> Option<V> {
        match self.inner.binary_search_by_key(&key, |&(ref k, _)| k) {
            Ok(idx) => Some(self.inner.remove(idx).1),
            Err(_) => None,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn merge(&mut self, other: &TagMap<K, V>)
        where K: Clone,
              V: Clone
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

    pub fn len(&self) -> usize {
        self.inner.len()
    }
}

impl<K, V> Default for TagMap<K, V> {
    fn default() -> TagMap<K, V> {
        TagMap { inner: Vec::with_capacity(15) }
    }
}

pub fn cmp<K, V>(left: &TagMap<K, V>, right: &TagMap<K, V>) -> Option<cmp::Ordering>
    where K: cmp::Ord,
          V: cmp::Ord
{
    if left.len() != right.len() {
        left.len().partial_cmp(&right.len())
    } else {
        left.inner.partial_cmp(&right.inner)
    }
}
