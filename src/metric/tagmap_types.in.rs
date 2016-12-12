#[derive(Clone,Debug,PartialEq,Serialize,Deserialize)]
pub struct TagMap<K, V> {
    inner: Vec<(K, V)>,
}
