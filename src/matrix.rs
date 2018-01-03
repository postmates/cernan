//! Collection of matrix implementations.

use std;
use std::fmt::Debug;
use std::str::FromStr;
use util;

type AdjacencyMap<T> = util::HashMap<String, Option<T>>;

type AdjacencyMatrix<T> = util::HashMap<String, AdjacencyMap<T>>;

/// Adjacency matrix struct.
pub struct Adjacency<M: Clone> {
    edges: AdjacencyMatrix<M>,
}

impl<M: Clone + Debug> Default for Adjacency<M> {
    fn default() -> Adjacency<M> {
        Adjacency {
            edges: Default::default(),
        }
    }
}

///  Poor man's adjacency matrix biased towards incident edge queries.
///
///  Edges are not symmetric.  Two values are symmetrically adjacent when
///  edges originate from each value to the other value.
impl<M: Clone + Debug> Adjacency<M> {
    /// Construct a new adjacency matrix.
    pub fn new() -> Self {
        Adjacency {
            edges: Default::default(),
        }
    }

    /// Adds an outbound edge from a node to another.
    pub fn add_asymmetric_edge(
        &mut self,
        from_str: &str,
        to_str: &str,
        metadata: Option<M>,
    ) {
        let to = String::from_str(to_str).unwrap();
        let from = String::from_str(from_str).unwrap();
        let vec = self.edges.entry(from).or_insert_with(Default::default);
        vec.insert(to, metadata);
    }

    /// Adds symmetric edges between the given node and a set of other nodes.
    pub fn add_edges(
        &mut self,
        from_str: &str,
        to_strs: Vec<String>,
        metadata: Option<M>,
    ) {
        for to_str in to_strs {
            self.add_asymmetric_edge(from_str, &to_str, metadata.clone());
            self.add_asymmetric_edge(&to_str, from_str, metadata.clone())
        }

        drop(metadata);
    }

    /// Returns the number of incident edges to the given node.
    pub fn num_edges(&mut self, id: &str) -> usize {
        match self.edges.get(id) {
            Some(value) => value.keys().len(),

            None => 0,
        }
    }

    /// Returns true iff relations exist for the given node id.
    pub fn contains_node(&self, id: &str) -> bool {
        self.edges.contains_key(id)
    }

    /// Filters and returns edges satisfying the given constraint.
    pub fn filter_nodes<F>(&self, id: &str, f: F) -> Vec<String>
    where
        for<'r> F: FnMut(&'r (&String, &Option<M>)) -> bool,
    {
        self.edges[id]
            .iter()
            .filter(f)
            .map(|(k, _v)| k.clone())
            .collect()
    }

    /// Iterates over edge relations in the matrix.
    pub fn iter(&self) -> std::collections::hash_map::Iter<String, AdjacencyMap<M>> {
        self.edges.iter()
    }

    /// Pops adjacency metadata for the given node.
    pub fn pop(&mut self, id: &str) -> Option<AdjacencyMap<M>> {
        self.edges.remove(id)
    }

    /// As pop, but returns a vec of node identifiers connected to the given
    /// node.
    pub fn pop_nodes(&mut self, id: &str) -> Vec<String> {
        match self.pop(id) {
            Some(map) => map.into_iter().map(|(k, _v)| k).collect(),

            None => Vec::new(),
        }
    }

    /// As pop, but returns a vec of edge metadata.
    /// Option values will be unwrapped and None values filtered.
    pub fn pop_metadata(&mut self, id: &str) -> Vec<M> {
        match self.pop(id) {
            Some(map) => map.into_iter()
                .filter(|&(ref _k, ref option_v)| option_v.is_some())
                .map(|(_, some_v)| some_v.unwrap())
                .collect(),

            None => Vec::new(),
        }
    }
}
