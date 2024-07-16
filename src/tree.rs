use petgraph::graph::NodeIndex;
use petgraph::prelude::EdgeRef;
use petgraph::stable_graph::StableGraph;
use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::Hash;

#[derive(Clone)]
pub struct Tree<T> {
    graph: StableGraph<T, usize>,
    nodes: HashMap<T, NodeIndex>,
    #[allow(dead_code)]
    root: T,
}

impl<T> Tree<T>
where
    T: Clone + Hash + Eq,
{
    pub fn new(root: T) -> Self {
        let mut graph = StableGraph::new();
        let mut nodes = HashMap::new();
        let item = graph.add_node(root.clone());
        nodes.insert(root.clone(), item);
        Self { graph, nodes, root }
    }

    #[allow(unused)]
    pub fn root(&self) -> &T {
        &self.root
    }

    pub fn add_child<B>(&mut self, parent: &B, child: T)
    where
        T: Borrow<B>,
        B: Hash + Eq + ?Sized,
    {
        if !self.nodes.contains_key(child.borrow()) {
            let item = self.graph.add_node(child.clone());
            self.nodes.insert(child, item);
            self.graph.add_edge(self.nodes[parent.borrow()], item, 0);
        }
    }

    pub fn add_children<B>(&mut self, parent: &B, children: Vec<T>)
    where
        T: Borrow<B>,
        B: Hash + Eq + ?Sized,
    {
        for child in children {
            self.add_child(parent.borrow(), child);
        }
    }

    pub fn remove_child<B>(&mut self, child: &B) -> Option<T>
    where
        T: Borrow<B>,
        B: Hash + Eq + ?Sized,
    {
        if let Some(node) = self.nodes.remove(child.borrow()) {
            return self.graph.remove_node(node);
        }
        None
    }

    pub fn children<B>(&self, parent: &B) -> Vec<&B>
    where
        T: Borrow<B>,
        B: Hash + Eq + ?Sized,
    {
        if let Some(node) = self.nodes.get(parent) {
            let mut children = Vec::new();
            for edge in self.graph.edges(*node) {
                let child = self.graph[edge.target()].borrow();
                children.push(child);
            }
            children
        } else {
            Vec::new()
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tree() {
        let root = "/".to_string();
        let mut tree = Tree::new(root);
        tree.add_child("/", "/a".to_string());
        let len = 100;
        for i in 0..len {
            tree.add_child("/a", format!("/a/{}", i));
            assert_eq!(tree.children("/a").len(), i + 1)
        }
        for i in 0..len {
            let res = tree.remove_child(&format!("/a/{}", i));
            assert!(res.is_some());
            assert_eq!(tree.children("/a").len(), len - 1 - i)
        }

        for i in 0..len {
            tree.add_child("/a", format!("/a/{}", i));
            assert_eq!(tree.children("/a").len(), i + 1)
        }
        for i in (0..len).rev() {
            let res = tree.remove_child(&format!("/a/{}", i));
            assert!(res.is_some());
            assert_eq!(tree.children("/a").len(), i)
        }

        tree.add_child("/", "/b".to_string());
        tree.add_child("/", "/c".to_string());
        tree.add_child("/a", "/a/d".to_string());
        assert_eq!(tree.children("/a"), vec!["/a/d".to_string()]);
        assert_eq!(tree.children("/e"), Vec::<String>::new());
        tree.remove_child("/c");
        assert_eq!(tree.children("/"), vec!["/b".to_string(), "/a".to_string()]);
        tree.add_child("/", "/c".to_string());
        // let _removed = tree.remove_children("/");
        // assert!(tree.children("/").is_empty());
        // tree.add_child("/", "/a".to_string());
        // assert!(tree.children("/a").is_empty());
    }
}
