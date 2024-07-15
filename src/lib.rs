use std::collections::HashMap;
use futures::Stream;
use pin_project::pin_project;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use crate::tree::Tree;

mod tree;

pub type SharedChildData = Arc<ChildData>;
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct ChildData {
    pub path: String,
    pub data: Vec<u8>,
    pub stat: zookeeper_client::Stat,
}

#[derive(Debug, Clone)]
pub enum Event {
    Add(SharedChildData),
    Delete(SharedChildData),
    Update {
        old: SharedChildData,
        new: SharedChildData,
    },
}

#[pin_project]
pub struct EventStream<T> {
    #[pin]
    pub(crate) watcher: tokio::sync::mpsc::UnboundedReceiver<T>,
}

impl<T> Stream for EventStream<T> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        this.watcher.as_mut().poll_recv(cx)
    }
}

type Path = Arc<String>;
struct Storage {
    data: HashMap<Path, ChildData>,
    tree: Tree<Path>,
}

impl Storage {
    pub fn new(root: String) -> Storage {
        let root = Arc::new(root);
        Storage {
            data: HashMap::new(),
            tree: Tree::new(root),
        }
    }
    pub fn replace(&mut self, data: HashMap<Path, ChildData>, tree: Tree<Path>) {
        self.data = data;
        self.tree = tree;
    }
}
