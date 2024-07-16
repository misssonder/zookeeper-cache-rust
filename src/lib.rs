use futures::Stream;
use pin_project::pin_project;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use thiserror::Error;

mod cache;
mod tree;

pub use cache::{Cache, CacheBuilder};

pub type Result<T> = std::result::Result<T, Error>;
pub type SharedChildData = Arc<ChildData>;

#[derive(Error, Debug, PartialEq)]
pub enum Error {
    #[error("zk error :{0}")]
    ZK(#[from] zookeeper_client::Error),
}

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
pub(crate) struct EventStream<T> {
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
