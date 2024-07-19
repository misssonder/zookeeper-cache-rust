use crate::tree::Tree;
use crate::{ChildData, Event};
use crate::{EventStream, Result, SharedChildData};
use async_recursion::async_recursion;
use futures::StreamExt;
use futures::{stream, Stream};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use tokio_util::sync::CancellationToken;
use zookeeper_client::{EventType, WatchedEvent};

type Path = String;
struct Storage {
    data: HashMap<Path, SharedChildData>,
    tree: Tree<Path>,
}

impl Storage {
    pub fn new(root: String) -> Storage {
        Storage {
            data: HashMap::new(),
            tree: Tree::new(root),
        }
    }

    #[allow(dead_code)]
    pub fn replace(&mut self, data: HashMap<Path, SharedChildData>, tree: Tree<Path>) {
        self.data = data;
        self.tree = tree;
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Version(u32, u32, u32);

#[derive(Clone, Debug)]
pub struct AuthPacket {
    pub scheme: String,
    pub auth: Vec<u8>,
}

/// CacheBuilder is the configuration of Cache
#[derive(Clone, Debug)]
pub struct CacheBuilder {
    /// The root path which be watched
    path: String,
    /// The authes of Zookeeper
    authes: Vec<AuthPacket>,
    /// The version of Zookeeper server
    server_version: Version,
    /// Session timeout
    session_timeout: Duration,
    /// Connect timeout
    connection_timeout: Duration,
    /// When got session expired, we will try to  reconnect of reconnect timeout
    reconnect_timeout: Duration,
}

impl Default for CacheBuilder {
    fn default() -> Self {
        Self {
            path: "/".to_string(),
            authes: vec![],
            server_version: Version(u32::MAX, u32::MAX, u32::MAX),
            session_timeout: Duration::ZERO,
            connection_timeout: Duration::ZERO,
            reconnect_timeout: Duration::from_secs(1),
        }
    }
}

impl From<&CacheBuilder> for zookeeper_client::Connector {
    fn from(val: &CacheBuilder) -> Self {
        let mut connector = zookeeper_client::Client::connector();
        connector.server_version(
            val.server_version.0,
            val.server_version.1,
            val.server_version.2,
        );
        for auth in val.authes.clone() {
            connector.auth(auth.scheme, auth.auth);
        }
        connector.session_timeout(val.session_timeout);
        connector.connection_timeout(val.connection_timeout);
        connector.readonly(true);
        connector
    }
}

impl CacheBuilder {
    pub fn new(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            ..Default::default()
        }
    }

    pub fn with_auth(mut self, scheme: String, auth: Vec<u8>) -> Self {
        self.authes.push(AuthPacket { scheme, auth });
        self
    }

    pub fn with_version(mut self, major: u32, minor: u32, patch: u32) -> Self {
        self.server_version = Version(major, minor, patch);
        self
    }

    pub fn with_session_timeout(mut self, timeout: Duration) -> Self {
        self.session_timeout = timeout;
        self
    }

    pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connection_timeout = timeout;
        self
    }

    pub fn with_reconnect_timeout(mut self, timeout: Duration) -> Self {
        self.reconnect_timeout = timeout;
        self
    }

    pub async fn build(
        self,
        addr: impl Into<String>,
    ) -> Result<(Cache, impl Stream<Item = Event>)> {
        Cache::new(addr, self).await
    }
}

/// Cache will watch root node and it's children nodes recursively
///```no_run
/// use futures::StreamExt;
/// use zookeeper_cache_rust::CacheBuilder;
/// async fn dox() -> zookeeper_cache_rust::Result<()>{
/// let (cache,mut stream) = CacheBuilder::default().build("localhost:2181").await?;
///    tokio::spawn(async move{
///         while let Some(_event) = stream.next().await{
///             // handle event
///         }
///    });
///    cache.get("/test").await;
///    Ok(())
/// }

pub struct Cache {
    addr: String,
    builder: CacheBuilder,
    storage: Arc<RwLock<Storage>>,
    event_sender: tokio::sync::mpsc::UnboundedSender<Event>,
    token: CancellationToken,
}

impl Drop for Cache {
    fn drop(&mut self) {
        self.token.cancel();
    }
}

impl Cache {
    pub async fn new(
        addr: impl Into<String>,
        builder: CacheBuilder,
    ) -> Result<(Self, impl Stream<Item = Event>)> {
        let mut connector: zookeeper_client::Connector = (&builder).into();
        let addr = addr.into();
        let client = connector.connect(&addr).await?;
        let storage = Arc::new(RwLock::new(Storage::new(builder.path.clone())));
        let (sender, watcher) = tokio::sync::mpsc::unbounded_channel();
        let events = EventStream { watcher };
        let cache = Self {
            addr,
            builder,
            storage,
            event_sender: sender,
            token: CancellationToken::new(),
        };
        let (sender, watcher) = tokio::sync::mpsc::unbounded_channel();
        cache.init_nodes(&client, &sender).await?;
        cache.watch(client, sender, watcher).await;
        Ok((cache, events))
    }

    /// Get data and stat through path
    ///```no_run
    /// use zookeeper_cache_rust::CacheBuilder;
    /// async fn dox()->zookeeper_cache_rust::Result<()>{
    ///    let (cache, _stream) = CacheBuilder::default().build("localhost:2181").await?;
    ///    cache.get("/test").await;
    ///    Ok(())
    /// }
    /// ```
    pub async fn get(&self, path: &str) -> Option<SharedChildData> {
        self.storage.read().await.data.get(path).cloned()
    }

    async fn init_nodes(
        &self,
        client: &zookeeper_client::Client,
        sender: &tokio::sync::mpsc::UnboundedSender<WatchedEvent>,
    ) -> Result<()> {
        let new = Arc::new(RwLock::new(Storage::new(self.builder.path.clone())));
        Self::fetch_all(
            client,
            self.builder.path.as_str(),
            &mut new.write().await,
            sender,
            true,
        )
        .await?;
        // send events of existed node
        let old = self.storage.read().await;
        let new = new.read().await;
        Self::compare_storage(self.builder.path.as_ref(), &old, &new, &self.event_sender).await;
        drop(old);
        let mut old = self.storage.write().await;
        old.replace(new.data.clone(), new.tree.clone());
        Ok(())
    }

    async fn watch(
        &self,
        client: zookeeper_client::Client,
        sender: tokio::sync::mpsc::UnboundedSender<WatchedEvent>,
        mut watcher: tokio::sync::mpsc::UnboundedReceiver<WatchedEvent>,
    ) {
        let storage = self.storage.clone();
        let sender = sender.clone();
        let builder = self.builder.clone();
        let event_sender = self.event_sender.clone();
        let token = self.token.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = token.cancelled() => {
                        return
                    }
                    event = watcher.recv() => {
                        match event{
                            Some(event) => {
                                Self::handle_event(&builder.path, &client, &storage, event, &sender, &event_sender).await;
                            }
                            None => break
                        }
                    }
                }
            }
        });
    }

    #[allow(clippy::too_many_arguments)]
    async fn handle_event(
        _path: &str,
        client: &zookeeper_client::Client,
        storage: &Arc<RwLock<Storage>>,
        event: WatchedEvent,
        sender: &tokio::sync::mpsc::UnboundedSender<WatchedEvent>,
        event_sender: &tokio::sync::mpsc::UnboundedSender<Event>,
    ) {
        match event.event_type {
            EventType::Session => {
                // todo
                // handle session changed
            }
            EventType::NodeDeleted => Self::handle_node_deleted(storage, event, event_sender).await,
            EventType::NodeDataChanged => {
                Self::handle_node_data_changed(client, storage, event, sender, event_sender).await
            }
            EventType::NodeChildrenChanged => {
                Self::handle_children_change(client, storage, event, sender, event_sender).await
            }
            EventType::NodeCreated => {
                Self::handle_node_created(client, storage, event, sender, event_sender).await
            }
        }
    }

    /// only used when root node be created
    async fn handle_node_created(
        client: &zookeeper_client::Client,
        storage: &Arc<RwLock<Storage>>,
        event: WatchedEvent,
        sender: &tokio::sync::mpsc::UnboundedSender<WatchedEvent>,
        event_sender: &tokio::sync::mpsc::UnboundedSender<Event>,
    ) {
        let mut storage = storage.write().await;
        if let Ok(status) = Self::get_root_node(client, &event.path, &mut storage, sender).await {
            match status {
                RootStatus::Ephemeral(data) => {
                    let _ = event_sender.send(Event::Add(data));
                }
                RootStatus::Persistent(data) => {
                    if let Err(err) = Self::list_children(client, &event.path, sender).await {
                        debug_assert_eq!(err, zookeeper_client::Error::NoNode);
                    }
                    let _ = event_sender.send(Event::Add(data));
                }
                _ => {}
            }
        }
    }

    async fn handle_node_deleted(
        storage: &Arc<RwLock<Storage>>,
        event: WatchedEvent,
        event_sender: &tokio::sync::mpsc::UnboundedSender<Event>,
    ) {
        let mut storage = storage.write().await;
        storage.tree.remove_child(&event.path);
        match storage.data.get(&event.path) {
            None => {}
            Some(_data) => {}
        }
        match storage.data.remove(&event.path) {
            None => {}
            Some(child_data) => {
                let _ = event_sender.send(Event::Delete(child_data));
            }
        }
    }

    async fn handle_node_data_changed(
        client: &zookeeper_client::Client,
        storage: &Arc<RwLock<Storage>>,
        event: WatchedEvent,
        sender: &tokio::sync::mpsc::UnboundedSender<WatchedEvent>,
        event_sender: &tokio::sync::mpsc::UnboundedSender<Event>,
    ) {
        let mut storage = storage.write().await;
        let old = storage.data.get(&event.path).unwrap().clone();
        if let Err(err) = Self::get_data(client, &event.path, &mut storage, sender).await {
            debug_assert_eq!(err, zookeeper_client::Error::NoNode);
            // data deleted
            storage.tree.remove_child(&event.path);
            let child_data = storage.data.remove(&event.path).unwrap();
            let _ = event_sender.send(Event::Delete(child_data));
            return;
        };
        let new = storage.data.get(&event.path).unwrap().clone();
        let _ = event_sender.send(Event::Update { old, new });
    }

    async fn handle_children_change(
        client: &zookeeper_client::Client,
        storage: &Arc<RwLock<Storage>>,
        event: WatchedEvent,
        sender: &tokio::sync::mpsc::UnboundedSender<WatchedEvent>,
        event_sender: &tokio::sync::mpsc::UnboundedSender<Event>,
    ) {
        let old_children = storage
            .read()
            .await
            .tree
            .children(&event.path)
            .into_iter()
            .map(|child| child.to_string())
            .collect::<Vec<_>>();
        let new_children = match Self::list_children(client, &event.path, sender).await {
            Ok(children) => children
                .iter()
                .map(|child| make_path(&event.path, child))
                .collect::<Vec<_>>(),
            Err(err) => {
                debug_assert_eq!(err, zookeeper_client::Error::NoNode);
                return;
            }
        };
        let (added, _) = compare(&old_children, &new_children);
        //only handle node added
        let added = added
            .into_iter()
            .map(|added| {
                let zk = client.clone();
                let path = event.path.clone();
                let sender = sender.clone();
                let event_sender = event_sender.clone();
                (zk, storage, path, added, sender, event_sender)
            })
            .collect::<Vec<_>>();
        stream::iter(added)
            .for_each_concurrent(
                // we fetch children through stream
                20,
                |(zk, storage, parent, child_path, sender, event_sender)| async move {
                    let mut storage = storage.write().await;
                    if let Err(err) =
                        Self::get_data(&zk, &child_path, &mut storage, &sender.clone()).await
                    {
                        debug_assert_eq!(err, zookeeper_client::Error::NoNode);
                        return;
                    }
                    storage.tree.add_child(&parent, child_path.clone());
                    let child_data = storage.data.get(&child_path).unwrap();
                    let _ = event_sender.send(Event::Add(child_data.clone()));
                },
            )
            .await;
    }

    async fn get_data(
        client: &zookeeper_client::Client,
        path: &str,
        storage: &mut RwLockWriteGuard<'_, Storage>,
        sender: &tokio::sync::mpsc::UnboundedSender<WatchedEvent>,
    ) -> std::result::Result<SharedChildData, zookeeper_client::Error> {
        let (data, stat, watcher) = client.get_and_watch_data(path).await?;
        let data = Arc::new(ChildData {
            path: path.to_string(),
            data,
            stat,
        });
        storage.data.insert(path.to_string(), data.clone());
        {
            let sender = sender.clone();
            tokio::spawn(async move {
                let _ = sender.send(watcher.changed().await);
            });
        }
        Ok(data)
    }

    async fn list_children(
        client: &zookeeper_client::Client,
        path: &str,
        sender: &tokio::sync::mpsc::UnboundedSender<WatchedEvent>,
    ) -> std::result::Result<Vec<String>, zookeeper_client::Error> {
        let (children, watcher) = client.list_and_watch_children(path).await?;
        {
            let sender = sender.clone();
            tokio::spawn(async move {
                let _ = sender.send(watcher.changed().await);
            });
        }
        Ok(children)
    }

    /// get the root node, if it exists return true, or return false
    #[async_recursion]
    async fn get_root_node(
        client: &zookeeper_client::Client,
        path: &str,
        storage: &mut RwLockWriteGuard<'_, Storage>,
        sender: &tokio::sync::mpsc::UnboundedSender<WatchedEvent>,
    ) -> std::result::Result<RootStatus, zookeeper_client::Error> {
        match client.check_and_watch_stat(path).await? {
            (None, watcher) => {
                let sender = sender.clone();
                tokio::spawn(async move {
                    let _ = sender.send(watcher.changed().await);
                });
                Ok(RootStatus::NotExist)
            }
            (Some(_), _) => {
                match Self::get_data(client, path, storage, sender).await {
                    Ok(data) if data.stat.ephemeral_owner != 0 => {
                        Ok(RootStatus::Ephemeral(data.clone()))
                    }
                    Ok(data) => Ok(RootStatus::Persistent(data.clone())),
                    Err(err) => {
                        debug_assert_eq!(err, zookeeper_client::Error::NoNode);
                        // if  node exist -> node deleted, we need to repeat this function
                        Self::get_root_node(client, path, storage, sender).await
                    }
                }
            }
        }
    }

    #[async_recursion]
    async fn fetch_all(
        client: &zookeeper_client::Client,
        path: &str,
        storage: &mut RwLockWriteGuard<Storage>,
        sender: &tokio::sync::mpsc::UnboundedSender<WatchedEvent>,
        root: bool,
    ) -> std::result::Result<(), zookeeper_client::Error> {
        let persistent = if root {
            matches!(
                Self::get_root_node(client, path, storage, sender).await?,
                RootStatus::Persistent(_)
            )
        } else {
            Self::get_data(client, path, storage, sender)
                .await?
                .stat
                .ephemeral_owner
                == 0
        };
        if persistent {
            let children = match Self::list_children(client, path, sender).await {
                Ok(children) => children,
                Err(_) => return Ok(()),
            };
            storage.tree.add_children(
                path,
                children
                    .iter()
                    .map(|child| make_path(path, child))
                    .collect(),
            );
            for child in children.iter() {
                if let Err(zookeeper_client::Error::NoNode) = Self::fetch_all(
                    client,
                    make_path(path, child).as_str(),
                    storage,
                    sender,
                    false,
                )
                .await
                {
                    continue;
                }
            }
        }
        Ok(())
    }

    #[async_recursion]
    async fn compare_storage(
        path: &str,
        old: &RwLockReadGuard<'_, Storage>,
        new: &RwLockReadGuard<'_, Storage>,
        sender: &tokio::sync::mpsc::UnboundedSender<Event>,
    ) {
        let old_data = old.data.get(path);
        let new_data = new.data.get(path);
        match (old_data, new_data) {
            (Some(data), None) => {
                let _ = sender.send(Event::Delete(data.clone()));
            }
            (None, Some(data)) => {
                let _ = sender.send(Event::Add(data.clone()));
            }
            (Some(old), Some(new)) => {
                if !old.eq(new) {
                    let _ = sender.send(Event::Update {
                        old: old.clone(),
                        new: new.clone(),
                    });
                }
            }
            _ => {}
        }
        let mut old_children = old.tree.children(path);
        let mut new_children = new.tree.children(path);
        old_children.append(&mut new_children);
        let children = old_children.into_iter().collect::<HashSet<_>>();
        for child in children.iter() {
            Self::compare_storage(child, old, new, sender).await;
        }
    }
}

fn make_path(parent: &str, child: &str) -> String {
    if let Some('/') = parent.chars().last() {
        format!("{}{}", parent, child)
    } else {
        format!("{}/{}", parent, child)
    }
}

fn compare(old: &[String], new: &[String]) -> (Vec<String>, Vec<String>) {
    let old_map = old.iter().collect::<HashSet<_>>();
    let new_map = new.iter().collect::<HashSet<_>>();
    let and = &new_map & &old_map;
    (
        (&new_map ^ &and)
            .into_iter()
            .map(|s| s.to_string())
            .collect(),
        (&old_map ^ &and)
            .into_iter()
            .map(|s| s.to_string())
            .collect(),
    )
}

enum RootStatus {
    NotExist,
    Ephemeral(SharedChildData),
    Persistent(SharedChildData),
}

#[cfg(test)]
mod tests {
    #[test]
    fn compare() {
        let old = ["1".to_string(), "2".to_string(), "3".to_string()];
        let new = ["2".to_string(), "3".to_string(), "4".to_string()];
        let (added, deleted) = super::compare(&old, &new);
        assert_eq!(added, vec!["4".to_string()]);
        assert_eq!(deleted, vec!["1".to_string()]);
    }
}
