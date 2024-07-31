use crate::tree::Tree;
use crate::{ChildData, Event};
use crate::{EventStream, Result, SharedChildData};
use async_recursion::async_recursion;
use futures::StreamExt;
use futures::{stream, Stream};
use std::collections::{HashMap, HashSet};
use std::mem;
use std::ops::DerefMut;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{RwLock, RwLockWriteGuard};
use tokio_util::sync::CancellationToken;
use zookeeper_client::{EventType, SessionState, WatchedEvent};

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

    pub fn replace(&mut self, data: HashMap<Path, SharedChildData>, tree: Tree<Path>) -> Storage {
        Storage {
            data: mem::replace(&mut self.data, data),
            tree: mem::replace(&mut self.tree, tree),
        }
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

/// CacheBuilder cant config the Cache's configuration
///```no_run
/// use std::time::Duration;
/// use zookeeper_cache::CacheBuilder;
/// async fn dox() -> zookeeper_cache::Result<()>{
///    let builder = CacheBuilder::new("/test")
///                .with_version(3,9,1)
///                .with_connect_timeout(Duration::from_secs(10))
///                .with_session_timeout(Duration::from_secs(10))
///                .with_reconnect_timeout(Duration::from_secs(1));
///    let (_cache,_stream) = builder.build("localhost:2181").await?;
///    Ok(())
/// }
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
/// use zookeeper_cache::CacheBuilder;
/// async fn dox() -> zookeeper_cache::Result<()>{
///    let (cache,mut stream) = CacheBuilder::default().build("localhost:2181").await?;
///        tokio::spawn(async move{
///            while let Some(_event) = stream.next().await{
///                // handle event
///            }
///        });
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
            builder: builder.clone(),
            storage,
            event_sender: sender,
            token: CancellationToken::new(),
        };
        let (sender, watcher) = tokio::sync::mpsc::unbounded_channel();
        Self::init_nodes(
            &client,
            &builder.path,
            cache.storage.write().await.deref_mut(),
            &sender,
            &cache.event_sender,
        )
        .await?;
        cache.watch(client, sender, watcher).await;
        Ok((cache, events))
    }

    /// Get data and stat through path
    ///```no_run
    /// use zookeeper_cache::CacheBuilder;
    /// async fn dox()->zookeeper_cache::Result<()>{
    ///    let (cache, _stream) = CacheBuilder::default().build("localhost:2181").await?;
    ///    cache.get("/test").await;
    ///    Ok(())
    /// }
    /// ```
    pub async fn get(&self, path: &str) -> Option<SharedChildData> {
        self.storage.read().await.data.get(path).cloned()
    }

    async fn init_nodes(
        client: &zookeeper_client::Client,
        path: &str,
        storage: &mut Storage,
        sender: &tokio::sync::mpsc::UnboundedSender<WatchedEvent>,
        event_sender: &tokio::sync::mpsc::UnboundedSender<Event>,
    ) -> Result<()> {
        let new = Arc::new(RwLock::new(Storage::new(path.to_string())));
        Self::fetch_all(client, path, &mut new.write().await, sender, true).await?;
        // send events of existed node
        let new = new.write().await;
        Self::compare_storage(path, storage, &new, event_sender).await;
        storage.replace(new.data.clone(), new.tree.clone());
        Ok(())
    }

    async fn watch(
        &self,
        mut client: zookeeper_client::Client,
        sender: tokio::sync::mpsc::UnboundedSender<WatchedEvent>,
        mut watcher: tokio::sync::mpsc::UnboundedReceiver<WatchedEvent>,
    ) {
        let addr = self.addr.clone();
        let storage = self.storage.clone();
        let sender = sender.clone();
        let builder = self.builder.clone();
        let event_sender = self.event_sender.clone();
        let token = self.token.clone();
        tokio::spawn(async move {
            let mut control = HandleControl::Handle;
            loop {
                tokio::select! {
                    _ = token.cancelled() => {
                        return
                    }
                    event = watcher.recv() => {
                        match event{
                            Some(event) => {
                                match control {
                                    HandleControl::Handle => {},
                                    HandleControl::Continue => {
                                        if event.event_type == EventType::Session && event.session_state.is_terminated(){
                                            continue;
                                        } else {
                                            control = HandleControl::Handle;
                                        }
                                    }
                                };
                                if let Some(reconnect) = Self::handle_event(&addr, &client, &builder, &storage, event, &sender, &event_sender, &token).await{
                                    client = reconnect;
                                    // to ignore the other session expired events
                                    control = HandleControl::Continue;
                                }
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
        addr: &str,
        client: &zookeeper_client::Client,
        builder: &CacheBuilder,
        storage: &Arc<RwLock<Storage>>,
        event: WatchedEvent,
        sender: &tokio::sync::mpsc::UnboundedSender<WatchedEvent>,
        event_sender: &tokio::sync::mpsc::UnboundedSender<Event>,
        token: &CancellationToken,
    ) -> Option<zookeeper_client::Client> {
        match event.event_type {
            EventType::Session => {
                if let Some(client) =
                    Self::handle_session(addr, builder, storage, event, sender, event_sender, token)
                        .await
                {
                    return Some(client);
                }
            }
            EventType::NodeDeleted => {
                Self::handle_node_deleted(storage, event, event_sender).await;
            }
            EventType::NodeDataChanged => {
                Self::handle_node_data_changed(client, storage, event, sender, event_sender).await;
            }
            EventType::NodeChildrenChanged => {
                Self::handle_children_changed(client, storage, event, sender, event_sender).await;
            }
            EventType::NodeCreated => {
                Self::handle_node_created(client, storage, event, sender, event_sender).await;
            }
        }
        None
    }

    async fn handle_session(
        addr: &str,
        builder: &CacheBuilder,
        storage: &Arc<RwLock<Storage>>,
        event: WatchedEvent,
        sender: &tokio::sync::mpsc::UnboundedSender<WatchedEvent>,
        event_sender: &tokio::sync::mpsc::UnboundedSender<Event>,
        token: &CancellationToken,
    ) -> Option<zookeeper_client::Client> {
        // todo add log
        match event.session_state {
            SessionState::Expired | SessionState::Closed => {
                let mut interval = tokio::time::interval(builder.reconnect_timeout);
                let mut connector: zookeeper_client::Connector = builder.into();
                let client = loop {
                    tokio::select! {
                        _ = token.cancelled() => {
                            return None
                        }
                        _ = interval.tick() => {
                             match connector.connect(addr).await {
                                Ok(zk) => break zk,
                                Err(_err) => {
                                }
                            };
                        }
                    }
                };
                {
                    loop {
                        match Self::init_nodes(
                            &client,
                            &builder.path,
                            storage.write().await.deref_mut(),
                            sender,
                            event_sender,
                        )
                        .await
                        {
                            Ok(_) => break,
                            Err(_err) => {
                                interval.tick().await;
                            }
                        }
                    }
                }
                return Some(client);
            }
            _ => {}
        };
        None
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

    async fn handle_children_changed(
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
                    let child_data =
                        match Self::get_data(&zk, &child_path, &mut storage, &sender).await {
                            Ok(data) => data,
                            Err(err) => {
                                debug_assert_eq!(err, zookeeper_client::Error::NoNode);
                                return;
                            }
                        };
                    storage.tree.add_child(&parent, child_path.clone());
                    if child_data.stat.ephemeral_owner == 0 {
                        if let Err(err) = Self::list_children(&zk, &child_path, &sender).await {
                            debug_assert_eq!(err, zookeeper_client::Error::NoNode);
                        }
                    }
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
        old: &Storage,
        new: &Storage,
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

#[derive(Clone, Debug)]
enum RootStatus {
    NotExist,
    Ephemeral(SharedChildData),
    Persistent(SharedChildData),
}

#[derive(Clone, Debug)]
enum HandleControl {
    Handle,
    Continue,
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
