use futures::StreamExt;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use testcontainers::core::{ExecCommand, IntoContainerPort};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage};
use tokio::sync::RwLock;
use zookeeper_cache_rust::{CacheBuilder, Error, Event, Result};

static ZK_IMAGE_TAG: &str = "3.9.1";
static ZK_PORT: u16 = 2181;
static PERSISTENT_OPEN: &zookeeper_client::CreateOptions<'static> =
    &zookeeper_client::CreateMode::Persistent.with_acls(zookeeper_client::Acls::anyone_all());
static EPHEMERAL_OPEN: &zookeeper_client::CreateOptions<'static> =
    &zookeeper_client::CreateMode::Ephemeral.with_acls(zookeeper_client::Acls::anyone_all());

async fn zookeeper() -> ContainerAsync<GenericImage> {
    let container = GenericImage::new("zookeeper", ZK_IMAGE_TAG)
        .with_exposed_port(ZK_PORT.tcp())
        .start()
        .await
        .unwrap();
    container
        .exec(ExecCommand::new(["./bin/zkServer.sh", "status"]))
        .await
        .unwrap();
    container.start().await.unwrap();
    container
}

struct TestZookeeper {
    server: ContainerAsync<GenericImage>,
}

impl TestZookeeper {
    pub async fn boot() -> Self {
        Self {
            server: zookeeper().await,
        }
    }
    pub async fn client(&self) -> Result<zookeeper_client::Client> {
        let url = self.url().await;
        Ok(zookeeper_client::Client::connect(&url).await?)
    }

    pub async fn url(&self) -> String {
        let port = self.server.get_host_port_ipv4(ZK_PORT).await.unwrap();
        let url = format!("localhost:{}", port);
        url
    }
}

#[tokio::test]
async fn cache() -> Result<()> {
    let server = TestZookeeper::boot().await;
    let url = server.url().await;
    let (_cache, mut stream) = CacheBuilder::new("/test").build(&url).await?;
    let client = server.client().await?;
    client.create("/test", &[], PERSISTENT_OPEN).await.unwrap();
    let event = stream.next().await.unwrap();
    assert!(matches!(event, Event::Add(data) if data.path.eq("/test")));

    client
        .create("/test/test", &[], EPHEMERAL_OPEN)
        .await
        .unwrap();
    let event = stream.next().await.unwrap();
    assert!(matches!(event, Event::Add(data) if data.path.eq("/test/test")));

    client.set_data("/test", &[1], None).await.unwrap();
    let event = stream.next().await.unwrap();
    assert!(matches!(event, Event::Update{old,..} if old.path.eq("/test")));

    client.set_data("/test/test", &[1], None).await.unwrap();
    let event = stream.next().await.unwrap();
    assert!(matches!(event, Event::Update{old,..} if old.path.eq("/test/test")));

    client.delete("/test/test", None).await.unwrap();
    let event = stream.next().await.unwrap();
    assert!(matches!(event, Event::Delete(data) if data.path.eq("/test/test")));

    client.delete("/test", None).await.unwrap();
    let event = stream.next().await.unwrap();
    assert!(matches!(event, Event::Delete(data) if data.path.eq("/test")));

    Ok(())
}

#[tokio::test]
async fn concurrency() -> Result<()> {
    use futures::stream::StreamExt;
    let server = TestZookeeper::boot().await;
    let url = server.url().await;
    let root = String::from("/test");
    let (cache, mut stream) = CacheBuilder::new(&root).build(url).await?;
    let client = server.client().await?;
    client.create("/test", &[], PERSISTENT_OPEN).await.unwrap();

    let handle_event_map = Arc::new(RwLock::new(HashMap::new()));
    {
        let handle_event_map = handle_event_map.clone();
        tokio::spawn(async move {
            while let Some(event) = stream.next().await {
                let event: Event = event;
                match event {
                    Event::Add(data) => {
                        handle_event_map
                            .write()
                            .await
                            .insert(data.path.clone(), data);
                    }
                    Event::Delete(data) => {
                        handle_event_map.write().await.remove(&data.path);
                    }
                    Event::Update { old: _, new } => {
                        handle_event_map.write().await.insert(new.path.clone(), new);
                    }
                }
            }
        });
    }
    let len = 100;
    let mut tasks = Vec::with_capacity(len);
    for i in 0..len {
        let client = client.clone();
        let root = root.clone();
        let task = tokio::spawn(async move {
            for _ in 0..100 {
                let path = format!("{}/{}", root, i);
                let _path = client.create(path.as_str(), &[], EPHEMERAL_OPEN).await?;
                client.set_data(path.as_ref(), &[1, 2], None).await?;
                client.delete(path.as_str(), None).await?;
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
            Ok::<_, Error>(())
        });
        tasks.push(task);
    }
    for task in tasks {
        task.await.unwrap().unwrap();
    }
    for i in 0..len {
        let path = format!("{}/{}", root, i);
        let data = client.get_data(path.as_ref()).await.ok();
        assert_eq!(cache.get(path.as_ref()).await.is_some(), data.is_some());
        match data {
            None => {}
            Some((data, stat)) => {
                let cache_data = cache.get(path.as_ref()).await.unwrap();
                assert_eq!(cache_data.data, data);
                assert_eq!(cache_data.stat, stat);
            }
        }
    }
    for (kev, val) in handle_event_map.read().await.iter() {
        assert_eq!(cache.get(kev).await, Some(val.clone()));
    }
    Ok(())
}
