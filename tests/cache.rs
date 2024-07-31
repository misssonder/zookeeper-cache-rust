use futures::StreamExt;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use testcontainers::core::{ExecCommand, IntoContainerPort};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage};
use tokio::sync::RwLock;
use zookeeper_cache::{CacheBuilder, Error, Event, Result};

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

    #[allow(dead_code)]
    pub async fn stop(&self) {
        self.server.stop().await.unwrap()
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
    let (root, second, third) = ("/test", "/test/test", "/test/test/test");
    let url = server.url().await;
    let (cache, mut stream) = CacheBuilder::new(root).build(&url).await?;
    let client = server.client().await?;
    {
        client.create(root, &[], PERSISTENT_OPEN).await.unwrap();
        let event = stream.next().await.unwrap();
        assert!(matches!(event, Event::Add(data) if data.path.eq(root)));
        assert!(cache.get(root).await.is_some());
    }

    {
        client.create(second, &[], PERSISTENT_OPEN).await.unwrap();
        let event = stream.next().await.unwrap();
        assert!(matches!(event, Event::Add(data) if data.path.eq(second)));
        assert!(cache.get(second).await.is_some());
    }
    {
        client.set_data(second, &[1], None).await.unwrap();
        let event = stream.next().await.unwrap();
        assert!(matches!(event, Event::Update{old,..} if old.path.eq(second)));
    }

    {
        client.create(third, &[], EPHEMERAL_OPEN).await.unwrap();
        let event = tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(event, Event::Add(data) if data.path.eq(third)));
        assert!(cache.get(third).await.is_some());
    }

    {
        client.delete(third, None).await.unwrap();
        let event = stream.next().await.unwrap();
        assert!(matches!(event, Event::Delete(data) if data.path.eq(third)));
        assert!(cache.get(third).await.is_none());
    }
    {
        client.create(third, &[], PERSISTENT_OPEN).await.unwrap();
        let event = tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(event, Event::Add(data) if data.path.eq(third)));
        assert!(cache.get(third).await.is_some());
    }
    {
        client.set_data(third, &[1], None).await.unwrap();
        let event = stream.next().await.unwrap();
        assert!(matches!(event, Event::Update{old,..} if old.path.eq(third)));
    }

    {
        client.delete(third, None).await.unwrap();
        let event = stream.next().await.unwrap();
        assert!(matches!(event, Event::Delete(data) if data.path.eq(third)));
        assert!(&cache.get(third).await.is_none());
    }

    {
        client.delete(second, None).await.unwrap();
        let event = stream.next().await.unwrap();
        assert!(matches!(event, Event::Delete(data) if data.path.eq(second)));
        assert!(&cache.get(second).await.is_none());
    }

    {
        client.delete(root, None).await.unwrap();
        let event = stream.next().await.unwrap();
        assert!(matches!(event, Event::Delete(data) if data.path.eq(root)));
        assert!(&cache.get(root).await.is_none());
    }

    Ok(())
}

#[tokio::test]
async fn cache_expired() -> Result<()> {
    let server = TestZookeeper::boot().await;
    let url = server.url().await;
    let (cache, mut stream) = CacheBuilder::new("/test")
        .with_session_timeout(Duration::from_secs(5))
        .build(url)
        .await
        .unwrap();
    let client = server.client().await.unwrap();
    client.create("/test", &[], PERSISTENT_OPEN).await.unwrap();
    let event = stream.next().await.unwrap();
    assert!(matches!(event, Event::Add(data) if data.path.eq("/test")));

    client
        .create("/test/1", &[], PERSISTENT_OPEN)
        .await
        .unwrap();
    let event = stream.next().await.unwrap();
    assert!(matches!(event, Event::Add(data) if data.path.eq("/test/1")));

    client
        .create("/test/2", &[], PERSISTENT_OPEN)
        .await
        .unwrap();
    let event = stream.next().await.unwrap();
    assert!(matches!(event, Event::Add(data) if data.path.eq("/test/2")));

    client
        .create("/test/3", &[], PERSISTENT_OPEN)
        .await
        .unwrap();
    let event = stream.next().await.unwrap();
    assert!(matches!(event, Event::Add(data) if data.path.eq("/test/3")));

    // block zookeeper client
    std::thread::sleep(Duration::from_secs(10));

    let client = server.client().await.unwrap();
    client
        .create("/test/4", &[], PERSISTENT_OPEN)
        .await
        .unwrap();
    client
        .create("/test/5", &[], PERSISTENT_OPEN)
        .await
        .unwrap();
    client
        .create("/test/6", &[], PERSISTENT_OPEN)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(1)).await;
    for path in [
        "/test", "/test/1", "/test/2", "/test/3", "/test/4", "/test/5", "/test/6",
    ] {
        let (data, _stat) = client.get_data(path).await.unwrap();
        println!("{}", path);
        assert_eq!(&data, &cache.get(path).await.unwrap().data);
    }
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
            for _ in 0..10 {
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
