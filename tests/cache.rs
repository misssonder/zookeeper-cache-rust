use futures::StreamExt;
use testcontainers::core::{ExecCommand, IntoContainerPort};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage};
use zookeeper_cache_rust::{CacheBuilder, Event, Result};

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
    client.create("/test", &[], EPHEMERAL_OPEN).await.unwrap();
    let event = stream.next().await.unwrap();
    assert!(matches!(event, Event::Add(data) if data.path.eq("/test")));
    client.set_data("/test", &[1], None).await.unwrap();
    let event = stream.next().await.unwrap();
    assert!(matches!(event, Event::Update{old,..} if old.path.eq("/test")));
    client.delete("/test", None).await.unwrap();
    let event = stream.next().await.unwrap();
    assert!(matches!(event, Event::Delete(data) if data.path.eq("/test")));
    Ok(())
}
