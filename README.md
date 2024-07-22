# Zookeeper-cache-rust
[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Fmisssonder%2Fzookeeper-cache-rust.svg?type=shield)](https://app.fossa.com/projects/git%2Bgithub.com%2Fmisssonder%2Fzookeeper-cache-rust?ref=badge_shield)

Zookeeper-cache-rust is a asynchronous, pure rust implementation of ZooKeeper client cache, which provides a easy to watch the nodes' status of zookeeper.
# Example
Cache will watch root node and it's children nodes recursively. 
```rust
use futures::StreamExt;
use zookeeper_cache_rust::CacheBuilder;
#[tokio::main]
async fn main() -> zookeeper_cache_rust::Result<()> {
    let (cache, mut stream) = CacheBuilder::default().build("localhost:2181").await?;
    tokio::spawn(async move {
        while let Some(event) = stream.next().await {
            // handle event
            println!("get event: {:?}", event);
        }
    });
    cache.get("/test").await;
    Ok(())
}
```
When a cache be created, it will return a **Stream** also, the item of the stream is **Event**. There three event types which instead of the data created, data updated, data deleted.
```rust
#[tokio::main]
async fn main() {
    let url = String::from("localhost:2181");
    let (cache, mut stream) = CacheBuilder::new("/test").build(&url).await.unwrap();
    let client = server.client().await.unwrap();
    client.create("/test", &[], PERSISTENT_OPEN).await.unwrap();
    let event = stream.next().await.unwrap();
    assert!(matches!(event, Event::Add(data) if data.path.eq("/test")));
    assert!(cache.get("/test").await.is_some());

    client.set_data("/test", &[1], None).await.unwrap();
    let event = stream.next().await.unwrap();
    assert!(matches!(event, Event::Update{old,..} if old.path.eq("/test")));

    client.delete("/test", None).await.unwrap();
    let event = stream.next().await.unwrap();
    assert!(matches!(event, Event::Delete(data) if data.path.eq("/test")));
}
```


## License
[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Fmisssonder%2Fzookeeper-cache-rust.svg?type=large)](https://app.fossa.com/projects/git%2Bgithub.com%2Fmisssonder%2Fzookeeper-cache-rust?ref=badge_large)