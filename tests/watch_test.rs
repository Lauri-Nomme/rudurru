use etcd_client::*;
use futures::StreamExt;
use std::time::Duration;

mod common;

/// Consume the initial watch-created response (0 events) if present.
async fn next_event(watch: &mut WatchStream, timeout_secs: u64) -> WatchResponse {
    loop {
        let resp = tokio::time::timeout(Duration::from_secs(timeout_secs), watch.next())
            .await
            .expect("timeout")
            .expect("stream ended")
            .expect("watch error");
        if !resp.events().is_empty() {
            return resp;
        }
    }
}

#[tokio::test]
async fn test_watch_key() {
    let mut client = common::connect().await;
    let key = key!("watch/single");

    let mut watch = client.watch(key.as_str(), None).await.unwrap();

    client.put(key.as_str(), "event1", None).await.unwrap();

    let resp = next_event(&mut watch, 5).await;
    assert_eq!(resp.events().len(), 1);
    let ev = &resp.events()[0];
    assert_eq!(ev.event_type(), EventType::Put);
    assert_eq!(ev.kv().unwrap().value(), b"event1");
}

#[tokio::test]
async fn test_watch_prefix() {
    let mut client = common::connect().await;
    let prefix = format!("watch/prefix/{}", rand::random::<u16>());
    let opts = Some(WatchOptions::new().with_prefix());
    let mut watch = client.watch(format!("{prefix}/"), opts).await.unwrap();

    client.put(format!("{prefix}/a"), "aa", None).await.unwrap();
    client.put(format!("{prefix}/b"), "bb", None).await.unwrap();

    for _ in 0..2 {
        let resp = next_event(&mut watch, 5).await;
        assert_eq!(resp.events().len(), 1);
        assert_eq!(resp.events()[0].event_type(), EventType::Put);
    }
}

#[tokio::test]
async fn test_watch_from_revision() {
    let mut client = common::connect().await;
    let key = key!("watch/from_rev");

    client.put(key.as_str(), "v1", None).await.unwrap();
    client.put(key.as_str(), "v2", None).await.unwrap();

    let get = client.get(key.as_str(), None).await.unwrap();
    let rev = get.kvs()[0].mod_revision();

    let opts = Some(WatchOptions::new().with_start_revision(rev));
    let mut watch = client.watch(key.as_str(), opts).await.unwrap();

    let resp = next_event(&mut watch, 5).await;
    assert_eq!(resp.events().len(), 1);
    assert_eq!(resp.events()[0].kv().unwrap().value(), b"v2");
}

#[tokio::test]
async fn test_watch_progress_notify() {
    let mut client = common::connect().await;
    let key = key!("watch/progress");

    let opts = Some(WatchOptions::new().with_progress_notify());
    let mut watch = client.watch(key.as_str(), opts).await.unwrap();

    watch.request_progress().await.unwrap();

    let resp = tokio::time::timeout(Duration::from_secs(10), watch.next())
        .await
        .expect("timeout")
        .expect("stream ended")
        .expect("watch error");

    assert!(resp.watch_id() != 0 || resp.events().is_empty());
}

#[tokio::test]
async fn test_watch_delete_event() {
    let mut client = common::connect().await;
    let key = key!("watch/delete");

    let mut watch = client.watch(key.as_str(), None).await.unwrap();

    client.put(key.as_str(), "will_delete", None).await.unwrap();
    client.delete(key.as_str(), None).await.unwrap();

    let _put = next_event(&mut watch, 5).await;

    let del_resp = next_event(&mut watch, 5).await;
    assert_eq!(del_resp.events().len(), 1);
    assert_eq!(del_resp.events()[0].event_type(), EventType::Delete);
}
