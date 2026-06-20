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

#[tokio::test]
async fn test_watch_cancel() {
    let mut client = common::connect().await;
    let key = key!("watch/cancel");

    let mut watch = client.watch(key.as_str(), None).await.unwrap();

    client.put(key.as_str(), "before_cancel", None).await.unwrap();
    let resp = next_event(&mut watch, 5).await;
    let watch_id = resp.watch_id();
    assert!(watch_id > 0);

    watch.cancel(watch_id).await.unwrap();

    // Wait for cancel confirmation
    let cancel_resp = tokio::time::timeout(Duration::from_secs(5), watch.next())
        .await
        .expect("timeout")
        .expect("stream ended")
        .expect("watch error");
    assert!(cancel_resp.canceled(), "should be canceled");
    assert_eq!(cancel_resp.watch_id(), watch_id);

    // Put after cancel should NOT produce an event
    client.put(key.as_str(), "after_cancel", None).await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    // Stream should end after cancel
    let ended = tokio::time::timeout(Duration::from_secs(3), watch.next())
        .await;
    assert!(ended.is_err() || ended.unwrap().is_none(), "stream should end after cancel");
}

#[tokio::test]
async fn test_watch_filters_no_put() {
    let mut client = common::connect().await;
    let key = key!("watch/noput");

    let opts = Some(WatchOptions::new().with_filters(vec![WatchFilterType::NoPut]));
    let mut watch = client.watch(key.as_str(), opts).await.unwrap();

    // Put should be filtered out
    client.put(key.as_str(), "filtered_put", None).await.unwrap();

    // Delete should still come through
    client.delete(key.as_str(), None).await.unwrap();

    let resp = next_event(&mut watch, 5).await;

    // Should receive delete event, not put
    assert_eq!(resp.events().len(), 1, "only 1 event expected (delete, put filtered)");
    assert_eq!(resp.events()[0].event_type(), EventType::Delete);
}

#[tokio::test]
async fn test_watch_filters_no_delete() {
    let mut client = common::connect().await;
    let key = key!("watch/nodel");

    let opts = Some(WatchOptions::new().with_filters(vec![WatchFilterType::NoDelete]));
    let mut watch = client.watch(key.as_str(), opts).await.unwrap();

    client.put(key.as_str(), "visible_put", None).await.unwrap();
    client.delete(key.as_str(), None).await.unwrap();

    let resp = next_event(&mut watch, 5).await;

    // Should receive put event, not delete
    assert_eq!(resp.events().len(), 1, "only 1 event expected (put, delete filtered)");
    assert_eq!(resp.events()[0].event_type(), EventType::Put);
}

#[tokio::test]
async fn test_watch_prev_kv() {
    let mut client = common::connect().await;
    let key = key!("watch/prevkv");

    let opts = Some(WatchOptions::new().with_prev_key());
    let mut watch = client.watch(key.as_str(), opts).await.unwrap();

    // First put — prev_kv is None (key didn't exist)
    client.put(key.as_str(), "old_value", None).await.unwrap();
    let resp = next_event(&mut watch, 5).await;
    assert_eq!(resp.events().len(), 1);
    let ev = &resp.events()[0];
    assert_eq!(ev.event_type(), EventType::Put);
    assert_eq!(ev.kv().unwrap().value(), b"old_value");
    assert!(ev.prev_kv().is_none(), "first put should have no prev_kv");

    // Second put — prev_kv should be "old_value"
    client.put(key.as_str(), "new_value", None).await.unwrap();
    let resp2 = next_event(&mut watch, 5).await;
    assert_eq!(resp2.events().len(), 1);
    let ev2 = &resp2.events()[0];
    assert_eq!(ev2.event_type(), EventType::Put);
    assert_eq!(ev2.kv().unwrap().value(), b"new_value");

    let prev = ev2.prev_kv().expect("prev_key should be present on second put");
    assert_eq!(prev.value(), b"old_value");
}

#[tokio::test]
async fn test_watch_client_assigned_id() {
    let mut client = common::connect().await;
    let key = key!("watch/assign_id");

    let opts = Some(WatchOptions::new().with_watch_id(42));
    let mut watch = client.watch(key.as_str(), opts).await.unwrap();

    client.put(key.as_str(), "assigned", None).await.unwrap();

    let resp = next_event(&mut watch, 5).await;
    assert_eq!(resp.watch_id(), 42, "watch_id should match client-assigned value");
}

#[tokio::test]
async fn test_watch_compact_revision() {
    let mut client = common::connect().await;
    let key = key!("watch/compact_rev");

    // Put and get current revision
    for _ in 0..3 {
        client.put(key.as_str(), "v", None).await.unwrap();
    }
    let get = client.get(key.as_str(), None).await.unwrap();
    let rev = get.header().unwrap().revision();

    // Compact to a revision just before current
    let compact_rev = rev - 1;
    let mut watch = client.watch(key.as_str(), None).await.unwrap();
    client.put(key.as_str(), "after_compact", None).await.unwrap();
    // Wait for put event
    let _resp = next_event(&mut watch, 5).await;

    // Compact
    client.compact(compact_rev, None).await.unwrap();

    // Now try to watch from before compacted revision — should get compact_revision error
    let opts = Some(WatchOptions::new().with_start_revision(1));
    let mut watch2 = client.watch(key.as_str(), opts).await.unwrap();

    let compact_resp = tokio::time::timeout(Duration::from_secs(5), watch2.next())
        .await
        .expect("timeout")
        .expect("stream ended")
        .expect("watch error");

    assert!(compact_resp.compact_revision() > 0, "should report compact_revision");
    assert!(compact_resp.canceled(), "watch should be canceled due to compaction");
}
