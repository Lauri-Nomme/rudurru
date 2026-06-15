use etcd_client::*;
use futures::StreamExt;
use std::sync::Arc;
use std::time::Duration;

mod common;

#[tokio::test]
async fn test_concurrent_puts() {
    let client = Arc::new(common::connect().await);
    let prefix = format!("concurrent/puts/{}", rand::random::<u16>());
    let n = 20;

    let mut handles = Vec::new();
    for i in 0..n {
        let c = client.clone();
        let key = format!("{prefix}/task_{i}");
        handles.push(tokio::spawn(async move {
            let mut c = (*c).clone();
            c.put(key.as_str(), format!("val_{i}"), None).await.unwrap();
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    let mut c = (*client).clone();
    let opts = Some(GetOptions::new().with_prefix());
    let resp = c.get(format!("{prefix}/"), opts).await.unwrap();
    assert_eq!(resp.count() as usize, n);
}

#[tokio::test]
async fn test_concurrent_create_txn() {
    let client = Arc::new(common::connect().await);
    let n = 10;
    let prefix = format!("concurrent/txn/{}", rand::random::<u16>());

    let mut handles = Vec::new();
    for i in 0..n {
        let c = client.clone();
        let key = format!("{prefix}/item_{i}");
        handles.push(tokio::spawn(async move {
            let mut c = (*c).clone();
            let txn = Txn::new()
                .when(vec![Compare::mod_revision(
                    key.as_str(),
                    CompareOp::Equal,
                    0,
                )])
                .and_then(vec![TxnOp::put(key.as_str(), "created", None)]);
            c.txn(txn).await.unwrap()
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    let mut c = (*client).clone();
    let opts = Some(GetOptions::new().with_prefix());
    let resp = c.get(format!("{prefix}/"), opts).await.unwrap();
    assert_eq!(resp.count() as usize, n);
}

#[tokio::test]
async fn test_concurrent_watches() {
    let client = Arc::new(common::connect().await);
    let prefix = format!("concurrent/watch/{}", rand::random::<u16>());
    let n = 5;

    let mut watch_handles = Vec::new();
    for i in 0..n {
        let c = client.clone();
        let key = format!("{prefix}/w{i}");
        watch_handles.push(tokio::spawn(async move {
            let mut c = (*c).clone();
            let mut watch = c.watch(key.as_str(), None).await.unwrap();
            let _created = tokio::time::timeout(Duration::from_secs(5), watch.next())
                .await
                .expect("timeout waiting for created")
                .expect("stream ended")
                .expect("watch error");
            c.put(format!("{key}_ready"), "1", None).await.unwrap();
            let resp = tokio::time::timeout(Duration::from_secs(10), watch.next()).await;
            let events = resp
                .expect("timeout waiting for trigger")
                .expect("stream ended")
                .expect("watch error");
            events.events().len()
        }));
    }

    for i in 0..n {
        let mut c = (*client).clone();
        let key = format!("{prefix}/w{i}");
        let ready = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let r = c.get(format!("{key}_ready"), None).await.unwrap();
                if r.count() > 0 {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await;
        assert!(ready.is_ok(), "watch {key} never became ready");
        c.put(key.as_str(), "trigger", None).await.unwrap();
    }

    for h in watch_handles {
        let event_count = h.await.unwrap();
        assert_eq!(event_count, 1);
    }
}
