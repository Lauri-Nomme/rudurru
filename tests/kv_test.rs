use etcd_client::*;

mod common;

#[tokio::test]
async fn test_put_and_get() {
    let mut client = common::connect().await;
    let key = key!("kv/put_get");
    let val = b"hello world";

    let put_resp = client.put(key.as_str(), val, None).await.unwrap();
    assert!(put_resp.header().is_some());

    let get_resp = client.get(key.as_str(), None).await.unwrap();
    assert_eq!(get_resp.count(), 1);
    let kv = &get_resp.kvs()[0];
    assert_eq!(kv.key(), key.as_bytes());
    assert_eq!(kv.value(), val);
}

#[tokio::test]
async fn test_get_missing_key() {
    let mut client = common::connect().await;
    let key = key!("kv/missing");

    let resp = client.get(key.as_str(), None).await.unwrap();
    assert_eq!(resp.count(), 0);
    assert!(resp.kvs().is_empty());
}

#[tokio::test]
async fn test_list_with_prefix() {
    let mut client = common::connect().await;
    let prefix = format!("list/{}", rand::random::<u16>());

    for i in 0..5 {
        let key = format!("{prefix}/item_{i}");
        client.put(key.as_str(), "val", None).await.unwrap();
    }

    let opts = Some(GetOptions::new().with_prefix());
    let resp = client.get(format!("{prefix}/"), opts).await.unwrap();
    assert_eq!(resp.count(), 5);
}

#[tokio::test]
async fn test_range_with_range_end() {
    let mut client = common::connect().await;
    let prefix = format!("rangeend/{}", rand::random::<u16>());

    for i in 0..3 {
        let key = format!("{prefix}/z{i}");
        client.put(key.as_str(), "val", None).await.unwrap();
    }

    let start = format!("{prefix}/z1");
    let end = format!("{prefix}/z999");
    let opts = Some(GetOptions::new().with_range(end.as_str()));
    let resp = client.get(start, opts).await.unwrap();
    assert_eq!(resp.kvs().len(), 2);
}

#[tokio::test]
async fn test_delete_range() {
    let mut client = common::connect().await;
    let prefix = format!("delete/{}", rand::random::<u16>());

    for i in 0..5 {
        let key = format!("{prefix}/k{i}");
        client.put(key.as_str(), "val", None).await.unwrap();
    }

    let opts = Some(DeleteOptions::new().with_prefix());
    let resp = client.delete(format!("{prefix}/"), opts).await.unwrap();
    assert_eq!(resp.deleted(), 5);

    let opts = Some(GetOptions::new().with_prefix());
    let get_resp = client.get(format!("{prefix}/"), opts).await.unwrap();
    assert_eq!(get_resp.count(), 0);
}

#[tokio::test]
async fn test_get_with_limit() {
    let mut client = common::connect().await;
    let prefix = format!("limit/{}", rand::random::<u16>());

    for i in 0..10 {
        let key = format!("{prefix}/n{i}");
        client.put(key.as_str(), "val", None).await.unwrap();
    }

    let opts = Some(GetOptions::new().with_prefix().with_limit(5));
    let resp = client.get(format!("{prefix}/"), opts).await.unwrap();
    assert_eq!(resp.kvs().len(), 5);
    assert!(resp.more());
}

#[tokio::test]
async fn test_get_keys_only() {
    let mut client = common::connect().await;
    let key = key!("kv/keys_only");
    client.put(key.as_str(), "some_value", None).await.unwrap();

    let opts = Some(GetOptions::new().with_keys_only());
    let resp = client.get(key.as_str(), opts).await.unwrap();
    let kv = &resp.kvs()[0];
    assert_eq!(kv.key(), key.as_bytes());
    assert!(kv.value().is_empty());
}

#[tokio::test]
async fn test_compaction() {
    let mut client = common::connect().await;
    let key = key!("kv/compact");

    for _ in 1..=3 {
        client.put(key.as_str(), "v", None).await.unwrap();
    }

    let resp = client.get(key.as_str(), None).await.unwrap();
    let rev = resp.header().unwrap().revision();
    let compact_rev = rev - 1;

    client.compact(compact_rev, None).await.unwrap();

    let opts = Some(GetOptions::new().with_revision(compact_rev));
    let old_resp = client.get(key.as_str(), opts).await.unwrap();
    assert_eq!(old_resp.count(), 1);
}

#[tokio::test]
async fn test_mod_revision_increments() {
    let mut client = common::connect().await;
    let key = key!("kv/mod_rev");

    let r1 = client.put(key.as_str(), "a", None).await.unwrap();
    let mod1 = r1.header().unwrap().revision();

    let r2 = client.put(key.as_str(), "b", None).await.unwrap();
    let mod2 = r2.header().unwrap().revision();

    assert!(mod2 > mod1);

    let get = client.get(key.as_str(), None).await.unwrap();
    assert_eq!(get.kvs()[0].version(), 2);
}
