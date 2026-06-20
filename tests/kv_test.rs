use etcd_client::*;

mod common;

#[tokio::test]
async fn test_put_prev_kv() {
    let mut client = common::connect().await;
    let key = key!("kv/put_prev");
    client.put(key.as_str(), "first", None).await.unwrap();

    let opts = Some(PutOptions::new().with_prev_key());
    let resp = client.put(key.as_str(), "second", opts).await.unwrap();
    let prev = resp.prev_key().expect("prev_key should be present");
    assert_eq!(prev.value(), b"first");

    // First put on a key should return no prev_key
    let key2 = key!("kv/put_prev2");
    let opts2 = Some(PutOptions::new().with_prev_key());
    let resp2 = client.put(key2.as_str(), "sole", opts2).await.unwrap();
    assert!(resp2.prev_key().is_none(), "no prev_key on first put");
}

#[tokio::test]
async fn test_delete_prev_kv() {
    let mut client = common::connect().await;
    let prefix = format!("del_prev/{}", rand::random::<u16>());
    client.put(format!("{prefix}/a"), "A", None).await.unwrap();
    client.put(format!("{prefix}/b"), "B", None).await.unwrap();

    let opts = Some(DeleteOptions::new().with_prefix().with_prev_key());
    let resp = client.delete(format!("{prefix}/"), opts).await.unwrap();
    assert_eq!(resp.deleted(), 2);
    let prevs = resp.prev_kvs();
    assert_eq!(prevs.len(), 2);
    assert_eq!(prevs[0].value(), b"A");
    assert_eq!(prevs[1].value(), b"B");
}

#[tokio::test]
async fn test_range_count_only() {
    let mut client = common::connect().await;
    let prefix = format!("count_only/{}", rand::random::<u16>());

    for i in 0..5 {
        client.put(format!("{prefix}/k{i}"), "val", None).await.unwrap();
    }

    let opts = Some(GetOptions::new().with_prefix().with_count_only());
    let resp = client.get(format!("{prefix}/"), opts).await.unwrap();
    assert_eq!(resp.count(), 5);
    assert!(resp.kvs().is_empty(), "count_only should return no kvs");
}

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

// ── Regression tests for bugs found during Phase 2 ────────────────────

#[tokio::test]
async fn test_point_vs_from_key_range() {
    let mut client = common::connect().await;
    let prefix = format!("aaa_pfk/{}", rand::random::<u16>());

    // Put keys at different levels
    client.put(format!("{prefix}/a"), "1", None).await.unwrap();
    client.put(format!("{prefix}/b"), "2", None).await.unwrap();
    client.put(format!("{prefix}/c"), "3", None).await.unwrap();

    // Point lookup for one key returns exactly 1
    let point = client.get(format!("{prefix}/b"), None).await.unwrap();
    assert_eq!(point.count(), 1, "point lookup should return exactly 1 key");

    // From-key (\0 range_end) returns all keys >= key
    let from = client
        .get(
            format!("{prefix}/b"),
            Some(GetOptions::new().with_from_key()),
        )
        .await
        .unwrap();
    assert!(
        from.count() >= 2,
        "from-key should return at least 2 keys (b, c), got {}",
        from.count()
    );
    assert_eq!(from.kvs()[0].key(), format!("{prefix}/b").as_bytes());
    assert_eq!(from.kvs()[1].key(), format!("{prefix}/c").as_bytes());
}

#[tokio::test]
async fn test_all_keys_range() {
    let mut client = common::connect().await;
    let prefix = format!("bbb_allk/{}", rand::random::<u16>());

    client.put(format!("{prefix}/x"), "1", None).await.unwrap();
    client.put(format!("{prefix}/y"), "2", None).await.unwrap();

    // Get all keys (key="", range_end="\0")
    let all = client
        .get("", Some(GetOptions::new().with_all_keys()))
        .await
        .unwrap();
    assert!(
        all.count() >= 2,
        "all-keys should return at least our 2 keys, got {}",
        all.count()
    );
}

#[tokio::test]
async fn test_prefix_with_ff_boundary() {
    let mut client = common::connect().await;
    let prefix = format!("ccc_ffb/{}", rand::random::<u16>());

    let mut key_a = format!("{prefix}/").into_bytes();
    key_a.push(0xFF);
    let mut key_b = format!("{prefix}/").into_bytes();
    key_b.extend_from_slice(&[0xFF, 0xFF]);

    client.put(key_a.clone(), "1", None).await.unwrap();
    client.put(key_b.clone(), "2", None).await.unwrap();

    let mut prefix_key = format!("{prefix}/").into_bytes();
    prefix_key.push(0xFF);
    let opts = Some(GetOptions::new().with_prefix());
    let resp = client.get(prefix_key, opts).await.unwrap();
    assert_eq!(resp.count(), 2, "should match keys with 0xFF prefix");
}

#[tokio::test]
async fn test_delete_from_key() {
    let mut client = common::connect().await;
    // Use ~~~ prefix (0x7E) so from-key delete only affects our keys (sorts after all alphanumeric)
    let prefix = format!("~~~_del/{}", rand::random::<u16>());

    client.put(format!("{prefix}/a"), "1", None).await.unwrap();
    client.put(format!("{prefix}/b"), "2", None).await.unwrap();
    client.put(format!("{prefix}/c"), "3", None).await.unwrap();

    // Delete all keys >= {prefix}/b using from-key
    let del = client
        .delete(
            format!("{prefix}/b"),
            Some(DeleteOptions::new().with_from_key()),
        )
        .await
        .unwrap();
    assert_eq!(del.deleted(), 2, "should delete b and c");

    let remaining = client
        .get(format!("{prefix}/"), Some(GetOptions::new().with_prefix()))
        .await
        .unwrap();
    assert_eq!(remaining.count(), 1, "only a should remain");
    assert_eq!(remaining.kvs()[0].key(), format!("{prefix}/a").as_bytes());
}

#[tokio::test]
async fn test_range_with_start_equals_end() {
    let mut client = common::connect().await;
    let prefix = format!("ddd_seq/{}", rand::random::<u16>());

    client.put(format!("{prefix}/m"), "1", None).await.unwrap();
    client.put(format!("{prefix}/n"), "2", None).await.unwrap();

    // Range [m, m) — should return nothing since start == end
    let resp = client
        .get(
            format!("{prefix}/m"),
            Some(GetOptions::new().with_range(format!("{prefix}/m"))),
        )
        .await
        .unwrap();
    assert_eq!(resp.count(), 0, "empty range should return 0 keys");
}

#[tokio::test]
async fn test_put_delete_put_create_revision() {
    let mut client = common::connect().await;
    let key = key!("kv/pdp_cr");

    // First put
    let r1 = client.put(key.as_str(), "v1", None).await.unwrap();
    let rev1 = r1.header().unwrap().revision();

    let get1 = client.get(key.as_str(), None).await.unwrap();
    assert_eq!(get1.kvs()[0].create_revision(), rev1);
    assert_eq!(get1.kvs()[0].mod_revision(), rev1);
    assert_eq!(get1.kvs()[0].version(), 1);

    // Delete
    client.delete(key.as_str(), None).await.unwrap();

    // Second put — should be a fresh create
    let r2 = client.put(key.as_str(), "v2", None).await.unwrap();
    let rev2 = r2.header().unwrap().revision();
    assert!(rev2 > rev1);

    let get2 = client.get(key.as_str(), None).await.unwrap();
    assert_eq!(get2.kvs()[0].create_revision(), rev2);
    assert_eq!(get2.kvs()[0].mod_revision(), rev2);
    assert_eq!(get2.kvs()[0].version(), 1);
}

#[tokio::test]
async fn test_put_update_keeps_create_revision() {
    let mut client = common::connect().await;
    let key = key!("kv/upd_cr");

    client.put(key.as_str(), "a", None).await.unwrap();
    let create_rev = {
        let get = client.get(key.as_str(), None).await.unwrap();
        get.kvs()[0].create_revision()
    };

    // Update — create_revision must stay the same
    client.put(key.as_str(), "b", None).await.unwrap();
    let get = client.get(key.as_str(), None).await.unwrap();
    let kv = &get.kvs()[0];
    assert_eq!(
        kv.create_revision(),
        create_rev,
        "create_revision unchanged on update"
    );
    assert!(
        kv.mod_revision() > create_rev,
        "mod_revision increases on update"
    );
    assert_eq!(kv.version(), 2, "version increments on update");
}
