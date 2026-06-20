use etcd_client::*;
use std::collections::HashSet;

mod common;

#[tokio::test]
async fn test_txn_compare_value_greater() {
    let mut client = common::connect().await;
    let key = key!("txn/val_gt");
    client.put(key.as_str(), "bbb", None).await.unwrap();

    // VALUE > "aaa" should succeed
    let txn = Txn::new()
        .when(vec![Compare::value(key.as_str(), CompareOp::Greater, "aaa")])
        .and_then(vec![TxnOp::put(key.as_str(), "gt_ok", None)]);
    let resp = client.txn(txn).await.unwrap();
    assert!(resp.succeeded(), "value GREATER should succeed");

    // VALUE > "zzz" should fail
    let txn2 = Txn::new()
        .when(vec![Compare::value(key.as_str(), CompareOp::Greater, "zzz")])
        .and_then(vec![TxnOp::put(key.as_str(), "gt_fail", None)]);
    let resp2 = client.txn(txn2).await.unwrap();
    assert!(!resp2.succeeded(), "value GREATER should fail when not greater");
}

#[tokio::test]
async fn test_txn_compare_value_less() {
    let mut client = common::connect().await;
    let key = key!("txn/val_lt");
    client.put(key.as_str(), "mmm", None).await.unwrap();

    // VALUE < "zzz" should succeed
    let txn = Txn::new()
        .when(vec![Compare::value(key.as_str(), CompareOp::Less, "zzz")])
        .and_then(vec![TxnOp::put(key.as_str(), "lt_ok", None)]);
    let resp = client.txn(txn).await.unwrap();
    assert!(resp.succeeded(), "value LESS should succeed");

    // VALUE < "aaa" should fail
    let txn2 = Txn::new()
        .when(vec![Compare::value(key.as_str(), CompareOp::Less, "aaa")])
        .and_then(vec![TxnOp::put(key.as_str(), "lt_fail", None)]);
    let resp2 = client.txn(txn2).await.unwrap();
    assert!(!resp2.succeeded(), "value LESS should fail when not less");
}

#[tokio::test]
async fn test_txn_compare_lease() {
    let mut client = common::connect().await;
    let key = key!("txn/lease_cmp");
    let lease_id = client.lease_grant(60, None).await.unwrap().id();

    let opts = Some(PutOptions::new().with_lease(lease_id));
    client.put(key.as_str(), "lease_val", opts).await.unwrap();

    // Compare LEASE == lease_id should succeed
    let txn = Txn::new()
        .when(vec![Compare::lease(key.as_str(), CompareOp::Equal, lease_id)])
        .and_then(vec![TxnOp::put(key.as_str(), "lease_matched", None)]);
    let resp = client.txn(txn).await.unwrap();
    assert!(resp.succeeded(), "lease EQUAL should succeed");

    let get = client.get(key.as_str(), None).await.unwrap();
    assert_eq!(get.kvs()[0].value(), b"lease_matched");
}

#[tokio::test]
async fn test_txn_with_range() {
    let mut client = common::connect().await;
    let prefix = format!("txn/range/{}", rand::random::<u16>());
    client.put(format!("{prefix}/a"), "1", None).await.unwrap();
    client.put(format!("{prefix}/b"), "2", None).await.unwrap();

    // Txn: compare that key doesn't exist, then range all keys
    let txn = Txn::new()
        .when(vec![Compare::version(format!("{prefix}/c"), CompareOp::Equal, 0)])
        .and_then(vec![
            TxnOp::get(format!("{prefix}/"), Some(GetOptions::new().with_prefix())),
        ]);
    let resp = client.txn(txn).await.unwrap();
    assert!(resp.succeeded());

    let op_responses = resp.op_responses();
    assert_eq!(op_responses.len(), 1);
    match &op_responses[0] {
        TxnOpResponse::Get(r) => {
            assert_eq!(r.count(), 2, "txn range should return 2 keys");
            let keys: HashSet<&str> = r.kvs().iter().map(|kv| std::str::from_utf8(kv.key()).unwrap()).collect();
            assert!(keys.contains(&format!("{prefix}/a").as_str()));
            assert!(keys.contains(&format!("{prefix}/b").as_str()));
        }
        _ => panic!("expected Range response"),
    }
}

#[tokio::test]
async fn test_txn_nested() {
    let mut client = common::connect().await;
    let k1 = key!("txn/nested1");
    let k2 = key!("txn/nested2");
    client.put(k1.as_str(), "outer", None).await.unwrap();

    // Txn: if mod_rev matches, do inner txn that puts k2
    let get = client.get(k1.as_str(), None).await.unwrap();
    let mod_rev = get.kvs()[0].mod_revision();

    let inner_txn = Txn::new()
        .when(vec![Compare::version(k2.as_str(), CompareOp::Equal, 0)])
        .and_then(vec![TxnOp::put(k2.as_str(), "nested_ok", None)]);

    let outer = Txn::new()
        .when(vec![Compare::mod_revision(k1.as_str(), CompareOp::Equal, mod_rev)])
        .and_then(vec![TxnOp::txn(inner_txn)]);
    let resp = client.txn(outer).await.unwrap();
    assert!(resp.succeeded(), "outer txn should succeed");

    // Rudurru's nested-txn handler is a stub: it returns succeeded: true but
    // does NOT execute inner operations. See `unsupported_features_tests`.
    let get2 = client.get(k2.as_str(), None).await.unwrap();
    assert_eq!(get2.count(), 0, "inner txn ops are not executed (stub behavior)");
}

#[tokio::test]
async fn test_txn_compare_version_greater() {
    let mut client = common::connect().await;
    let key = key!("txn/ver_gt");
    client.put(key.as_str(), "v1", None).await.unwrap();
    client.put(key.as_str(), "v2", None).await.unwrap();

    // Version should be 2 after two puts
    let txn = Txn::new()
        .when(vec![Compare::version(key.as_str(), CompareOp::Greater, 1)])
        .and_then(vec![TxnOp::put(key.as_str(), "gt_ok", None)]);
    let resp = client.txn(txn).await.unwrap();
    assert!(resp.succeeded(), "version GREATER than 1 should succeed");
}

#[tokio::test]
async fn test_txn_compare_mod_greater() {
    let mut client = common::connect().await;
    let key = key!("txn/mod_gt");
    let r1 = client.put(key.as_str(), "a", None).await.unwrap();
    let rev1 = r1.header().unwrap().revision();
    let r2 = client.put(key.as_str(), "b", None).await.unwrap();
    let _rev2 = r2.header().unwrap().revision();

    // mod_revision should be rev2 > rev1 (key last modified at rev2)
    let txn = Txn::new()
        .when(vec![Compare::mod_revision(key.as_str(), CompareOp::Greater, rev1)])
        .and_then(vec![TxnOp::put("dummy_mod_gt", "ok", None)]);
    let resp = client.txn(txn).await.unwrap();
    assert!(resp.succeeded(), "mod_revision GREATER should succeed");

    // mod_revision GREATER than current (rev2) should fail
    // (use a fresh key so no prior txn has bumped its mod_revision)
    let key2 = key!("txn/mod_gt2");
    client.put(key2.as_str(), "a", None).await.unwrap();
    let r2b = client.put(key2.as_str(), "b", None).await.unwrap();
    let rev2b = r2b.header().unwrap().revision();

    let txn2 = Txn::new()
        .when(vec![Compare::mod_revision(key2.as_str(), CompareOp::Greater, rev2b)])
        .and_then(vec![TxnOp::put(key2.as_str(), "mod_gt_fail", None)]);
    let resp2 = client.txn(txn2).await.unwrap();
    assert!(!resp2.succeeded(), "mod_revision GREATER than current should fail");
}

#[tokio::test]
async fn test_create_if_not_exists() {
    let mut client = common::connect().await;
    let key = key!("txn/create");

    let txn = Txn::new()
        .when(vec![Compare::mod_revision(
            key.as_str(),
            CompareOp::Equal,
            0,
        )])
        .and_then(vec![TxnOp::put(key.as_str(), "first", None)]);
    let resp = client.txn(txn).await.unwrap();
    assert!(resp.succeeded(), "first create should succeed");

    let txn2 = Txn::new()
        .when(vec![Compare::mod_revision(
            key.as_str(),
            CompareOp::Equal,
            0,
        )])
        .and_then(vec![TxnOp::put(key.as_str(), "second", None)]);
    let resp2 = client.txn(txn2).await.unwrap();
    assert!(!resp2.succeeded(), "second create should fail");

    let get = client.get(key.as_str(), None).await.unwrap();
    assert_eq!(get.kvs()[0].value(), b"first");
}

#[tokio::test]
async fn test_update_if_match() {
    let mut client = common::connect().await;
    let key = key!("txn/update");
    client.put(key.as_str(), "original", None).await.unwrap();

    let get = client.get(key.as_str(), None).await.unwrap();
    let mod_rev = get.kvs()[0].mod_revision();

    let txn = Txn::new()
        .when(vec![Compare::mod_revision(
            key.as_str(),
            CompareOp::Equal,
            mod_rev,
        )])
        .and_then(vec![TxnOp::put(key.as_str(), "updated", None)]);
    let resp = client.txn(txn).await.unwrap();
    assert!(resp.succeeded(), "update with correct mod_rev");

    let get2 = client.get(key.as_str(), None).await.unwrap();
    assert_eq!(get2.kvs()[0].value(), b"updated");

    let txn2 = Txn::new()
        .when(vec![Compare::mod_revision(
            key.as_str(),
            CompareOp::Equal,
            mod_rev,
        )])
        .and_then(vec![TxnOp::put(key.as_str(), "stale", None)]);
    let resp2 = client.txn(txn2).await.unwrap();
    assert!(!resp2.succeeded(), "update with stale mod_rev");
}

#[tokio::test]
async fn test_delete_if_match() {
    let mut client = common::connect().await;
    let key = key!("txn/delete");
    client.put(key.as_str(), "todelete", None).await.unwrap();

    let get = client.get(key.as_str(), None).await.unwrap();
    let mod_rev = get.kvs()[0].mod_revision();

    let txn = Txn::new()
        .when(vec![Compare::mod_revision(
            key.as_str(),
            CompareOp::Equal,
            mod_rev,
        )])
        .and_then(vec![TxnOp::delete(key.as_str(), None)]);
    let resp = client.txn(txn).await.unwrap();
    assert!(resp.succeeded(), "delete with correct mod_rev");

    let get2 = client.get(key.as_str(), None).await.unwrap();
    assert_eq!(get2.count(), 0);
}

#[tokio::test]
async fn test_transaction_cas() {
    let mut client = common::connect().await;
    let key = key!("txn/cas");
    client.put(key.as_str(), "initial", None).await.unwrap();

    let txn = Txn::new()
        .when(vec![Compare::value(
            key.as_str(),
            CompareOp::Equal,
            "initial",
        )])
        .and_then(vec![TxnOp::put(key.as_str(), "updated", None)])
        .or_else(vec![TxnOp::put(key.as_str(), "failed", None)]);
    let resp = client.txn(txn).await.unwrap();
    assert!(resp.succeeded());

    let get = client.get(key.as_str(), None).await.unwrap();
    assert_eq!(get.kvs()[0].value(), b"updated");

    let txn2 = Txn::new()
        .when(vec![Compare::value(
            key.as_str(),
            CompareOp::Equal,
            "initial",
        )])
        .and_then(vec![TxnOp::put(key.as_str(), "should_not_happen", None)])
        .or_else(vec![TxnOp::put(key.as_str(), "fallback", None)]);
    let resp2 = client.txn(txn2).await.unwrap();
    assert!(!resp2.succeeded());

    let get2 = client.get(key.as_str(), None).await.unwrap();
    assert_eq!(get2.kvs()[0].value(), b"fallback");
}

#[tokio::test]
async fn test_transaction_multi_cond() {
    let mut client = common::connect().await;
    let k1 = key!("txn/multi_a");
    let k2 = key!("txn/multi_b");

    client.put(k1.as_str(), "x", None).await.unwrap();
    client.put(k2.as_str(), "y", None).await.unwrap();

    let get1 = client.get(k1.as_str(), None).await.unwrap();
    let get2 = client.get(k2.as_str(), None).await.unwrap();
    let rev1 = get1.kvs()[0].mod_revision();
    let rev2 = get2.kvs()[0].mod_revision();

    let txn = Txn::new()
        .when(vec![
            Compare::mod_revision(k1.as_str(), CompareOp::Equal, rev1),
            Compare::mod_revision(k2.as_str(), CompareOp::Equal, rev2),
        ])
        .and_then(vec![
            TxnOp::put(k1.as_str(), "updated_x", None),
            TxnOp::put(k2.as_str(), "updated_y", None),
        ]);
    let resp = client.txn(txn).await.unwrap();
    assert!(resp.succeeded());

    let r = client.get(k1.as_str(), None).await.unwrap();
    assert_eq!(r.kvs()[0].value(), b"updated_x");
}

// ── Regression tests ──────────────────────────────────────────────────

#[tokio::test]
async fn test_txn_compare_value_equal() {
    let mut client = common::connect().await;
    let key = key!("txn/val_eq");
    client.put(key.as_str(), "match_me", None).await.unwrap();

    // Compare::value(..., Equal, "match_me") should succeed
    let txn = Txn::new()
        .when(vec![Compare::value(
            key.as_str(),
            CompareOp::Equal,
            "match_me",
        )])
        .and_then(vec![TxnOp::put(key.as_str(), "matched", None)]);
    let resp = client.txn(txn).await.unwrap();
    assert!(resp.succeeded(), "value equal should succeed");

    let get = client.get(key.as_str(), None).await.unwrap();
    assert_eq!(get.kvs()[0].value(), b"matched");
}

#[tokio::test]
async fn test_txn_compare_value_not_equal() {
    let mut client = common::connect().await;
    let key = key!("txn/val_neq");
    client.put(key.as_str(), "original", None).await.unwrap();

    // Compare::value(..., NotEqual, "wrong") should succeed
    let txn = Txn::new()
        .when(vec![Compare::value(
            key.as_str(),
            CompareOp::NotEqual,
            "wrong",
        )])
        .and_then(vec![TxnOp::put(key.as_str(), "diff", None)]);
    let resp = client.txn(txn).await.unwrap();
    assert!(resp.succeeded(), "value not-equal should succeed");

    let get = client.get(key.as_str(), None).await.unwrap();
    assert_eq!(get.kvs()[0].value(), b"diff");
}

#[tokio::test]
async fn test_txn_compare_version() {
    let mut client = common::connect().await;
    let key = key!("txn/ver");

    // Key doesn't exist yet — version should be 0
    let txn = Txn::new()
        .when(vec![Compare::version(key.as_str(), CompareOp::Equal, 0)])
        .and_then(vec![TxnOp::put(key.as_str(), "v1", None)]);
    let resp = client.txn(txn).await.unwrap();
    assert!(resp.succeeded(), "version==0 should succeed for new key");

    // Now version should be 1
    let txn2 = Txn::new()
        .when(vec![Compare::version(key.as_str(), CompareOp::Equal, 1)])
        .and_then(vec![TxnOp::put(key.as_str(), "v2", None)]);
    let resp2 = client.txn(txn2).await.unwrap();
    assert!(
        resp2.succeeded(),
        "version==1 should succeed after first put"
    );
}

#[tokio::test]
async fn test_txn_compare_create_revision() {
    let mut client = common::connect().await;
    let key = key!("txn/cr");

    client.put(key.as_str(), "first", None).await.unwrap();
    let get = client.get(key.as_str(), None).await.unwrap();
    let create_rev = get.kvs()[0].create_revision();

    let txn = Txn::new()
        .when(vec![Compare::create_revision(
            key.as_str(),
            CompareOp::Equal,
            create_rev,
        )])
        .and_then(vec![TxnOp::put(key.as_str(), "checked", None)]);
    let resp = client.txn(txn).await.unwrap();
    assert!(resp.succeeded(), "create_revision match should succeed");

    // Now try with wrong create_revision
    let txn2 = Txn::new()
        .when(vec![Compare::create_revision(
            key.as_str(),
            CompareOp::Equal,
            create_rev + 999,
        )])
        .and_then(vec![TxnOp::put(key.as_str(), "wrong", None)]);
    let resp2 = client.txn(txn2).await.unwrap();
    assert!(!resp2.succeeded(), "wrong create_revision should fail");
}
