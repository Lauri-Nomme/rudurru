use etcd_client::*;

mod common;

#[tokio::test]
async fn test_create_if_not_exists() {
    let mut client = common::connect().await;
    let key = key!("txn/create");

    let txn = Txn::new()
        .when(vec![Compare::mod_revision(key.as_str(), CompareOp::Equal, 0)])
        .and_then(vec![TxnOp::put(key.as_str(), "first", None)]);
    let resp = client.txn(txn).await.unwrap();
    assert!(resp.succeeded(), "first create should succeed");

    let txn2 = Txn::new()
        .when(vec![Compare::mod_revision(key.as_str(), CompareOp::Equal, 0)])
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
        .when(vec![Compare::mod_revision(key.as_str(), CompareOp::Equal, mod_rev)])
        .and_then(vec![TxnOp::put(key.as_str(), "updated", None)]);
    let resp = client.txn(txn).await.unwrap();
    assert!(resp.succeeded(), "update with correct mod_rev");

    let get2 = client.get(key.as_str(), None).await.unwrap();
    assert_eq!(get2.kvs()[0].value(), b"updated");

    let txn2 = Txn::new()
        .when(vec![Compare::mod_revision(key.as_str(), CompareOp::Equal, mod_rev)])
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
        .when(vec![Compare::mod_revision(key.as_str(), CompareOp::Equal, mod_rev)])
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
        .when(vec![Compare::value(key.as_str(), CompareOp::Equal, "initial")])
        .and_then(vec![TxnOp::put(key.as_str(), "updated", None)])
        .or_else(vec![TxnOp::put(key.as_str(), "failed", None)]);
    let resp = client.txn(txn).await.unwrap();
    assert!(resp.succeeded());

    let get = client.get(key.as_str(), None).await.unwrap();
    assert_eq!(get.kvs()[0].value(), b"updated");

    let txn2 = Txn::new()
        .when(vec![Compare::value(key.as_str(), CompareOp::Equal, "initial")])
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
