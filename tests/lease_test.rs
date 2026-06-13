use etcd_client::*;
use futures::StreamExt;
use std::time::Duration;

mod common;

#[tokio::test]
async fn test_lease_grant_revoke() {
    let mut client = common::connect().await;

    let grant = client.lease_grant(60, None).await.unwrap();
    let lease_id = grant.id();
    assert!(lease_id > 0);
    assert_eq!(grant.ttl(), 60);

    client.lease_revoke(lease_id).await.unwrap();
}

#[tokio::test]
async fn test_lease_with_key_expiry() {
    let mut client = common::connect().await;
    let key = key!("lease/expiry");

    let grant = client.lease_grant(3, None).await.unwrap();
    let id = grant.id();

    let opts = Some(PutOptions::new().with_lease(id));
    client.put(key.as_str(), "lease_bound", opts).await.unwrap();

    let get = client.get(key.as_str(), None).await.unwrap();
    assert_eq!(get.count(), 1);

    tokio::time::sleep(Duration::from_secs(5)).await;

    let get2 = client.get(key.as_str(), None).await.unwrap();
    assert_eq!(get2.count(), 0);
}

#[tokio::test]
async fn test_lease_keepalive() {
    let mut client = common::connect().await;

    let grant = client.lease_grant(3, None).await.unwrap();
    let id = grant.id();

    let (mut keeper, mut stream) = client.lease_keep_alive(id).await.unwrap();
    keeper.keep_alive().await.unwrap();

    let resp = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("timeout")
        .expect("stream ended")
        .expect("error");
    assert_eq!(resp.id(), id);
    assert!(resp.ttl() > 0);
}

#[tokio::test]
async fn test_lease_ttl() {
    let mut client = common::connect().await;

    let grant = client.lease_grant(120, None).await.unwrap();
    let id = grant.id();

    let ttl_resp = client.lease_time_to_live(id, None).await.unwrap();
    assert_eq!(ttl_resp.id(), id);
    assert_eq!(ttl_resp.granted_ttl(), 120);
}

#[tokio::test]
async fn test_lease_list() {
    let mut client = common::connect().await;

    let g1 = client.lease_grant(60, None).await.unwrap();
    let g2 = client.lease_grant(60, None).await.unwrap();

    let leases = client.leases().await.unwrap();
    let ids: Vec<i64> = leases.leases().iter().map(|l| l.id()).collect();
    assert!(ids.contains(&g1.id()));
    assert!(ids.contains(&g2.id()));
}
