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

#[tokio::test]
async fn test_lease_grant_with_id() {
    let mut client = common::connect().await;
    let desired_id: i64 = 12345;

    let opts = Some(LeaseGrantOptions::new().with_id(desired_id));
    let grant = client.lease_grant(60, opts).await.unwrap();
    assert_eq!(grant.id(), desired_id, "lease ID should match requested ID");
    assert_eq!(grant.ttl(), 60);

    // Clean up
    client.lease_revoke(desired_id).await.unwrap();
}

#[tokio::test]
async fn test_lease_revoke_cascade() {
    let mut client = common::connect().await;
    let key1 = key!("lease/cascade1");
    let key2 = key!("lease/cascade2");

    let grant = client.lease_grant(60, None).await.unwrap();
    let id = grant.id();

    // Attach two keys to the lease
    client.put(key1.as_str(), "attached1", Some(PutOptions::new().with_lease(id))).await.unwrap();
    client.put(key2.as_str(), "attached2", Some(PutOptions::new().with_lease(id))).await.unwrap();

    // Verify keys exist
    let get = client.get(key1.as_str(), None).await.unwrap();
    assert_eq!(get.count(), 1, "key1 should exist before revoke");
    let get2 = client.get(key2.as_str(), None).await.unwrap();
    assert_eq!(get2.count(), 1, "key2 should exist before revoke");

    // Revoke lease — keys should be cascadingly deleted
    client.lease_revoke(id).await.unwrap();

    let get_after = client.get(key1.as_str(), None).await.unwrap();
    assert_eq!(get_after.count(), 0, "key1 should be deleted after lease revoke");
    let get2_after = client.get(key2.as_str(), None).await.unwrap();
    assert_eq!(get2_after.count(), 0, "key2 should be deleted after lease revoke");
}

#[tokio::test]
async fn test_lease_ttl_with_keys() {
    let mut client = common::connect().await;
    let key = key!("lease/ttl_keys");

    let grant = client.lease_grant(60, None).await.unwrap();
    let id = grant.id();

    let opts = Some(PutOptions::new().with_lease(id));
    client.put(key.as_str(), "lease_key", opts).await.unwrap();

    // Query TTL with keys option
    let ttl_opts = Some(LeaseTimeToLiveOptions::new().with_keys());
    let ttl_resp = client.lease_time_to_live(id, ttl_opts).await.unwrap();
    assert_eq!(ttl_resp.id(), id);
    assert_eq!(ttl_resp.granted_ttl(), 60);
    let attached_keys = ttl_resp.keys();
    assert!(!attached_keys.is_empty(), "attached keys should be returned");
    assert!(attached_keys.iter().any(|k| k.as_slice() == key.as_bytes()), "our key should be in attached keys");
}
