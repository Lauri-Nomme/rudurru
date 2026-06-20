use etcd_client::*;

mod common;

#[tokio::test]
async fn test_status() {
    let mut client = common::connect().await;

    let status = client.status().await.unwrap();
    assert!(!status.version().is_empty());
    assert!(status.db_size() > 0);
}

#[tokio::test]
async fn test_status_full() {
    let mut client = common::connect().await;

    let status = client.status().await.unwrap();
    assert!(!status.version().is_empty(), "version should not be empty");
    assert!(status.db_size() > 0, "db_size should be > 0");
    assert!(status.leader() > 0, "leader should be set");
    assert!(status.raft_index() > 0, "raft_index should be > 0");
}

#[tokio::test]
async fn test_alarm_get() {
    let mut client = common::connect().await;

    let resp = client.alarm(AlarmAction::Get, AlarmType::None, None).await.unwrap();
    assert!(resp.header().is_some(), "alarm response should have header");
    assert!(resp.alarms().is_empty(), "no alarms should be active");
}

#[tokio::test]
async fn test_hash_kv() {
    let mut client = common::connect().await;

    client.put("hashkv_a", "data_a", None).await.unwrap();
    client.put("hashkv_b", "data_b", None).await.unwrap();
    let get = client.get("hashkv_b", None).await.unwrap();
    let rev = get.header().unwrap().revision();

    let resp = client.hash_kv(rev).await.unwrap();
    assert!(resp.hash() != 0, "hash_kv should return a non-zero hash");
    assert!(resp.header().is_some(), "hash_kv should have a header");
    assert!(resp.compact_version() >= 0, "compact_version should be >= 0");

    client.put("hashkv_c", "data_c", None).await.unwrap();
    let get2 = client.get("hashkv_c", None).await.unwrap();
    let rev2 = get2.header().unwrap().revision();
    let resp2 = client.hash_kv(rev2).await.unwrap();
    assert_ne!(resp.hash(), resp2.hash(), "hash should differ with more keys");
}

#[tokio::test]
async fn test_member_list() {
    let mut client = common::connect().await;

    let resp = client.member_list().await.unwrap();
    assert!(!resp.members().is_empty());
    assert!(resp.header().is_some());
}

#[tokio::test]
async fn test_hash() {
    let mut client = common::connect().await;

    let hash1 = client.hash().await.unwrap();
    assert!(hash1.hash() != 0);

    client.put("hash_test_key", "data", None).await.unwrap();

    let hash2 = client.hash().await.unwrap();
    assert_ne!(hash1.hash(), hash2.hash(), "hash should change after write");

    client.delete("hash_test_key", None).await.unwrap();
}

#[tokio::test]
async fn test_snapshot() {
    let mut client = common::connect().await;

    let mut stream = client.snapshot().await.unwrap();
    let mut total_bytes = 0usize;
    while let Some(chunk) = stream.message().await.unwrap() {
        total_bytes += chunk.blob().len();
    }
    assert!(total_bytes > 0, "snapshot should contain data");
}

#[tokio::test]
async fn test_defragment() {
    let mut client = common::connect().await;

    let resp = client.defragment().await.unwrap();
    // defragment succeeds on etcd 3.5.17, though header may be None
    let _ = resp.header();
}
