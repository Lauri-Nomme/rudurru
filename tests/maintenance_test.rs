

mod common;

#[tokio::test]
async fn test_status() {
    let mut client = common::connect().await;

    let status = client.status().await.unwrap();
    assert!(!status.version().is_empty());
    assert!(status.db_size() > 0);
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
