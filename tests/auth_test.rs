use etcd_client::*;

mod common;

#[tokio::test]
async fn test_auth_full_lifecycle() {
    // ---- Phase 1: Role CRUD ----
    let mut client = common::connect().await;
    let role_name = format!("test_role_{}", rand::random::<u16>());
    client.role_add(role_name.as_str()).await.unwrap();

    let perm = Permission::new(PermissionType::Readwrite, "/test/");
    client
        .role_grant_permission(role_name.as_str(), perm)
        .await
        .unwrap();

    let got = client.role_get(role_name.as_str()).await.unwrap();
    assert!(got.header().is_some());

    // ---- Phase 2: User CRUD ----
    let user_name = format!("test_user_{}", rand::random::<u16>());
    let role_b = format!("test_ur_role_{}", rand::random::<u16>());
    client.role_add(role_b.as_str()).await.unwrap();
    client
        .user_add(user_name.as_str(), "password", None)
        .await
        .unwrap();
    client
        .user_grant_role(user_name.as_str(), role_b.as_str())
        .await
        .unwrap();

    let uinfo = client.user_get(user_name.as_str()).await.unwrap();
    assert!(uinfo.roles().contains(&role_b));

    // ---- Phase 3: Enable/disable auth ----
    client.user_add("root", "root_password", None).await.unwrap();
    client.role_add("root").await.unwrap();
    client.user_grant_role("root", "root").await.unwrap();

    client.auth_enable().await.unwrap();

    let ep = std::env::var("ETCD_ENDPOINT")
        .unwrap_or_else(|_| "http://localhost:2379".to_string());
    let mut auth_client = etcd_client::Client::connect(
        [ep],
        Some(ConnectOptions::new().with_user("root", "root_password")),
    )
    .await
    .unwrap();

    auth_client.auth_disable().await.unwrap();

    // ---- Cleanup ----
    let mut clean = common::connect().await;
    clean.user_delete("root").await.unwrap();
    clean.role_delete("root").await.unwrap();
    clean.user_delete(user_name.as_str()).await.unwrap();
    clean.role_delete(role_b.as_str()).await.unwrap();
    clean.role_delete(role_name.as_str()).await.unwrap();
}
