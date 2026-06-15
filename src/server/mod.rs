mod auth;
mod cluster;
mod kv;
mod lease;
mod maintenance;
mod watch;

use crate::proto::etcdserverpb;
use crate::storage::Store;
use std::sync::Arc;

/// Unlimited gRPC message size (responses may exceed tonic's 4MB default).
const MAX_MSG_SIZE: usize = usize::MAX;

pub fn new_kv(store: Arc<Store>) -> etcdserverpb::kv_server::KvServer<kv::Kv> {
    etcdserverpb::kv_server::KvServer::new(kv::Kv::new(store))
        .max_decoding_message_size(MAX_MSG_SIZE)
        .max_encoding_message_size(MAX_MSG_SIZE)
}

pub fn new_watch(store: Arc<Store>) -> etcdserverpb::watch_server::WatchServer<watch::Watch> {
    etcdserverpb::watch_server::WatchServer::new(watch::Watch::new(store))
        .max_decoding_message_size(MAX_MSG_SIZE)
        .max_encoding_message_size(MAX_MSG_SIZE)
}

pub fn new_lease(store: Arc<Store>) -> etcdserverpb::lease_server::LeaseServer<lease::Lease> {
    etcdserverpb::lease_server::LeaseServer::new(lease::Lease::new(store))
        .max_decoding_message_size(MAX_MSG_SIZE)
        .max_encoding_message_size(MAX_MSG_SIZE)
}

pub fn new_cluster(
    store: Arc<Store>,
) -> etcdserverpb::cluster_server::ClusterServer<cluster::Cluster> {
    etcdserverpb::cluster_server::ClusterServer::new(cluster::Cluster::new(store))
        .max_decoding_message_size(MAX_MSG_SIZE)
        .max_encoding_message_size(MAX_MSG_SIZE)
}

pub fn new_maintenance(
    store: Arc<Store>,
) -> etcdserverpb::maintenance_server::MaintenanceServer<maintenance::Maintenance> {
    etcdserverpb::maintenance_server::MaintenanceServer::new(maintenance::Maintenance::new(store))
        .max_decoding_message_size(MAX_MSG_SIZE)
        .max_encoding_message_size(MAX_MSG_SIZE)
}

pub fn new_auth(store: Arc<Store>) -> etcdserverpb::auth_server::AuthServer<auth::Auth> {
    etcdserverpb::auth_server::AuthServer::new(auth::Auth::new(store))
        .max_decoding_message_size(MAX_MSG_SIZE)
        .max_encoding_message_size(MAX_MSG_SIZE)
}
