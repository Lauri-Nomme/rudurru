mod kv;
mod watch;
mod lease;
mod cluster;
mod maintenance;
mod auth;

use crate::proto::etcdserverpb;

pub fn new_kv() -> etcdserverpb::kv_server::KvServer<kv::Kv> {
    etcdserverpb::kv_server::KvServer::new(kv::Kv)
}

pub fn new_watch() -> etcdserverpb::watch_server::WatchServer<watch::Watch> {
    etcdserverpb::watch_server::WatchServer::new(watch::Watch)
}

pub fn new_lease() -> etcdserverpb::lease_server::LeaseServer<lease::Lease> {
    etcdserverpb::lease_server::LeaseServer::new(lease::Lease)
}

pub fn new_cluster() -> etcdserverpb::cluster_server::ClusterServer<cluster::Cluster> {
    etcdserverpb::cluster_server::ClusterServer::new(cluster::Cluster)
}

pub fn new_maintenance() -> etcdserverpb::maintenance_server::MaintenanceServer<maintenance::Maintenance> {
    etcdserverpb::maintenance_server::MaintenanceServer::new(maintenance::Maintenance)
}

pub fn new_auth() -> etcdserverpb::auth_server::AuthServer<auth::Auth> {
    etcdserverpb::auth_server::AuthServer::new(auth::Auth)
}
