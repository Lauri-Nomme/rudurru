use crate::proto::etcdserverpb;
use crate::storage::Store;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

#[derive(Debug)]
pub struct Lease {
    store: Arc<Store>,
}

impl Lease {
    pub fn new(store: Arc<Store>) -> Self {
        Self { store }
    }
}

#[tonic::async_trait]
impl etcdserverpb::lease_server::Lease for Lease {
    async fn lease_grant(
        &self,
        req: Request<etcdserverpb::LeaseGrantRequest>,
    ) -> Result<Response<etcdserverpb::LeaseGrantResponse>, Status> {
        let remote = req
            .remote_addr()
            .map(|a| a.to_string())
            .unwrap_or_else(|| "unknown".into());
        let inner = req.into_inner();
        tracing::info!(
            remote_addr = %remote,
            ttl = inner.ttl,
            requested_id = inner.id,
            "LeaseGrant"
        );
        let resp = self.store.lease_grant(inner).await;
        let rev = resp.header.as_ref().map(|h| h.revision).unwrap_or(0);
        tracing::info!(
            remote_addr = %remote,
            granted_id = resp.id,
            ttl = resp.ttl,
            revision = rev,
            error = %resp.error,
            response = "ok",
            "LeaseGrantResp"
        );
        Ok(Response::new(resp))
    }

    async fn lease_revoke(
        &self,
        req: Request<etcdserverpb::LeaseRevokeRequest>,
    ) -> Result<Response<etcdserverpb::LeaseRevokeResponse>, Status> {
        let remote = req
            .remote_addr()
            .map(|a| a.to_string())
            .unwrap_or_else(|| "unknown".into());
        let inner = req.into_inner();
        tracing::info!(
            remote_addr = %remote,
            id = inner.id,
            "LeaseRevoke"
        );
        let resp = self.store.lease_revoke(inner).await;
        let rev = resp.header.as_ref().map(|h| h.revision).unwrap_or(0);
        tracing::info!(
            remote_addr = %remote,
            revision = rev,
            response = "ok",
            "LeaseRevokeResp"
        );
        Ok(Response::new(resp))
    }

    type LeaseKeepAliveStream =
        ReceiverStream<Result<etcdserverpb::LeaseKeepAliveResponse, Status>>;

    async fn lease_keep_alive(
        &self,
        req: Request<tonic::Streaming<etcdserverpb::LeaseKeepAliveRequest>>,
    ) -> Result<Response<Self::LeaseKeepAliveStream>, Status> {
        let remote = req
            .remote_addr()
            .map(|a| a.to_string())
            .unwrap_or_else(|| "unknown".into());
        let mut in_stream = req.into_inner();
        let store = self.store.clone();

        let (tx, rx) = mpsc::channel(64);

        tokio::spawn(async move {
            loop {
                match in_stream.message().await {
                    Ok(Some(msg)) => {
                        tracing::trace!(
                            remote_addr = %remote,
                            id = msg.id,
                            "LeaseKeepAlive"
                        );
                        let resp = store.lease_keep_alive(msg.id).await;
                        tracing::trace!(
                            remote_addr = %remote,
                            id = resp.id,
                            ttl = resp.ttl,
                            "LeaseKeepAliveResp"
                        );
                        if tx.send(Ok(resp)).await.is_err() {
                            return;
                        }
                    }
                    Ok(None) => break,
                    Err(e) => {
                        tracing::warn!(remote_addr = %remote, error = %e, "LeaseKeepAliveStreamError");
                        break;
                    }
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn lease_time_to_live(
        &self,
        req: Request<etcdserverpb::LeaseTimeToLiveRequest>,
    ) -> Result<Response<etcdserverpb::LeaseTimeToLiveResponse>, Status> {
        let remote = req
            .remote_addr()
            .map(|a| a.to_string())
            .unwrap_or_else(|| "unknown".into());
        let inner = req.into_inner();
        tracing::info!(
            remote_addr = %remote,
            id = inner.id,
            keys = inner.keys,
            "LeaseTimeToLive"
        );
        let resp = self.store.lease_time_to_live(inner).await;
        let rev = resp.header.as_ref().map(|h| h.revision).unwrap_or(0);
        tracing::info!(
            remote_addr = %remote,
            id = resp.id,
            ttl = resp.ttl,
            granted_ttl = resp.granted_ttl,
            keys_count = resp.keys.len(),
            revision = rev,
            response = "ok",
            "LeaseTimeToLiveResp"
        );
        Ok(Response::new(resp))
    }

    async fn lease_leases(
        &self,
        req: Request<etcdserverpb::LeaseLeasesRequest>,
    ) -> Result<Response<etcdserverpb::LeaseLeasesResponse>, Status> {
        let remote = req
            .remote_addr()
            .map(|a| a.to_string())
            .unwrap_or_else(|| "unknown".into());
        tracing::info!(remote_addr = %remote, "LeaseLeases");
        let resp = self.store.lease_leases().await;
        tracing::info!(
            remote_addr = %remote,
            lease_count = resp.leases.len(),
            response = "ok",
            "LeaseLeasesResp"
        );
        Ok(Response::new(resp))
    }
}
