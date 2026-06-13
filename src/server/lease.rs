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
        let resp = self.store.lease_grant(req.into_inner()).await;
        Ok(Response::new(resp))
    }

    async fn lease_revoke(
        &self,
        req: Request<etcdserverpb::LeaseRevokeRequest>,
    ) -> Result<Response<etcdserverpb::LeaseRevokeResponse>, Status> {
        let resp = self.store.lease_revoke(req.into_inner()).await;
        Ok(Response::new(resp))
    }

    type LeaseKeepAliveStream = ReceiverStream<Result<etcdserverpb::LeaseKeepAliveResponse, Status>>;

    async fn lease_keep_alive(
        &self,
        req: Request<tonic::Streaming<etcdserverpb::LeaseKeepAliveRequest>>,
    ) -> Result<Response<Self::LeaseKeepAliveStream>, Status> {
        let mut in_stream = req.into_inner();
        let store = self.store.clone();

        let (tx, rx) = mpsc::channel(64);

        tokio::spawn(async move {
            loop {
                match in_stream.message().await {
                    Ok(Some(msg)) => {
                        let resp = store.lease_keep_alive(msg.id).await;
                        if tx.send(Ok(resp)).await.is_err() {
                            return;
                        }
                    }
                    Ok(None) => break,
                    Err(_) => break,
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn lease_time_to_live(
        &self,
        req: Request<etcdserverpb::LeaseTimeToLiveRequest>,
    ) -> Result<Response<etcdserverpb::LeaseTimeToLiveResponse>, Status> {
        let resp = self.store.lease_time_to_live(req.into_inner()).await;
        Ok(Response::new(resp))
    }

    async fn lease_leases(
        &self,
        _req: Request<etcdserverpb::LeaseLeasesRequest>,
    ) -> Result<Response<etcdserverpb::LeaseLeasesResponse>, Status> {
        let resp = self.store.lease_leases().await;
        Ok(Response::new(resp))
    }
}
