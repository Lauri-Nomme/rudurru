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
        _req: Request<etcdserverpb::LeaseGrantRequest>,
    ) -> Result<Response<etcdserverpb::LeaseGrantResponse>, Status> {
        Err(Status::unimplemented("not implemented"))
    }

    async fn lease_revoke(
        &self,
        _req: Request<etcdserverpb::LeaseRevokeRequest>,
    ) -> Result<Response<etcdserverpb::LeaseRevokeResponse>, Status> {
        Err(Status::unimplemented("not implemented"))
    }

    type LeaseKeepAliveStream = ReceiverStream<Result<etcdserverpb::LeaseKeepAliveResponse, Status>>;

    async fn lease_keep_alive(
        &self,
        _req: Request<tonic::Streaming<etcdserverpb::LeaseKeepAliveRequest>>,
    ) -> Result<Response<Self::LeaseKeepAliveStream>, Status> {
        let (tx, rx) = mpsc::channel(1);
        tokio::spawn(async move {
            let _ = tx.send(Err(Status::unimplemented("not implemented")));
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn lease_time_to_live(
        &self,
        _req: Request<etcdserverpb::LeaseTimeToLiveRequest>,
    ) -> Result<Response<etcdserverpb::LeaseTimeToLiveResponse>, Status> {
        Err(Status::unimplemented("not implemented"))
    }

    async fn lease_leases(
        &self,
        _req: Request<etcdserverpb::LeaseLeasesRequest>,
    ) -> Result<Response<etcdserverpb::LeaseLeasesResponse>, Status> {
        Err(Status::unimplemented("not implemented"))
    }
}
