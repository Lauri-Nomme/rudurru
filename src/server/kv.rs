use crate::proto::etcdserverpb;
use crate::storage::Store;
use std::sync::Arc;
use tonic::{Request, Response, Status};

#[derive(Debug)]
pub struct Kv {
    store: Arc<Store>,
}

impl Kv {
    pub fn new(store: Arc<Store>) -> Self {
        Self { store }
    }
}

#[tonic::async_trait]
impl etcdserverpb::kv_server::Kv for Kv {
    async fn range(
        &self,
        req: Request<etcdserverpb::RangeRequest>,
    ) -> Result<Response<etcdserverpb::RangeResponse>, Status> {
        let resp = self.store.range(req.into_inner()).await;
        Ok(Response::new(resp))
    }

    async fn put(
        &self,
        req: Request<etcdserverpb::PutRequest>,
    ) -> Result<Response<etcdserverpb::PutResponse>, Status> {
        let resp = self.store.put(req.into_inner()).await;
        Ok(Response::new(resp))
    }

    async fn delete_range(
        &self,
        req: Request<etcdserverpb::DeleteRangeRequest>,
    ) -> Result<Response<etcdserverpb::DeleteRangeResponse>, Status> {
        let resp = self.store.delete_range(req.into_inner()).await;
        Ok(Response::new(resp))
    }

    async fn txn(
        &self,
        req: Request<etcdserverpb::TxnRequest>,
    ) -> Result<Response<etcdserverpb::TxnResponse>, Status> {
        let resp = self.store.txn(req.into_inner()).await;
        Ok(Response::new(resp))
    }

    async fn compact(
        &self,
        req: Request<etcdserverpb::CompactionRequest>,
    ) -> Result<Response<etcdserverpb::CompactionResponse>, Status> {
        let revision = req.get_ref().revision;
        let keys_before = self.store.state.read().await.keys.len();
        let resp = self.store.compact(req.into_inner()).await;
        tracing::info!(revision, keys_before, "compact");
        Ok(Response::new(resp))
    }
}
