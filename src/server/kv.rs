use crate::proto::etcdserverpb;
use tonic::{Request, Response, Status};

#[derive(Debug, Default)]
pub struct Kv;

#[tonic::async_trait]
impl etcdserverpb::kv_server::Kv for Kv {
    async fn range(
        &self,
        _req: Request<etcdserverpb::RangeRequest>,
    ) -> Result<Response<etcdserverpb::RangeResponse>, Status> {
        Err(Status::unimplemented("not implemented"))
    }

    async fn put(
        &self,
        _req: Request<etcdserverpb::PutRequest>,
    ) -> Result<Response<etcdserverpb::PutResponse>, Status> {
        Err(Status::unimplemented("not implemented"))
    }

    async fn delete_range(
        &self,
        _req: Request<etcdserverpb::DeleteRangeRequest>,
    ) -> Result<Response<etcdserverpb::DeleteRangeResponse>, Status> {
        Err(Status::unimplemented("not implemented"))
    }

    async fn txn(
        &self,
        _req: Request<etcdserverpb::TxnRequest>,
    ) -> Result<Response<etcdserverpb::TxnResponse>, Status> {
        Err(Status::unimplemented("not implemented"))
    }

    async fn compact(
        &self,
        _req: Request<etcdserverpb::CompactionRequest>,
    ) -> Result<Response<etcdserverpb::CompactionResponse>, Status> {
        Err(Status::unimplemented("not implemented"))
    }
}
