use crate::proto::etcdserverpb;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

#[derive(Debug, Default)]
pub struct Maintenance;

#[tonic::async_trait]
impl etcdserverpb::maintenance_server::Maintenance for Maintenance {
    async fn alarm(
        &self,
        _req: Request<etcdserverpb::AlarmRequest>,
    ) -> Result<Response<etcdserverpb::AlarmResponse>, Status> {
        Err(Status::unimplemented("not implemented"))
    }

    async fn status(
        &self,
        _req: Request<etcdserverpb::StatusRequest>,
    ) -> Result<Response<etcdserverpb::StatusResponse>, Status> {
        Err(Status::unimplemented("not implemented"))
    }

    async fn defragment(
        &self,
        _req: Request<etcdserverpb::DefragmentRequest>,
    ) -> Result<Response<etcdserverpb::DefragmentResponse>, Status> {
        Err(Status::unimplemented("not implemented"))
    }

    async fn hash(
        &self,
        _req: Request<etcdserverpb::HashRequest>,
    ) -> Result<Response<etcdserverpb::HashResponse>, Status> {
        Err(Status::unimplemented("not implemented"))
    }

    async fn hash_kv(
        &self,
        _req: Request<etcdserverpb::HashKvRequest>,
    ) -> Result<Response<etcdserverpb::HashKvResponse>, Status> {
        Err(Status::unimplemented("not implemented"))
    }

    type SnapshotStream = ReceiverStream<Result<etcdserverpb::SnapshotResponse, Status>>;

    async fn snapshot(
        &self,
        _req: Request<etcdserverpb::SnapshotRequest>,
    ) -> Result<Response<Self::SnapshotStream>, Status> {
        let (tx, rx) = mpsc::channel(1);
        tokio::spawn(async move {
            let _ = tx.send(Err(Status::unimplemented("not implemented")));
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn move_leader(
        &self,
        _req: Request<etcdserverpb::MoveLeaderRequest>,
    ) -> Result<Response<etcdserverpb::MoveLeaderResponse>, Status> {
        Err(Status::unimplemented("not implemented"))
    }
}
