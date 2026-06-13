use crate::proto::etcdserverpb;
use tonic::{Request, Response, Status};

#[derive(Debug, Default)]
pub struct Cluster;

#[tonic::async_trait]
impl etcdserverpb::cluster_server::Cluster for Cluster {
    async fn member_add(
        &self,
        _req: Request<etcdserverpb::MemberAddRequest>,
    ) -> Result<Response<etcdserverpb::MemberAddResponse>, Status> {
        Err(Status::unimplemented("not implemented"))
    }

    async fn member_remove(
        &self,
        _req: Request<etcdserverpb::MemberRemoveRequest>,
    ) -> Result<Response<etcdserverpb::MemberRemoveResponse>, Status> {
        Err(Status::unimplemented("not implemented"))
    }

    async fn member_update(
        &self,
        _req: Request<etcdserverpb::MemberUpdateRequest>,
    ) -> Result<Response<etcdserverpb::MemberUpdateResponse>, Status> {
        Err(Status::unimplemented("not implemented"))
    }

    async fn member_list(
        &self,
        _req: Request<etcdserverpb::MemberListRequest>,
    ) -> Result<Response<etcdserverpb::MemberListResponse>, Status> {
        Err(Status::unimplemented("not implemented"))
    }

    async fn member_promote(
        &self,
        _req: Request<etcdserverpb::MemberPromoteRequest>,
    ) -> Result<Response<etcdserverpb::MemberPromoteResponse>, Status> {
        Err(Status::unimplemented("not implemented"))
    }
}
