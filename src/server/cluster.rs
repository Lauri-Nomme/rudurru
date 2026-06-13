use crate::proto::etcdserverpb;
use crate::storage::{self, Store};
use std::sync::Arc;
use tonic::{Request, Response, Status};

fn header() -> etcdserverpb::ResponseHeader {
    etcdserverpb::ResponseHeader {
        cluster_id: 1,
        member_id: 1,
        revision: storage::current_revision() as i64,
        raft_term: 1,
    }
}

fn self_member() -> etcdserverpb::Member {
    etcdserverpb::Member {
        id: 1,
        name: "rudurru".into(),
        peer_ur_ls: vec!["http://localhost:2380".into()],
        client_ur_ls: vec!["http://localhost:2379".into()],
        is_learner: false,
    }
}

#[derive(Debug)]
#[expect(dead_code)]
pub struct Cluster {
    store: Arc<Store>,
}

impl Cluster {
    pub fn new(store: Arc<Store>) -> Self {
        Self { store }
    }
}

#[tonic::async_trait]
impl etcdserverpb::cluster_server::Cluster for Cluster {
    async fn member_add(
        &self,
        _req: Request<etcdserverpb::MemberAddRequest>,
    ) -> Result<Response<etcdserverpb::MemberAddResponse>, Status> {
        Err(Status::unimplemented("single-node cluster"))
    }

    async fn member_remove(
        &self,
        _req: Request<etcdserverpb::MemberRemoveRequest>,
    ) -> Result<Response<etcdserverpb::MemberRemoveResponse>, Status> {
        Err(Status::unimplemented("single-node cluster"))
    }

    async fn member_update(
        &self,
        _req: Request<etcdserverpb::MemberUpdateRequest>,
    ) -> Result<Response<etcdserverpb::MemberUpdateResponse>, Status> {
        Err(Status::unimplemented("single-node cluster"))
    }

    async fn member_list(
        &self,
        _req: Request<etcdserverpb::MemberListRequest>,
    ) -> Result<Response<etcdserverpb::MemberListResponse>, Status> {
        Ok(Response::new(etcdserverpb::MemberListResponse {
            header: Some(header()),
            members: vec![self_member()],
        }))
    }

    async fn member_promote(
        &self,
        _req: Request<etcdserverpb::MemberPromoteRequest>,
    ) -> Result<Response<etcdserverpb::MemberPromoteResponse>, Status> {
        Err(Status::unimplemented("single-node cluster"))
    }
}
