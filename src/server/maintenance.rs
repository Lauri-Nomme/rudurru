use crate::proto::etcdserverpb;
use crate::storage::{self, Store};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

const CHUNK_SIZE: usize = 64 * 1024;

fn header() -> etcdserverpb::ResponseHeader {
    etcdserverpb::ResponseHeader {
        cluster_id: 1,
        member_id: 1,
        revision: storage::current_revision() as i64,
        raft_term: 1,
    }
}

#[derive(Debug)]
pub struct Maintenance {
    store: Arc<Store>,
}

impl Maintenance {
    pub fn new(store: Arc<Store>) -> Self {
        Self { store }
    }
}

#[tonic::async_trait]
impl etcdserverpb::maintenance_server::Maintenance for Maintenance {
    async fn alarm(
        &self,
        _req: Request<etcdserverpb::AlarmRequest>,
    ) -> Result<Response<etcdserverpb::AlarmResponse>, Status> {
        Ok(Response::new(etcdserverpb::AlarmResponse {
            header: Some(header()),
            alarms: vec![],
        }))
    }

    async fn status(
        &self,
        _req: Request<etcdserverpb::StatusRequest>,
    ) -> Result<Response<etcdserverpb::StatusResponse>, Status> {
        let db_size = self.store.db_size().await;
        Ok(Response::new(etcdserverpb::StatusResponse {
            header: Some(header()),
            version: "3.5.0".into(),
            db_size,
            leader: 1,
            raft_index: storage::current_revision(),
            raft_term: 1,
            db_size_in_use: db_size,
            is_learner: false,
            raft_applied_index: storage::current_revision(),
            errors: vec![],
        }))
    }

    async fn defragment(
        &self,
        _req: Request<etcdserverpb::DefragmentRequest>,
    ) -> Result<Response<etcdserverpb::DefragmentResponse>, Status> {
        Ok(Response::new(etcdserverpb::DefragmentResponse {
            header: None,
        }))
    }

    async fn hash(
        &self,
        _req: Request<etcdserverpb::HashRequest>,
    ) -> Result<Response<etcdserverpb::HashResponse>, Status> {
        let h = self.store.store_hash().await;
        Ok(Response::new(etcdserverpb::HashResponse {
            header: Some(header()),
            hash: h as u32,
        }))
    }

    async fn hash_kv(
        &self,
        _req: Request<etcdserverpb::HashKvRequest>,
    ) -> Result<Response<etcdserverpb::HashKvResponse>, Status> {
        let h = self.store.store_hash().await;
        let compact_rev = self.store.compact_rev().await;
        Ok(Response::new(etcdserverpb::HashKvResponse {
            header: Some(header()),
            hash: h as u32,
            compact_revision: compact_rev as i64,
        }))
    }

    type SnapshotStream = ReceiverStream<Result<etcdserverpb::SnapshotResponse, Status>>;

    async fn snapshot(
        &self,
        _req: Request<etcdserverpb::SnapshotRequest>,
    ) -> Result<Response<Self::SnapshotStream>, Status> {
        let store = self.store.clone();
        let (tx, rx) = mpsc::channel(4);

        tokio::spawn(async move {
            let state = store.state.read().await;
            let mut buf = Vec::new();
            let rev = storage::current_revision();
            // Header: revision(8) + key_count(4)
            buf.extend_from_slice(&rev.to_le_bytes());
            buf.extend_from_slice(&(state.keys.len() as u32).to_le_bytes());
            for (k, ks) in state.keys.iter() {
                if ks.deleted {
                    continue;
                }
                let key_len = (k.len() as u32).to_le_bytes();
                let val_len = (ks.value.len() as u32).to_le_bytes();
                buf.extend_from_slice(&key_len);
                buf.extend_from_slice(k);
                buf.extend_from_slice(&val_len);
                buf.extend_from_slice(&ks.value);
            }
            let total = buf.len() as u64;
            drop(state);

            let mut offset = 0usize;
            while offset < buf.len() {
                let end = (offset + CHUNK_SIZE).min(buf.len());
                let remaining = total.saturating_sub(end as u64);
                let chunk = buf[offset..end].to_vec();
                let resp = etcdserverpb::SnapshotResponse {
                    header: None,
                    remaining_bytes: remaining,
                    blob: chunk,
                };
                if tx.send(Ok(resp)).await.is_err() {
                    return;
                }
                offset = end;
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn move_leader(
        &self,
        _req: Request<etcdserverpb::MoveLeaderRequest>,
    ) -> Result<Response<etcdserverpb::MoveLeaderResponse>, Status> {
        Err(Status::unimplemented("single-node cluster"))
    }
}
