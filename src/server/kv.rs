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

fn fmt_key(key: &[u8]) -> String {
    String::from_utf8_lossy(key).into_owned()
}

fn log_compare(compare: &etcdserverpb::Compare) {
    let result = match compare.result {
        0 => "==",
        1 => ">",
        2 => "<",
        3 => "!=",
        _ => "?",
    };
    let target = match compare.target {
        0 => "version",
        1 => "create_revision",
        2 => "mod_revision",
        3 => "value",
        4 => "lease",
        _ => "?",
    };
    let val = match &compare.target_union {
        Some(etcdserverpb::compare::TargetUnion::Version(v)) => v.to_string(),
        Some(etcdserverpb::compare::TargetUnion::CreateRevision(v)) => v.to_string(),
        Some(etcdserverpb::compare::TargetUnion::ModRevision(v)) => v.to_string(),
        Some(etcdserverpb::compare::TargetUnion::Value(v)) => format!("{} bytes", v.len()),
        Some(etcdserverpb::compare::TargetUnion::Lease(v)) => v.to_string(),
        None => "?".into(),
    };
    tracing::trace!(
        key = %fmt_key(&compare.key),
        range_end = %fmt_key(&compare.range_end),
        result,
        target,
        val,
        "TxnCompare"
    );
}

fn log_request_op(label: &str, op: &etcdserverpb::RequestOp) {
    match &op.request {
        Some(etcdserverpb::request_op::Request::RequestRange(r)) => {
            tracing::trace!(
                op_type = "range",
                key = %fmt_key(&r.key),
                range_end = %fmt_key(&r.range_end),
                limit = r.limit,
                "{}Op", label
            );
        }
        Some(etcdserverpb::request_op::Request::RequestPut(p)) => {
            tracing::trace!(
                op_type = "put",
                key = %fmt_key(&p.key),
                value_len = p.value.len(),
                lease = p.lease,
                "{}Op", label
            );
        }
        Some(etcdserverpb::request_op::Request::RequestDeleteRange(d)) => {
            tracing::trace!(
                op_type = "delete_range",
                key = %fmt_key(&d.key),
                range_end = %fmt_key(&d.range_end),
                "{}Op", label
            );
        }
        Some(etcdserverpb::request_op::Request::RequestTxn(_)) => {
            tracing::trace!(op_type = "txn", "{}Op", label);
        }
        None => {
            tracing::trace!(op_type = "none", "{}Op", label);
        }
    }
}

fn log_response_op(label: &str, op: &etcdserverpb::ResponseOp) {
    match &op.response {
        Some(etcdserverpb::response_op::Response::ResponseRange(r)) => {
            let rev = r.header.as_ref().map(|h| h.revision).unwrap_or(0);
            tracing::trace!(
                op_type = "range",
                count = r.kvs.len(),
                more = r.more,
                revision = rev,
                "{}Resp", label
            );
        }
        Some(etcdserverpb::response_op::Response::ResponsePut(p)) => {
            let rev = p.header.as_ref().map(|h| h.revision).unwrap_or(0);
            tracing::trace!(
                op_type = "put",
                has_prev_kv = !p.prev_kv.is_empty(),
                revision = rev,
                "{}Resp", label
            );
        }
        Some(etcdserverpb::response_op::Response::ResponseDeleteRange(d)) => {
            let rev = d.header.as_ref().map(|h| h.revision).unwrap_or(0);
            tracing::trace!(
                op_type = "delete_range",
                deleted = d.deleted,
                revision = rev,
                "{}Resp", label
            );
        }
        Some(etcdserverpb::response_op::Response::ResponseTxn(_)) => {
            tracing::trace!(op_type = "txn", "{}Resp", label);
        }
        None => {
            tracing::trace!(op_type = "none", "{}Resp", label);
        }
    }
}

#[tonic::async_trait]
impl etcdserverpb::kv_server::Kv for Kv {
    async fn range(
        &self,
        req: Request<etcdserverpb::RangeRequest>,
    ) -> Result<Response<etcdserverpb::RangeResponse>, Status> {
        let remote = req
            .remote_addr()
            .map(|a| a.to_string())
            .unwrap_or_else(|| "unknown".into());
        let inner = req.into_inner();
        // Range is the most frequent operation — demoted to trace
        tracing::trace!(
            remote_addr = %remote,
            key = %fmt_key(&inner.key),
            range_end = %fmt_key(&inner.range_end),
            limit = inner.limit,
            "Range"
        );
        let resp = self.store.range(inner).await?;
        let rev = resp.header.as_ref().map(|h| h.revision).unwrap_or(0);
        tracing::trace!(
            remote_addr = %remote,
            count = resp.kvs.len(),
            more = resp.more,
            revision = rev,
            response = "ok",
            "RangeResp"
        );
        Ok(Response::new(resp))
    }

    async fn put(
        &self,
        req: Request<etcdserverpb::PutRequest>,
    ) -> Result<Response<etcdserverpb::PutResponse>, Status> {
        let remote = req
            .remote_addr()
            .map(|a| a.to_string())
            .unwrap_or_else(|| "unknown".into());
        let inner = req.into_inner();
        tracing::trace!(
            remote_addr = %remote,
            key = %fmt_key(&inner.key),
            value_len = inner.value.len(),
            lease = inner.lease,
            "Put"
        );
        let resp = match self.store.put(inner).await {
            Ok(r) => r,
            Err(e) => {
                tracing::trace!(
                    remote_addr = %remote,
                    response = "error",
                    error = %e.message(),
                    "PutResp"
                );
                return Err(e);
            }
        };
        let rev = resp.header.as_ref().map(|h| h.revision).unwrap_or(0);
        tracing::trace!(
            remote_addr = %remote,
            has_prev_kv = !resp.prev_kv.is_empty(),
            revision = rev,
            response = "ok",
            "PutResp"
        );
        Ok(Response::new(resp))
    }

    async fn delete_range(
        &self,
        req: Request<etcdserverpb::DeleteRangeRequest>,
    ) -> Result<Response<etcdserverpb::DeleteRangeResponse>, Status> {
        let remote = req
            .remote_addr()
            .map(|a| a.to_string())
            .unwrap_or_else(|| "unknown".into());
        let inner = req.into_inner();
        tracing::trace!(
            remote_addr = %remote,
            key = %fmt_key(&inner.key),
            range_end = %fmt_key(&inner.range_end),
            "DeleteRange"
        );
        let resp = match self.store.delete_range(inner).await {
            Ok(r) => r,
            Err(e) => {
                tracing::trace!(
                    remote_addr = %remote,
                    response = "error",
                    error = %e.message(),
                    "DeleteRangeResp"
                );
                return Err(e);
            }
        };
        let rev = resp.header.as_ref().map(|h| h.revision).unwrap_or(0);
        tracing::trace!(
            remote_addr = %remote,
            deleted = resp.deleted,
            revision = rev,
            response = "ok",
            "DeleteRangeResp"
        );
        Ok(Response::new(resp))
    }

    async fn txn(
        &self,
        req: Request<etcdserverpb::TxnRequest>,
    ) -> Result<Response<etcdserverpb::TxnResponse>, Status> {
        let remote = req
            .remote_addr()
            .map(|a| a.to_string())
            .unwrap_or_else(|| "unknown".into());
        let inner = req.into_inner();

        tracing::trace!(
            remote_addr = %remote,
            compare_count = inner.compare.len(),
            success_count = inner.success.len(),
            failure_count = inner.failure.len(),
            "Txn"
        );
        for (i, c) in inner.compare.iter().enumerate() {
            tracing::trace!(remote_addr = %remote, index = i, "TxnCompareStart");
            log_compare(c);
        }
        for (i, op) in inner.success.iter().enumerate() {
            tracing::trace!(remote_addr = %remote, index = i, "TxnCompareSuccess");
            log_request_op("TxnSuccess", op);
        }
        for (i, op) in inner.failure.iter().enumerate() {
            tracing::trace!(remote_addr = %remote, index = i, "TxnCompareFailure");
            log_request_op("TxnFailure", op);
        }

        let resp = match self.store.txn(inner).await {
            Ok(r) => r,
            Err(e) => {
                tracing::info!(
                    remote_addr = %remote,
                    response = "error",
                    error = %e.message(),
                    "TxnResp"
                );
                return Err(e);
            }
        };

        let rev = resp.header.as_ref().map(|h| h.revision).unwrap_or(0);
        tracing::trace!(
            remote_addr = %remote,
            succeeded = resp.succeeded,
            response_count = resp.responses.len(),
            revision = rev,
            response = "ok",
            "TxnResp"
        );
        for (i, op) in resp.responses.iter().enumerate() {
            tracing::trace!(remote_addr = %remote, index = i, "TxnRespOp");
            log_response_op("TxnResp", op);
        }
        Ok(Response::new(resp))
    }

    async fn compact(
        &self,
        req: Request<etcdserverpb::CompactionRequest>,
    ) -> Result<Response<etcdserverpb::CompactionResponse>, Status> {
        let remote = req
            .remote_addr()
            .map(|a| a.to_string())
            .unwrap_or_else(|| "unknown".into());
        let revision = req.get_ref().revision;
        let physical = req.get_ref().physical;
        let keys_before = self.store.state.read().keys.len();
        let result = self.store.compact(req.into_inner()).await;
        let keys_after = self.store.state.read().keys.len();
        let rev = result
            .as_ref()
            .ok()
            .and_then(|r| r.header.as_ref().map(|h| h.revision))
            .unwrap_or(0);
        tracing::info!(
            remote_addr = %remote,
            revision,
            physical,
            keys_before,
            keys_after,
            response_revision = rev,
            response = if result.is_ok() { "ok" } else { "error" },
            "Compact"
        );
        result.map(Response::new)
    }
}
