use crate::proto::etcdserverpb;
use crate::proto::etcdserverpb::watch_request;
use crate::proto::mvccpb;
use crate::storage::{self, current_revision, wal, Store, WatchEvent, WatchRegistration};
use prost::bytes::Bytes;
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot, Notify};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

static NEXT_WATCH_ID: AtomicI64 = AtomicI64::new(1);
static NEXT_STREAM_ID: AtomicU64 = AtomicU64::new(1);

fn next_watch_id() -> i64 {
    NEXT_WATCH_ID.fetch_add(1, Ordering::SeqCst)
}

fn next_stream_id() -> u64 {
    NEXT_STREAM_ID.fetch_add(1, Ordering::SeqCst)
}

fn make_header(revision: i64) -> etcdserverpb::ResponseHeader {
    etcdserverpb::ResponseHeader {
        cluster_id: 1,
        member_id: 1,
        revision,
        raft_term: 1,
    }
}

fn event_to_proto(event: &WatchEvent) -> mvccpb::Event {
    mvccpb::Event {
        r#type: event.event_type as i32,
        kv: event.kv_bytes.clone(),
        prev_kv: event.prev_kv_bytes.clone(),
    }
}

fn should_send_event(filters: &[i32], event_type: i32) -> bool {
    for &f in filters {
        match f {
            0 if event_type == 0 => return false,
            1 if event_type == 1 => return false,
            _ => {}
        }
    }
    true
}

struct PendingCreate {
    key: Vec<u8>,
    range_end: Vec<u8>,
    start_revision: u64,
    progress_notify: bool,
    filters: Vec<i32>,
    prev_kv: bool,
}

struct GlobalCreate {
    pending: PendingCreate,
    event_tx: mpsc::UnboundedSender<WatchEvent>,
    reply: oneshot::Sender<Result<etcdserverpb::WatchResponse, Status>>,
    stream_id: u64,
    remote_addr: String,
    watch_id: i64,
}

struct WatchContext {
    key: Vec<u8>,
    range_end: Vec<u8>,
    start_revision: u64,
    watch_id: i64,
    progress_notify: bool,
    filters: Vec<i32>,
    prev_kv: bool,
    bound: storage::RangeBound,
    event_tx: mpsc::UnboundedSender<WatchEvent>,
    stream_id: u64,
    remote_addr: String,
    reply: Option<oneshot::Sender<Result<etcdserverpb::WatchResponse, Status>>>,
}

#[derive(Debug)]
pub struct Watch {
    store: Arc<Store>,
    global_tx: mpsc::UnboundedSender<GlobalCreate>,
}

impl Watch {
    pub fn new(store: Arc<Store>) -> Self {
        let (global_tx, global_rx) = mpsc::unbounded_channel();
        let store_clone = store.clone();
        tokio::spawn(global_watch_loop(store_clone, global_rx));
        Self { store, global_tx }
    }
}

#[tonic::async_trait]
impl etcdserverpb::watch_server::Watch for Watch {
    type WatchStream = ReceiverStream<Result<etcdserverpb::WatchResponse, Status>>;

    async fn watch(
        &self,
        req: Request<tonic::Streaming<etcdserverpb::WatchRequest>>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        let remote_addr = req
            .remote_addr()
            .map(|a| a.to_string())
            .unwrap_or_else(|| "unknown".into());
        let mut in_stream = req.into_inner();
        let store = self.store.clone();
        let global_tx = self.global_tx.clone();
        let (tx, rx) = mpsc::channel(4096);
        let stream_id = next_stream_id();

        tokio::spawn(async move {
            loop {
                let msg = in_stream.message().await;
                match msg {
                    Ok(Some(req)) => {
                        let Some(union) = req.request_union else {
                            continue;
                        };
                        match union {
                            watch_request::RequestUnion::CreateRequest(create) => {
                                let watch_id = if create.watch_id != 0 {
                                    create.watch_id
                                } else {
                                    next_watch_id()
                                };
                                let progress_notify = create.progress_notify;

                                let (event_tx, mut event_rx) = mpsc::unbounded_channel();
                                let (reply_tx, reply_rx) = oneshot::channel();

                                if global_tx
                                    .send(GlobalCreate {
                                        pending: PendingCreate {
                                            key: create.key,
                                            range_end: create.range_end,
                                            start_revision: create.start_revision as u64,
                                            progress_notify: create.progress_notify,
                                            filters: create.filters.to_vec(),
                                            prev_kv: create.prev_kv,
                                        },
                                        event_tx,
                                        reply: reply_tx,
                                        stream_id,
                                        remote_addr: remote_addr.clone(),
                                        watch_id,
                                    })
                                    .is_err()
                                {
                                    tracing::error!("watch_global_queue_closed");
                                    return;
                                }

                                match reply_rx.await {
                                    Ok(Ok(resp)) => {
                                        if tx.send(Ok(resp)).await.is_err() {
                                            return;
                                        }
                                    }
                                    Ok(Err(status)) => {
                                        let _ = tx.send(Err(status)).await;
                                        return;
                                    }
                                    Err(_) => return,
                                }

                                let tx_clone = tx.clone();
                                let store_clone = store.clone();
                                tokio::spawn(async move {
                                    let mut interval =
                                        tokio::time::interval(Duration::from_secs(300));
                                    interval.tick().await;
                                    loop {
                                        tokio::select! {
                                            biased;
                                            event = event_rx.recv() => {
                                                let Some(event) = event else { break };
                                                let resp = etcdserverpb::WatchResponse {
                                                    header: Some(make_header(event.revision as i64)),
                                                    watch_id,
                                                    created: false,
                                                    canceled: false,
                                                    compact_revision: 0,
                                                    cancel_reason: String::new(),
                                                    events: vec![event_to_proto(&event)],
                                                    fragment: false,
                                                };
                                                if tx_clone.send(Ok(resp)).await.is_err() {
                                                    tracing::warn!(watch_id, "watch_dropped");
                                                    let mut s = store_clone.state.write();
                                                    s.cancel_watcher(watch_id);
                                                    break;
                                                }
                                            }
                                            _ = interval.tick() => {
                                                if !progress_notify {
                                                    continue;
                                                }
                                                let resp = etcdserverpb::WatchResponse {
                                                    header: Some(make_header(current_revision() as i64)),
                                                    watch_id,
                                                    created: false,
                                                    canceled: false,
                                                    compact_revision: 0,
                                                    cancel_reason: String::new(),
                                                    events: vec![],
                                                    fragment: false,
                                                };
                                                if tx_clone.send(Ok(resp)).await.is_err() {
                                                    tracing::warn!(watch_id, "watch_dropped");
                                                    let mut s = store_clone.state.write();
                                                    s.cancel_watcher(watch_id);
                                                    break;
                                                }
                                            }
                                        }
                                    }
                                });
                            }
                            other => match other {
                                watch_request::RequestUnion::CancelRequest(cancel) => {
                                    let watch_id = cancel.watch_id;
                                    {
                                        let mut state = store.state.write();
                                        state.cancel_watcher(watch_id);
                                    }
                                    tracing::info!(watch_id, "watch_canceled");
                                    let resp = etcdserverpb::WatchResponse {
                                        header: Some(make_header(current_revision() as i64)),
                                        watch_id,
                                        created: false,
                                        canceled: true,
                                        compact_revision: 0,
                                        cancel_reason: String::new(),
                                        events: vec![],
                                        fragment: false,
                                    };
                                    if tx.send(Ok(resp)).await.is_err() {
                                        return;
                                    }
                                }
                                watch_request::RequestUnion::ProgressRequest(_) => {
                                    let resp = etcdserverpb::WatchResponse {
                                        header: Some(make_header(current_revision() as i64)),
                                        watch_id: 0,
                                        created: false,
                                        canceled: false,
                                        compact_revision: 0,
                                        cancel_reason: String::new(),
                                        events: vec![],
                                        fragment: false,
                                    };
                                    if tx.send(Ok(resp)).await.is_err() {
                                        return;
                                    }
                                }
                                _ => {}
                            },
                        }
                    }
                    Ok(None) => break,
                    Err(_) => break,
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

async fn global_watch_loop(store: Arc<Store>, mut rx: mpsc::UnboundedReceiver<GlobalCreate>) {
    loop {
        let first = match rx.recv().await {
            Some(c) => c,
            None => return,
        };

        let mut batch: Vec<GlobalCreate> = vec![first];
        let notify = Arc::new(Notify::new());
        let n = notify.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            n.notify_one();
        });

        loop {
            tokio::select! {
                biased;
                item = rx.recv() => {
                    match item {
                        Some(c) => batch.push(c),
                        None => break,
                    }
                }
                _ = notify.notified() => break,
            }
        }

        flush_global_batch(&mut batch, &store).await;
    }
}

fn current_checkpoint(store: &Store) -> (u64, u64) {
    let state = store.state.read();
    let rev = current_revision();
    let off = std::fs::metadata(&state.wal.path)
        .map(|m| m.len())
        .unwrap_or(0);
    (rev, off)
}

async fn flush_global_batch(batch: &mut Vec<GlobalCreate>, store: &Arc<Store>) {
    let (checkpoint_rev, checkpoint_offset) = current_checkpoint(store);
    flush_global_batch_at(batch, store, checkpoint_rev, checkpoint_offset).await;
}

async fn flush_global_batch_at(
    batch: &mut Vec<GlobalCreate>,
    store: &Arc<Store>,
    checkpoint_rev: u64,
    checkpoint_offset: u64,
) {
    if batch.is_empty() {
        return;
    }

    // Separate compacted vs. active
    let compact_rev = store.compact_rev().await;
    let mut active: Vec<WatchContext> = Vec::with_capacity(batch.len());

    for c in batch.drain(..) {
        if c.pending.start_revision > 0 && c.pending.start_revision < compact_rev {
            tracing::info!(
                start_revision = c.pending.start_revision,
                compact_rev,
                key = %String::from_utf8_lossy(&c.pending.key),
                stream_id = c.stream_id,
                remote_addr = %c.remote_addr,
                "watch_compacted"
            );
            let resp = etcdserverpb::WatchResponse {
                header: Some(make_header(current_revision() as i64)),
                watch_id: -1,
                created: false,
                canceled: true,
                compact_revision: compact_rev as i64,
                cancel_reason: "compacted".into(),
                events: vec![],
                fragment: false,
            };
            let _ = c.reply.send(Ok(resp));
            continue;
        }

        // Reject only if the gap is egregious (bookmark races of 1-2 revs
        // are normal — etcd accepts them and the watcher catches up immediately).
        let cur_rev = current_revision();
        let gap = c.pending.start_revision.saturating_sub(cur_rev);
        if c.pending.start_revision > cur_rev && gap > 10_000 {
            let cancel_reason = format!(
                "too large resource version: {}, current: {}",
                c.pending.start_revision, cur_rev
            );
            tracing::info!(
                start_revision = c.pending.start_revision,
                current_revision = cur_rev,
                gap,
                key = %String::from_utf8_lossy(&c.pending.key),
                range_end = %String::from_utf8_lossy(&c.pending.range_end),
                stream_id = c.stream_id,
                remote_addr = %c.remote_addr,
                %cancel_reason,
                "watch_too_large"
            );
            let resp = etcdserverpb::WatchResponse {
                header: Some(make_header(cur_rev as i64)),
                watch_id: -1,
                created: false,
                canceled: true,
                compact_revision: 0,
                cancel_reason,
                events: vec![],
                fragment: false,
            };
            let _ = c.reply.send(Ok(resp));
            continue;
        }

        let bound = storage::resolve_range(&c.pending.key, &c.pending.range_end);
        active.push(WatchContext {
            key: c.pending.key,
            range_end: c.pending.range_end,
            start_revision: c.pending.start_revision,
            watch_id: c.watch_id,
            progress_notify: c.pending.progress_notify,
            filters: c.pending.filters,
            prev_kv: c.pending.prev_kv,
            bound,
            event_tx: c.event_tx,
            stream_id: c.stream_id,
            remote_addr: c.remote_addr,
            reply: Some(c.reply),
        });
    }

    if active.is_empty() {
        return;
    }

    // Reject client-assigned watch IDs that conflict with existing watchers
    // before doing any work. We need to peek into state, so we acquire and
    // release the lock briefly before Phase 1.
    {
        let state = store.state.read();
        let mut rejected = Vec::new();
        for (i, ctx) in active.iter().enumerate() {
            if state.watchers.iter().any(|w| w.watch_id == ctx.watch_id)
                || active[..i].iter().any(|other| other.watch_id == ctx.watch_id)
            {
                rejected.push(i);
            }
        }
        drop(state);
        for &i in rejected.iter().rev() {
            let mut ctx = active.swap_remove(i);
            let resp = etcdserverpb::WatchResponse {
                header: Some(make_header(current_revision() as i64)),
                watch_id: -1,
                created: false,
                canceled: true,
                compact_revision: 0,
                cancel_reason: "duplicate watch_id".into(),
                events: vec![],
                fragment: false,
            };
            tracing::info!(watch_id = ctx.watch_id, stream_id = ctx.stream_id, "watch_duplicate_id");
            if let Some(reply) = ctx.reply.take() {
                let _ = reply.send(Ok(resp));
            }
        }
    }

    if active.is_empty() {
        return;
    }

    // ── Phase 1: scan without lock, shared across all streams ──────
    let (phase1_us, mut prev_kv_map) = {
        let t0 = Instant::now();
        let wal_path = store.wal_path().await;
        let mut prev_kv_map: HashMap<Vec<u8>, Bytes> = HashMap::new();
        if let Ok(mut reader) = wal::WalFile::open(&wal_path) {
            let _ = reader.scan_kv(0, |rec| {
                let rev = rec.mod_revision().unwrap_or(0) as u64;
                if rev > checkpoint_rev {
                    return;
                }
                let key = match rec.key() {
                    Some(k) => k,
                    None => return,
                };
                let deleted = (rec.flags & wal::DELETED) != 0;
                let event_type = if deleted {
                    mvccpb::event::EventType::Delete
                } else {
                    mvccpb::event::EventType::Put
                };
                let prev_kv = prev_kv_map.get(key).cloned().unwrap_or(Bytes::new());
                if deleted {
                    prev_kv_map.remove(key);
                } else {
                    prev_kv_map.insert(key.to_vec(), Bytes::from(rec.kv_bytes.clone()));
                }
                let event = WatchEvent {
                    revision: rev,
                    event_type,
                    key: Bytes::copy_from_slice(key),
                    kv_bytes: Bytes::from(rec.kv_bytes.clone()),
                    prev_kv_bytes: prev_kv,
                };
                for ctx in &active {
                    if rev >= ctx.start_revision
                        && storage::matches_range(ctx.bound.to_ref(), key)
                        && should_send_event(&ctx.filters, event.event_type as i32)
                    {
                        let _ = ctx.event_tx.send(event.clone());
                    }
                }
            });
        }
        (t0.elapsed().as_micros(), prev_kv_map)
    };

    // ── Phase 2: under lock, catch up then register all ────────────
    let t_lock = Instant::now();
    let mut state = store.state.write();
    let lock_us = t_lock.elapsed().as_micros();

    let t_work = Instant::now();

    {
        let _ = state.wal.scan_kv(checkpoint_offset, |rec| {
            let rev = rec.mod_revision().unwrap_or(0) as u64;
            if rev <= checkpoint_rev {
                return;
            }
            let key = match rec.key() {
                Some(k) => k,
                None => return,
            };
            let deleted = (rec.flags & wal::DELETED) != 0;
            let event_type = if deleted {
                mvccpb::event::EventType::Delete
            } else {
                mvccpb::event::EventType::Put
            };
            let prev_kv = prev_kv_map.get(key).cloned().unwrap_or(Bytes::new());
            if deleted {
                prev_kv_map.remove(key);
            } else {
                prev_kv_map.insert(key.to_vec(), Bytes::from(rec.kv_bytes.clone()));
            }
            let event = WatchEvent {
                revision: rev,
                event_type,
                    key: Bytes::copy_from_slice(key),
                kv_bytes: Bytes::from(rec.kv_bytes.clone()),
                prev_kv_bytes: prev_kv,
            };
            for ctx in &active {
                if storage::matches_range(ctx.bound.to_ref(), key)
                    && should_send_event(&ctx.filters, event.event_type as i32)
                {
                    let _ = ctx.event_tx.send(event.clone());
                }
            }
        });
    }

    for ctx in &active {
        state.register_watcher(WatchRegistration {
            key: ctx.key.clone(),
            range_end: ctx.range_end.clone(),
            start_revision: ctx.start_revision,
            sender: ctx.event_tx.clone(),
            watch_id: ctx.watch_id,
            progress_notify: ctx.progress_notify,
            filters: ctx.filters.clone(),
            prev_kv: ctx.prev_kv,
            bound: ctx.bound.clone(),
        });
    }

    let scan_us = t_work.elapsed().as_micros();
    drop(state);

    let batch_size = active.len();

    // ── send created responses ─────────────────────────────────────
    for ctx in &active {
        tracing::info!(
            stream_id = ctx.stream_id,
            remote_addr = %ctx.remote_addr,
            watch_id = ctx.watch_id,
            start_revision = ctx.start_revision,
            batch_size,
            phase1_us,
            lock_us,
            scan_us,
            key = %String::from_utf8_lossy(&ctx.key),
            "watch_replay"
        );
    }

    for mut ctx in active {
        let resp = etcdserverpb::WatchResponse {
            header: Some(make_header(current_revision() as i64)),
            watch_id: ctx.watch_id,
            created: true,
            canceled: false,
            compact_revision: 0,
            cancel_reason: String::new(),
            events: vec![],
            fragment: false,
        };
        let _ = ctx.reply.take().unwrap().send(Ok(resp));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::mvccpb::KeyValue;
    use prost::Message;

    fn temp_wal() -> String {
        format!("/tmp/rudurru_watch_test_{}", std::process::id())
    }

    fn decode_val(bytes: &Bytes) -> Vec<u8> {
        KeyValue::decode(bytes.as_ref())
            .map(|kv| kv.value.to_vec())
            .unwrap_or_default()
    }

    #[tokio::test]
    async fn test_phase2_prev_kv_uses_phase1_map() {
        let path = temp_wal();
        // Clean up from prior runs
        let _ = std::fs::remove_file(&path);
        let store = Arc::new(Store::open(&path).await.unwrap());

        // Build history: v1, v2 at known revisions
        store
            .put(etcdserverpb::PutRequest {
                key: b"foo".to_vec(),
                value: b"v1".to_vec(),
                ..Default::default()
            })
            .await;
        store
            .put(etcdserverpb::PutRequest {
                key: b"foo".to_vec(),
                value: b"v2".to_vec(),
                ..Default::default()
            })
            .await;

        // Snapshot the checkpoint state AFTER v2
        let checkpoint_rev = current_revision();
        let checkpoint_offset = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);

        // Simulate a concurrent put between Phase 1 and Phase 2
        store
            .put(etcdserverpb::PutRequest {
                key: b"foo".to_vec(),
                value: b"v3".to_vec(),
                ..Default::default()
            })
            .await;

        // Set up a watcher that filters from v2 onward (prev_kv=true)
        let (event_tx, mut event_rx) = mpsc::unbounded_channel();
        let (reply_tx, _reply_rx) = oneshot::channel();

        let mut batch = vec![GlobalCreate {
            pending: PendingCreate {
                key: b"foo".to_vec(),
                range_end: vec![],
                start_revision: checkpoint_rev, // ≥ v2, < v3
                progress_notify: false,
                filters: vec![],
                prev_kv: true,
            },
            event_tx,
            reply: reply_tx,
            stream_id: 1,
            remote_addr: "test".into(),
            watch_id: 42,
        }];

        flush_global_batch_at(&mut batch, &store, checkpoint_rev, checkpoint_offset).await;

        // Collect replayed events
        let mut events: Vec<WatchEvent> = Vec::new();
        while let Ok(e) = event_rx.try_recv() {
            events.push(e);
        }

        // We expect two events: v2 (from Phase 1) and v3 (from Phase 2)
        assert_eq!(events.len(), 2, "should replay v2 and v3");

        // v2 event: kv=v2, prev_kv should be v1
        let ev2 = &events[0];
        assert_eq!(decode_val(&ev2.kv_bytes), b"v2");
        assert_eq!(decode_val(&ev2.prev_kv_bytes), b"v1", "v2 prev_kv should be v1");

        // v3 event: kv=v3, prev_kv should be v2
        let ev3 = &events[1];
        assert_eq!(decode_val(&ev3.kv_bytes), b"v3");
        assert_eq!(decode_val(&ev3.prev_kv_bytes), b"v2", "v3 prev_kv should be v2 — fails without Phase 1→Phase 2 prev_kv_map handoff");

        let _ = std::fs::remove_file(&path);
    }
}
