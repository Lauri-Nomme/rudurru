use crate::proto::etcdserverpb;
use crate::proto::etcdserverpb::watch_request;
use crate::proto::mvccpb;
use crate::storage::{self, current_revision, wal, Store, WatchEvent, WatchRegistration};
use prost::bytes::Bytes;
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

fn kvrec_to_event(rec: &wal::KvWalRecord) -> Option<WatchEvent> {
    let deleted = (rec.flags & wal::DELETED) != 0;
    let rev = rec.mod_revision().unwrap_or(0) as u64;
    let key = rec.key()?.to_vec();
    Some(WatchEvent {
        revision: rev,
        event_type: if deleted {
            mvccpb::event::EventType::Delete
        } else {
            mvccpb::event::EventType::Put
        },
        key,
        kv_bytes: Bytes::from(rec.kv_bytes.clone()),
        prev_kv_bytes: Bytes::new(),
    })
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
                                    while let Some(event) = event_rx.recv().await {
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
                                            let mut s = store_clone.state.write().await;
                                            s.cancel_watcher(watch_id);
                                            break;
                                        }
                                    }
                                });
                            }
                            other => match other {
                                watch_request::RequestUnion::CancelRequest(cancel) => {
                                    let watch_id = cancel.watch_id;
                                    {
                                        let mut state = store.state.write().await;
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

async fn flush_global_batch(batch: &mut Vec<GlobalCreate>, store: &Arc<Store>) {
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
        if c.pending.start_revision > cur_rev && c.pending.start_revision - cur_rev > 10_000 {
            tracing::info!(
                start_revision = c.pending.start_revision,
                current_revision = cur_rev,
                key = %String::from_utf8_lossy(&c.pending.key),
                stream_id = c.stream_id,
                remote_addr = %c.remote_addr,
                "watch_too_large"
            );
            let resp = etcdserverpb::WatchResponse {
                header: Some(make_header(cur_rev as i64)),
                watch_id: -1,
                created: false,
                canceled: true,
                compact_revision: 0,
                cancel_reason: format!(
                    "too large resource version: {}, current: {}",
                    c.pending.start_revision, cur_rev
                ),
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

    // ── checkpoint rev + offset ────────────────────────────────────
    let (checkpoint_rev, checkpoint_offset) = {
        let state = store.state.read().await;
        let rev = current_revision();
        let off = std::fs::metadata(&state.wal.path)
            .map(|m| m.len())
            .unwrap_or(0);
        (rev, off)
    };

    // ── Phase 1: scan without lock, shared across all streams ──────
    let phase1_us = {
        let t0 = Instant::now();
        let wal_path = store.wal_path().await;
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
                let event = match kvrec_to_event(rec) {
                    Some(e) => e,
                    None => return,
                };
                for ctx in &active {
                    if rev >= ctx.start_revision && storage::matches_range(ctx.bound.to_ref(), key)
                    {
                        let _ = ctx.event_tx.send(event.clone());
                    }
                }
            });
        }
        t0.elapsed().as_micros()
    };

    // ── Phase 2: under lock, catch up then register all ────────────
    let t_lock = Instant::now();
    let mut state = store.state.write().await;
    let lock_us = t_lock.elapsed().as_micros();

    let t_work = Instant::now();

    let _ = state.wal.scan_kv(checkpoint_offset, |rec| {
        let rev = rec.mod_revision().unwrap_or(0) as u64;
        if rev <= checkpoint_rev {
            return;
        }
        let key = match rec.key() {
            Some(k) => k,
            None => return,
        };
        let event = match kvrec_to_event(rec) {
            Some(e) => e,
            None => return,
        };
        for ctx in &active {
            if storage::matches_range(ctx.bound.to_ref(), key) {
                let _ = ctx.event_tx.send(event.clone());
            }
        }
    });

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
