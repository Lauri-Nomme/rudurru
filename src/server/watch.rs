use crate::proto::etcdserverpb;
use crate::proto::etcdserverpb::watch_request;
use crate::proto::mvccpb;
use crate::storage::{self, Store, WatchEvent, WatchRegistration, current_revision, wal};
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

static NEXT_WATCH_ID: AtomicI64 = AtomicI64::new(1);

struct PendingCreate {
    key: Vec<u8>,
    range_end: Vec<u8>,
    start_revision: u64,
    watch_id: i64,
    progress_notify: bool,
    filters: Vec<i32>,
    prev_kv: bool,
}

fn next_watch_id() -> i64 {
    NEXT_WATCH_ID.fetch_add(1, Ordering::SeqCst)
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
        kv: Some(event.kv.clone()),
        prev_kv: event.prev_kv.clone(),
    }
}

fn rec_to_event(rec: &wal::WalRecord) -> WatchEvent {
    let deleted = (rec.flags & wal::DELETED) != 0;
    let has_lease = (rec.flags & wal::HAS_LEASE) != 0;
    WatchEvent {
        revision: rec.revision,
        event_type: if deleted { mvccpb::event::EventType::Delete } else { mvccpb::event::EventType::Put },
        kv: mvccpb::KeyValue {
            key: rec.key.clone(),
            create_revision: rec.revision as i64,
            mod_revision: rec.revision as i64,
            version: 1,
            value: rec.value.clone(),
            lease: if has_lease { rec.lease_id.unwrap_or(0) } else { 0 },
        },
        prev_kv: None,
    }
}

#[derive(Debug)]
pub struct Watch {
    store: Arc<Store>,
}

impl Watch {
    pub fn new(store: Arc<Store>) -> Self {
        Self { store }
    }
}

#[tonic::async_trait]
impl etcdserverpb::watch_server::Watch for Watch {
    type WatchStream = ReceiverStream<Result<etcdserverpb::WatchResponse, Status>>;

    async fn watch(
        &self,
        req: Request<tonic::Streaming<etcdserverpb::WatchRequest>>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        let mut in_stream = req.into_inner();
        let store = self.store.clone();

        let (tx, rx) = mpsc::channel(4096);

        tokio::spawn(async move {
            use std::sync::Arc;
            use std::time::Duration;
            use tokio::sync::Notify;

            let mut batch: Vec<PendingCreate> = Vec::new();
            let flush_notify = Arc::new(Notify::new());

            loop {
                tokio::select! {
                    biased;

                    msg = in_stream.message() => {
                        match msg {
                            Ok(Some(req)) => {
                                let Some(union) = req.request_union else { continue };
                                match union {
                                    watch_request::RequestUnion::CreateRequest(create) => {
                                        let watch_id = if create.watch_id != 0 {
                                            create.watch_id
                                        } else {
                                            next_watch_id()
                                        };
                                        batch.push(PendingCreate {
                                            key: create.key,
                                            range_end: create.range_end,
                                            start_revision: create.start_revision as u64,
                                            watch_id,
                                            progress_notify: create.progress_notify,
                                            filters: create.filters.to_vec(),
                                            prev_kv: create.prev_kv,
                                        });

                                        // Start flush timer on first create in a batch.
                                        // During a burst, creates arrive faster than 50ms
                                        // so the timer fires after the burst ends.
                                        if batch.len() == 1 {
                                            let n = flush_notify.clone();
                                            tokio::spawn(async move {
                                                tokio::time::sleep(Duration::from_millis(50)).await;
                                                n.notify_one();
                                            });
                                        }
                                    }
                                    other => {
                                        if !batch.is_empty() {
                                            flush_watch_batch(&mut batch, &store, &tx).await;
                                        }
                                        match other {
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
                                        }
                                    }
                                }
                            }
                            Ok(None) => {
                                if !batch.is_empty() {
                                    flush_watch_batch(&mut batch, &store, &tx).await;
                                }
                                break;
                            }
                            Err(_) => break,
                        }
                    }

                    _ = flush_notify.notified() => {
                        if !batch.is_empty() {
                            flush_watch_batch(&mut batch, &store, &tx).await;
                        }
                    }
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

async fn flush_watch_batch(
    batch: &mut Vec<PendingCreate>,
    store: &Arc<Store>,
    tx: &mpsc::Sender<Result<etcdserverpb::WatchResponse, Status>>,
) {
    use std::time::Instant;
    let creates: Vec<PendingCreate> = batch.drain(..).collect();
    if creates.is_empty() {
        return;
    }

    // ── compact check ──────────────────────────────────────────────
    let compact_rev = store.compact_rev().await;
    let mut active: Vec<WatchContext> = Vec::with_capacity(creates.len());

    for c in creates {
        if c.start_revision > 0 && c.start_revision < compact_rev {
            tracing::info!(
                start_revision = c.start_revision,
                compact_rev,
                key = %String::from_utf8_lossy(&c.key),
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
            let _ = tx.send(Ok(resp)).await;
            continue;
        }

        let (event_tx, event_rx) = mpsc::unbounded_channel();
        let bound = storage::resolve_range(&c.key, &c.range_end);
        active.push(WatchContext {
            key: c.key,
            range_end: c.range_end,
            start_revision: c.start_revision,
            watch_id: c.watch_id,
            progress_notify: c.progress_notify,
            filters: c.filters,
            prev_kv: c.prev_kv,
            bound,
            event_tx,
            event_rx: Some(event_rx),
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

    // ── Phase 1: scan without lock, shared across the batch ────────
    let phase1_us = {
        let t0 = Instant::now();
        let wal_path = store.wal_path().await;
        if let Ok(mut reader) = wal::WalFile::open(&wal_path) {
            let _ = reader.scan(0, |rec| {
                if rec.revision > checkpoint_rev {
                    return;
                }
                for ctx in &active {
                    if rec.revision >= ctx.start_revision
                        && storage::matches_range(ctx.bound.to_ref(), &rec.key)
                    {
                        let _ = ctx.event_tx.send(rec_to_event(rec));
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

    // Single scan from checkpoint_offset for all active watchers
    let _ = state.wal.scan(checkpoint_offset, |rec| {
        if rec.revision <= checkpoint_rev {
            return;
        }
        for ctx in &active {
            if storage::matches_range(ctx.bound.to_ref(), &rec.key) {
                let _ = ctx.event_tx.send(rec_to_event(rec));
            }
        }
    });

    // Register all watchers under the same lock
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
        });
    }

    let scan_us = t_work.elapsed().as_micros();
    drop(state);

    // ── send created responses and spawn event forwarding ──────────
    for mut ctx in active {
        tracing::info!(
            watch_id = ctx.watch_id,
            start_revision = ctx.start_revision,
            phase1_us,
            lock_us,
            scan_us,
            key = %String::from_utf8_lossy(&ctx.key),
            "watch_replay"
        );

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

        if tx.send(Ok(resp)).await.is_err() {
            let mut s = store.state.write().await;
            s.cancel_watcher(ctx.watch_id);
            return;
        }

        tracing::info!(
            watch_id = ctx.watch_id,
            start_revision = ctx.start_revision,
            key = %String::from_utf8_lossy(&ctx.key),
            "watch_created"
        );

        let tx_clone = tx.clone();
        let store_clone = store.clone();
        let mut rx = ctx.event_rx.take().unwrap();
        tokio::spawn(async move {
            while let Some(event) = rx.recv().await {
                let resp = etcdserverpb::WatchResponse {
                    header: Some(make_header(event.revision as i64)),
                    watch_id: ctx.watch_id,
                    created: false,
                    canceled: false,
                    compact_revision: 0,
                    cancel_reason: String::new(),
                    events: vec![event_to_proto(&event)],
                    fragment: false,
                };
                if tx_clone.send(Ok(resp)).await.is_err() {
                    tracing::warn!(watch_id = ctx.watch_id, "watch_dropped");
                    let mut s = store_clone.state.write().await;
                    s.cancel_watcher(ctx.watch_id);
                    break;
                }
            }
        });
    }
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
    event_rx: Option<mpsc::UnboundedReceiver<WatchEvent>>,
}
