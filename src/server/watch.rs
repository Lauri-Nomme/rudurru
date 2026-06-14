use crate::proto::etcdserverpb;
use crate::proto::etcdserverpb::watch_request;
use crate::proto::mvccpb;
use crate::storage::{self, Store, WatchEvent, WatchRegistration, current_revision, wal};
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use tokio::sync::mpsc;
use tokio::sync::Semaphore;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

static NEXT_WATCH_ID: AtomicI64 = AtomicI64::new(1);

/// Limits concurrent Phase 1 WAL scans to prevent IO thrash and
/// eliminate write lock contention. With many watchers created
/// concurrently during k3s startup, each Phase 1 reads the full
/// 77MB WAL from a separate file handle. Without throttling,
/// 140 concurrent reads = 11GB total, thrashing the page cache
/// AND all 140 pile up behind the write lock (500ms+ lock waits).
///
/// 4 permits: reads overlap without thrashing (308MB in flight).
/// Total Phase 1 wall time ≈ (140/4) × 190ms = 6.7s at startup.
/// Writes are NEVER blocked for more than ~50µs (the Phase 2 scan).
static PHASE1_SEM: Semaphore = Semaphore::const_new(4);

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
            loop {
                let msg = in_stream.message().await;
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

                                let key = create.key;
                                let range_end = create.range_end;
                                let start_revision = create.start_revision as u64;
                                let (event_tx, mut event_rx) = mpsc::unbounded_channel();

                                // Checkpoint both revision and file offset before any WAL scans.
                                // Phase 1 (no lock) replays revisions <= checkpoint_rev.
                                // Phase 2 (under lock) scans from checkpoint_offset and replays
                                // revisions > checkpoint_rev. This avoids scanning from byte 0
                                // when Phase 1 is skipped (start_revision > checkpoint_rev),
                                // and eliminates the race where new data appears between the
                                // P5-style current_revision() check and the write lock acquisition.
                                let (checkpoint_rev, checkpoint_offset) = if start_revision > 0 {
                                    let state = store.state.read().await;
                                    let rev = current_revision();
                                    let off = std::fs::metadata(&state.wal.path)
                                        .map(|m| m.len())
                                        .unwrap_or(0);
                                    (rev, off)
                                } else {
                                    (0, 0)
                                };

                                if start_revision > 0 && start_revision < store.compact_rev().await {
                                    let compact_rev = store.compact_rev().await;
                                    tracing::info!(
                                        start_revision,
                                        compact_rev,
                                        key = %String::from_utf8_lossy(&key),
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
                                    if tx.send(Ok(resp)).await.is_err() { return; }
                                    continue;
                                }

                                // Phase 1: scan WAL without the write lock.
                                // Semaphore limits concurrent readers to prevent IO thrash.
                                let phase1_us = {
                                    let _permit = PHASE1_SEM.acquire().await;
                                    let t0 = std::time::Instant::now();
                                    if start_revision > 0 && start_revision <= checkpoint_rev {
                                        if let Ok(mut reader) = wal::WalFile::open(&store.wal_path().await) {
                                            let bound = storage::resolve_range(&key, &range_end);
                                            let _ = reader.scan(0, |rec| {
                                                if rec.revision >= start_revision
                                                    && rec.revision <= checkpoint_rev
                                                    && storage::matches_range(bound.to_ref(), &rec.key)
                                                {
                                                    let _ = event_tx.send(rec_to_event(rec));
                                                }
                                            });
                                        }
                                    }
                                    t0.elapsed().as_micros()
                                };

                                // Phase 2: under lock, catch up events after checkpoint_rev, then register
                                let t_lock = std::time::Instant::now();
                                let mut state = store.state.write().await;
                                let lock_us = t_lock.elapsed().as_micros();

                                let t_work = std::time::Instant::now();
                                if start_revision > 0 {
                                    // Scan from checkpoint_offset (not phase1_end) to avoid
                                    // re-scanning the entire WAL when Phase 1 was skipped.
                                    // Records with revision > checkpoint_rev are new since
                                    // the checkpoint; records <= checkpoint_rev were already
                                    // handled by Phase 1 (if it ran) or are below our
                                    // start_revision (if Phase 1 was skipped).
                                    let bound = storage::resolve_range(&key, &range_end);
                                    let _ = state.wal.scan(checkpoint_offset, |rec| {
                                        if rec.revision > checkpoint_rev
                                            && storage::matches_range(bound.to_ref(), &rec.key)
                                        {
                                            let _ = event_tx.send(rec_to_event(rec));
                                        }
                                    });
                                }

                                state.register_watcher(WatchRegistration {
                                    key: key.clone(),
                                    range_end,
                                    start_revision,
                                    sender: event_tx,
                                    watch_id,
                                    progress_notify: create.progress_notify,
                                    filters: create.filters.to_vec(),
                                    prev_kv: create.prev_kv,
                                });

                                let scan_us = t_work.elapsed().as_micros();
                                drop(state);

                                tracing::info!(
                                    watch_id,
                                    start_revision,
                                    phase1_us,
                                    lock_us,
                                    scan_us,
                                    key = %String::from_utf8_lossy(&key),
                                    "watch_replay"
                                );

                                let resp = etcdserverpb::WatchResponse {
                                    header: Some(make_header(current_revision() as i64)),
                                    watch_id,
                                    created: true,
                                    canceled: false,
                                    compact_revision: 0,
                                    cancel_reason: String::new(),
                                    events: vec![],
                                    fragment: false,
                                };

                                if tx.send(Ok(resp)).await.is_err() {
                                    let mut state = store.state.write().await;
                                    state.cancel_watcher(watch_id);
                                    return;
                                }

                                tracing::info!(
                                    watch_id,
                                    start_revision,
                                    key = %String::from_utf8_lossy(&key),
                                    "watch_created"
                                );

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
                                            let mut state = store_clone.state.write().await;
                                            state.cancel_watcher(watch_id);
                                            break;
                                        }
                                    }
                                });
                            }
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
