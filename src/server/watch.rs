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

                                let checkpoint_rev = if start_revision > 0 { current_revision() } else { 0 };

                                if start_revision > 0 && start_revision < store.compact_rev().await {
                                    let resp = etcdserverpb::WatchResponse {
                                        header: Some(make_header(current_revision() as i64)),
                                        watch_id: -1,
                                        created: false,
                                        canceled: true,
                                        compact_revision: store.compact_rev().await as i64,
                                        cancel_reason: "compacted".into(),
                                        events: vec![],
                                        fragment: false,
                                    };
                                    if tx.send(Ok(resp)).await.is_err() { return; }
                                    continue;
                                }

                                let phase1_end = if start_revision > 0 && start_revision <= checkpoint_rev {
                                    if let Ok(mut reader) = wal::WalFile::open(&store.wal_path().await) {
                                        let bound = storage::resolve_range(&key, &range_end);
                                        reader.scan(0, |rec| {
                                            if rec.revision >= start_revision
                                                && rec.revision <= checkpoint_rev
                                                && storage::matches_range(bound.to_ref(), &rec.key)
                                            {
                                                let _ = event_tx.send(rec_to_event(rec));
                                            }
                                        }).unwrap_or(0)
                                    } else {
                                        0
                                    }
                                } else {
                                    0
                                };

                                // Phase 2: under lock, catch up events after checkpoint_rev, then register
                                {
                                    let mut state = store.state.write().await;

                                    if start_revision > 0 {
                                        let bound = storage::resolve_range(&key, &range_end);
                                        let _ = state.wal.scan(phase1_end, |rec| {
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
                                }

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
