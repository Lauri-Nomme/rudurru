use crate::proto::mvccpb;
use crate::storage::state::{make_kv_bytes, with_mod_revision};
use crate::storage::wal;
use crate::storage::{StoreState, WatchEvent};
use prost::bytes::Bytes;
use prost::Message;
use tonic::Status;

impl StoreState {
    pub(crate) fn apply(
        &mut self,
        key: Vec<u8>,
        value: Vec<u8>,
        lease: i64,
        rev: u64,
        kv_bytes: Option<Bytes>,
    ) -> Option<crate::storage::KeyState> {
        let prev = self.keys.get(&key).filter(|k| k.is_alive()).cloned();
        let is_new = prev.is_none();
        let rebirth = self.keys.get(&key).map_or(false, |k| k.delete_revision != 0);

        let mut entry = crate::storage::KeyState {
            value: std::sync::Arc::from(value.into_boxed_slice()),
            mod_revision: rev,
            create_revision: prev.as_ref().map(|k| k.create_revision).unwrap_or(rev),
            version: prev.as_ref().map(|k| k.version + 1).unwrap_or(1),
            lease,
            delete_revision: 0,
            rebirth,
            kv_bytes: Bytes::new(),
        };
        entry.kv_bytes = kv_bytes.unwrap_or_else(|| make_kv_bytes(&key, &entry));
        let event_key = Bytes::from(key.clone());
        self.keys.insert(key, entry.clone());
        if is_new {
            self.key_count += 1;
        }

        let event = WatchEvent {
            revision: rev,
            event_type: mvccpb::event::EventType::Put,
            key: event_key,
            kv_bytes: entry.kv_bytes.clone(),
            prev_kv_bytes: prev
                .as_ref()
                .map(|p| p.kv_bytes.clone())
                .unwrap_or_default(),
        };
        self.notify_watchers(event);

        prev
    }

    pub(crate) fn apply_delete(&mut self, key: Vec<u8>, rev: u64) -> Option<crate::storage::KeyState> {
        let entry = self.keys.get_mut(&key)?;
        if entry.delete_revision != 0 {
            return None;
        }
        let prev = entry.clone();

        entry.delete_revision = rev;
        self.key_count = self.key_count.saturating_sub(1);

        // mvccpb.Event contract (proto/kv.proto): "A DELETE/EXPIRE event contains
        // the deleted key with its modification revision set to the revision of
        // deletion." Reusing the previous KeyValue blob would carry the *last
        // write's* mod_revision, so every DELETE event would advertise a
        // revision that goes backwards — clients that key their caches off
        // `kv.mod_revision` (kube-apiserver's watch cache does) would skip it.
        //
        // `mod_revision` is a fixed-width overlong varint, so the 10 bytes are
        // patched in place instead of re-encoding the blob.
        let tombstone_bytes = with_mod_revision(&prev.kv_bytes, rev);

        let event = WatchEvent {
            revision: rev,
            event_type: mvccpb::event::EventType::Delete,
            key: Bytes::from(key),
            kv_bytes: tombstone_bytes,
            prev_kv_bytes: prev.kv_bytes.clone(),
        };
        self.notify_watchers(event);

        Some(prev)
    }

    pub(crate) fn delete_keys_for_lease(&mut self, id: i64) -> Result<(), Status> {
        let keys_to_delete: Vec<Vec<u8>> = self
            .keys
            .iter()
            .filter(|(_, ks)| ks.lease == id && ks.is_alive())
            .map(|(k, _)| k.clone())
            .collect();
        if keys_to_delete.is_empty() {
            return Ok(());
        }
        let rev = self.alloc_revision();
        let mut records = Vec::with_capacity(keys_to_delete.len());
        for key in &keys_to_delete {
            let prev = self.keys.get(key).filter(|k| k.is_alive()).cloned();
            if let Some(p) = prev {
                let mut flags = wal::DELETED;
                if p.lease != 0 {
                    flags |= wal::HAS_LEASE;
                }
                records.push(wal::KvWalRecord::new(
                    flags, key, &p.value,
                    p.create_revision as i64, rev as i64, p.version, p.lease,
                ));
            }
        }
        if let Err(e) = self.wal.append_kv_batch(&records) {
            tracing::error!("WAL batch append failed on lease key deletion: {e}");
            return Err(Status::new(
                tonic::Code::Internal,
                "WAL write failed during lease key deletion",
            ));
        }
        for key in &keys_to_delete {
            self.apply_delete(key.clone(), rev);
        }
        Ok(())
    }
}

/// Apply a KvWalRecord during startup replay. Rebuilds in-memory state
/// and fires watch events for any watchers caught up during replay.
pub(crate) fn apply_record(state: &mut StoreState, rec: &wal::KvWalRecord) {
    let deleted = (rec.flags & wal::DELETED) != 0;
    let rev = rec.mod_revision().unwrap_or(0) as u64;

    if deleted {
        let key = rec.key().unwrap_or_default().to_vec();
        state.apply_delete(key, rev);
    } else if let Ok(kv) = mvccpb::KeyValue::decode(&rec.kv_bytes[..]) {
        state.apply(kv.key, kv.value, kv.lease, rev, Some(Bytes::from(rec.kv_bytes.clone())));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::etcdserverpb;
    use crate::storage::{resolve_range, Store, WatchRegistration};
    use std::sync::Arc;
    use tokio::sync::mpsc;

    fn temp_wal() -> String {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        format!(
            "/tmp/rudurru_apply_test_{}_{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        )
    }

    /// mvccpb.Event contract: a DELETE event carries the deleted key with
    /// `mod_revision` set to the revision of the deletion — not the revision of
    /// the last write. A regressing mod_revision makes revision-tracking clients
    /// (kube-apiserver's watch cache) skip the event.
    #[tokio::test]
    async fn delete_event_reports_deletion_revision() {
        let path = temp_wal();
        let _ = std::fs::remove_file(&path);
        let store = Arc::new(Store::open(&path).await.unwrap());

        store
            .put(etcdserverpb::PutRequest {
                key: b"k".to_vec(),
                value: b"v".to_vec(),
                ..Default::default()
            })
            .await
            .unwrap();
        let put_rev = store.current_revision();

        let (tx, mut rx) = mpsc::unbounded_channel();
        {
            let mut state = store.state.write();
            state.register_watcher(WatchRegistration {
                key: b"k".to_vec(),
                range_end: Vec::new(),
                start_revision: 0,
                sender: tx,
                watch_id: 1,
                progress_notify: false,
                filters: Vec::new(),
                prev_kv: true,
                bound: resolve_range(b"k", b""),
            });
        }

        store
            .delete_range(etcdserverpb::DeleteRangeRequest {
                key: b"k".to_vec(),
                ..Default::default()
            })
            .await
            .unwrap();

        let event = rx.try_recv().expect("watcher must receive the delete event");
        let kv = mvccpb::KeyValue::decode(event.kv_bytes.as_ref()).unwrap();
        let prev_kv = mvccpb::KeyValue::decode(event.prev_kv_bytes.as_ref()).unwrap();

        assert_eq!(event.event_type, mvccpb::event::EventType::Delete);
        assert_eq!(kv.key, b"k", "DELETE kv must carry the deleted key");
        assert_eq!(
            kv.mod_revision, event.revision as i64,
            "DELETE kv.mod_revision must be the revision of deletion"
        );
        assert_ne!(
            kv.mod_revision, put_rev as i64,
            "DELETE kv.mod_revision must not be the previous write's revision"
        );
        assert_eq!(
            prev_kv.value, b"v",
            "prev_kv must still carry the last value"
        );

        let _ = std::fs::remove_file(&path);
    }
}
