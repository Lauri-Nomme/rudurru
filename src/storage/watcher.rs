use crate::storage::matches_range;
use crate::storage::StoreState;
use crate::storage::WatchEvent;
use crate::storage::WatchRegistration;
use prost::bytes::Bytes;

impl StoreState {
    pub(crate) fn register_watcher(&mut self, reg: WatchRegistration) -> i64 {
        let watch_id = reg.watch_id;
        self.watchers.push(reg);
        watch_id
    }

    pub(crate) fn cancel_watcher(&mut self, watch_id: i64) -> bool {
        let len_before = self.watchers.len();
        self.watchers.retain(|w| w.watch_id != watch_id);
        len_before != self.watchers.len()
    }

    pub(crate) fn notify_watchers(&mut self, event: WatchEvent) {
        for i in 0..self.watchers.len() {
            let watcher = &self.watchers[i];
            if !matches_range(watcher.bound.to_ref(), &event.key) {
                continue;
            }

            if event.revision < watcher.start_revision {
                continue;
            }

            let mut should_send = true;
            for &filter in &watcher.filters {
                match filter {
                    0 if event.event_type == crate::proto::mvccpb::event::EventType::Put => {
                        should_send = false;
                        break;
                    }
                    1 if event.event_type == crate::proto::mvccpb::event::EventType::Delete => {
                        should_send = false;
                        break;
                    }
                    _ => {}
                }
            }
            if !should_send {
                continue;
            }

            let mut event = event.clone();
            if !watcher.prev_kv {
                event.prev_kv_bytes = Bytes::new();
            }
            if watcher.sender.send(event).is_err() {
                tracing::warn!(watch_id = watcher.watch_id, "watcher_send_failed");
            }
        }
    }
}
