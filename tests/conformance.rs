//! Differential conformance checks against a real etcd.
//!
//! Every check prints one canonical line:
//!
//! ```text
//! CHECK|<name>|<value>
//! ```
//!
//! `scripts/conformance.sh` runs this file against a reference etcd and against
//! rudurru on the same operations, diffs the two outputs per check name, and
//! fails on deviations not listed in `tests/conformance/known-deviations.txt`.
//!
//! Values must be canonical (no absolute revisions, timings, ids or hashes), or
//! the diff reports noise instead of protocol differences.
//!
//! The whole suite is one `#[ignore]`d test: it is destructive on purpose (the
//! last check grants a lease with a negative TTL), so `cargo test` must not pick
//! it up. Run it through the script:
//!
//! ```text
//! ETCD_ENDPOINT=http://127.0.0.1:12399 cargo test --test conformance -- --ignored --nocapture
//! ```
use etcd_client::*;
use std::time::Duration;

fn emit(name: &str, value: impl std::fmt::Display) {
    println!("CHECK|{name}|{value}");
}

/// `code` for a gRPC error, or `ok`.
fn code<T>(r: &Result<T, Error>) -> String {
    match r {
        Ok(_) => "ok".to_string(),
        Err(Error::GRpcStatus(s)) => format!("code={:?}", s.code()),
        Err(e) => format!("error={e:?}"),
    }
}

fn code_msg<T>(r: &Result<T, Error>) -> String {
    match r {
        Ok(_) => "ok".to_string(),
        Err(Error::GRpcStatus(s)) => format!("code={:?} msg={}", s.code(), s.message()),
        Err(e) => format!("error={e:?}"),
    }
}

async fn endpoint_client() -> Client {
    let ep = std::env::var("ETCD_ENDPOINT").expect(
        "ETCD_ENDPOINT must be set explicitly (never default to localhost:2379, that is production)",
    );
    Client::connect([&ep], None)
        .await
        .unwrap_or_else(|e| panic!("connect to {ep}: {e}"))
}

async fn rev(c: &mut Client) -> i64 {
    c.get("__conformance_none__", None)
        .await
        .unwrap()
        .header()
        .unwrap()
        .revision()
}

fn uniq(tag: &str) -> String {
    format!("conformance/{tag}/{}", rand::random::<u32>())
}

#[tokio::test]
#[ignore = "differential harness; run via scripts/conformance.sh (destructive: last check kills the server if broken)"]
async fn conformance() {
    let mut c = endpoint_client().await;

    // ── put / get ─────────────────────────────────────────────────────
    let k = uniq("kv");
    c.put(k.as_str(), "v1", None).await.unwrap();
    let g = c.get(k.as_str(), None).await.unwrap();
    emit("put.basic", format!("kvs={}", g.kvs().len()));

    let p = c
        .put(k.as_str(), "v2", Some(PutOptions::new().with_prev_key()))
        .await
        .unwrap();
    emit(
        "put.prev_kv",
        String::from_utf8_lossy(p.prev_key().map(|kv| kv.value()).unwrap_or_default()).to_string(),
    );

    // ignore_value with a non-empty value is itself invalid
    emit(
        "put.value_provided_with_ignore_value",
        code_msg(&c.put(k.as_str(), "nonempty", Some(PutOptions::new().with_ignore_value())).await),
    );
    emit(
        "put.lease_provided_with_ignore_lease",
        code_msg(&c.put(k.as_str(), "v", Some(PutOptions::new().with_ignore_lease().with_lease(1))).await),
    );
    c.put(k.as_str(), "", Some(PutOptions::new().with_ignore_value())).await.unwrap();
    let v = c.get(k.as_str(), None).await.unwrap().kvs()[0].value().to_vec();
    emit(
        "put.ignore_value_existing",
        format!("value={}", String::from_utf8_lossy(&v)),
    );

    let missing = uniq("missing");
    emit(
        "put.ignore_value_missing",
        code_msg(&c.put(missing.as_str(), "", Some(PutOptions::new().with_ignore_value())).await),
    );
    emit(
        "put.ignore_lease_missing",
        code_msg(&c.put(missing.as_str(), "v", Some(PutOptions::new().with_ignore_lease())).await),
    );
    emit("put.empty_key", code_msg(&c.put("", "v", None).await));
    emit(
        "put.unknown_lease",
        code_msg(&c.put(uniq("lease").as_str(), "v", Some(PutOptions::new().with_lease(987654321))).await),
    );

    // ── range ─────────────────────────────────────────────────────────
    let pfx = uniq("range");
    for (k, v) in [("a", "3"), ("b", "1"), ("c", "2")] {
        c.put(format!("{pfx}/{k}"), v, None).await.unwrap();
    }
    let get = |c: &mut Client, opts: GetOptions| {
        let p = format!("{pfx}/");
        let mut c = c.clone();
        async move { c.get(p.as_str(), Some(opts)).await }
    };

    let r = get(&mut c, GetOptions::new().with_prefix()).await.unwrap();
    emit("range.prefix", format!("kvs={}", r.kvs().len()));

    let r = get(&mut c, GetOptions::new().with_prefix().with_count_only()).await.unwrap();
    emit("range.count_only", format!("count={} kvs={}", r.count(), r.kvs().len()));

    let r = get(&mut c, GetOptions::new().with_prefix().with_limit(1)).await.unwrap();
    emit("range.limit", format!("kvs={} more={}", r.kvs().len(), r.more()));

    let r = get(&mut c, GetOptions::new().with_prefix().with_keys_only()).await.unwrap();
    emit(
        "range.keys_only",
        format!("values_empty={}", r.kvs().iter().all(|kv| kv.value().is_empty())),
    );

    let r = get(&mut c, GetOptions::new().with_prefix().with_sort(SortTarget::Value, SortOrder::Descend))
        .await
        .unwrap();
    emit(
        "range.sort_desc_value",
        r.kvs()
            .iter()
            .map(|kv| String::from_utf8_lossy(kv.value()).to_string())
            .collect::<Vec<_>>()
            .join(","),
    );

    let r = get(&mut c, GetOptions::new().with_prefix().with_min_mod_revision(i64::MAX)).await.unwrap();
    emit("range.min_mod_revision", format!("kvs={}", r.kvs().len()));

    // historical read: capture a revision between two writes
    let hk = uniq("hist");
    c.put(hk.as_str(), "old", None).await.unwrap();
    let hist_rev = rev(&mut c).await;
    c.put(hk.as_str(), "new", None).await.unwrap();
    let old = c.get(hk.as_str(), Some(GetOptions::new().with_revision(hist_rev))).await.unwrap();
    emit(
        "range.historical",
        String::from_utf8_lossy(old.kvs().first().map(|kv| kv.value()).unwrap_or_default()).to_string(),
    );

    let now = rev(&mut c).await;
    emit(
        "range.future_rev",
        code_msg(&c.get(hk.as_str(), Some(GetOptions::new().with_revision(now + 5))).await),
    );

    // compaction watermark (read below it)
    c.compact(now, None).await.unwrap();
    emit(
        "range.compacted",
        code_msg(&c.get(hk.as_str(), Some(GetOptions::new().with_revision(now - 1))).await),
    );

    emit("range.empty_key", code_msg(&c.get("", None).await));

    // ── delete ────────────────────────────────────────────────────────
    let dk = uniq("del");
    c.put(dk.as_str(), "dv", None).await.unwrap();
    emit("delete.empty_key", code_msg(&c.delete("", None).await));
    let d = c.delete(dk.as_str(), Some(DeleteOptions::new().with_prev_key())).await.unwrap();
    emit(
        "delete.result",
        format!(
            "deleted={} prev={}",
            d.deleted(),
            String::from_utf8_lossy(d.prev_kvs().first().map(|kv| kv.value()).unwrap_or_default())
        ),
    );

    // ── txn ───────────────────────────────────────────────────────────
    let tp = uniq("txn");
    let before = rev(&mut c).await;
    let txn = Txn::new().and_then(vec![
        TxnOp::put(format!("{tp}/a"), "1", None),
        TxnOp::put(format!("{tp}/b"), "2", None),
        TxnOp::put(format!("{tp}/c"), "3", None),
    ]);
    c.txn(txn).await.unwrap();
    let after = rev(&mut c).await;
    emit("txn.revisions_for_3_writes", format!("consumed={}", after - before));

    let pk = uniq("txn-atomic");
    let before2 = rev(&mut c).await;
    let txn = Txn::new().and_then(vec![
        TxnOp::put(format!("{pk}/a"), "1", None),
        TxnOp::put(format!("{pk}/b"), "2", None),
    ]);
    c.txn(txn).await.unwrap();
    let mid = c
        .get(format!("{pk}/").as_str(), Some(GetOptions::new().with_prefix().with_revision(before2 + 1)))
        .await;
    emit(
        "txn.atomic_mid_revision",
        match mid {
            Ok(r) => format!("kvs_at_mid={}", r.kvs().len()),
            Err(_) => "errored".to_string(),
        },
    );

    let ck = uniq("txn-cmp");
    let txn = Txn::new()
        .when(vec![Compare::value(ck.as_str(), CompareOp::Equal, "nope")])
        .and_then(vec![TxnOp::put(ck.as_str(), "written", None)]);
    let r = c.txn(txn).await.unwrap();
    let exists = !c.get(ck.as_str(), None).await.unwrap().kvs().is_empty();
    emit(
        "txn.compare_failure",
        format!("succeeded={} written={exists}", r.succeeded()),
    );

    let ok = uniq("txn-nested-outer");
    let ik = uniq("txn-nested-inner");
    let nested = TxnOp::txn(Txn::new().and_then(vec![TxnOp::put(ik.as_str(), "v", None)]));
    let txn = Txn::new().and_then(vec![TxnOp::put(ok.as_str(), "v", None), nested]);
    c.txn(txn).await.unwrap();
    emit(
        "txn.nested",
        format!("inner_written={}", !c.get(ik.as_str(), None).await.unwrap().kvs().is_empty()),
    );

    // reads inside a txn see the txn's own earlier writes
    let own = uniq("txn-own");
    c.put(own.as_str(), "pre", None).await.unwrap();
    let txn = Txn::new().and_then(vec![
        TxnOp::put(own.as_str(), "mid", None),
        TxnOp::get(own.as_str(), None),
    ]);
    let r = c.txn(txn).await.unwrap();
    let seen = match &r.op_responses()[1] {
        TxnOpResponse::Get(g) => g
            .kvs()
            .first()
            .map(|kv| String::from_utf8_lossy(kv.value()).to_string())
            .unwrap_or_default(),
        _ => "<not a get>".to_string(),
    };
    emit("txn.read_own_writes", format!("value={seen}"));

    // two writes on the same key are rejected
    let dup = uniq("txn-dup");
    let txn = Txn::new().and_then(vec![
        TxnOp::put(dup.as_str(), "1", None),
        TxnOp::put(dup.as_str(), "2", None),
    ]);
    emit("txn.duplicate_write_keys", code_msg(&c.txn(txn).await));

    // A dynamic validation error in the middle of a txn (second op reuses a
    // value from a key that does not exist): etcd rolls the whole txn back, so
    // the first write must not survive.
    let partial = uniq("txn-dyn-partial");
    let absent = uniq("txn-dyn-absent");
    let txn = Txn::new().and_then(vec![
        TxnOp::put(partial.as_str(), "written", None),
        TxnOp::put(
            absent.as_str(),
            "",
            Some(PutOptions::new().with_ignore_value()),
        ),
    ]);
    let r = c.txn(txn).await;
    let survived = !c.get(partial.as_str(), None).await.unwrap().kvs().is_empty();
    emit(
        "txn.dynamic_error_atomicity",
        format!("{} first_write_survived={survived}", code_msg(&r)),
    );

    // ── watch ─────────────────────────────────────────────────────────
    let wk = uniq("watch");
    let mut w = c
        .watch(wk.as_str(), Some(WatchOptions::new().with_prev_key()))
        .await
        .unwrap();
    let put = c.put(wk.as_str(), "first", None).await.unwrap();
    let put_rev = put.header().unwrap().revision();
    let ev = next_event(&mut w, EventType::Put).await;
    emit(
        "watch.put_event",
        match ev {
            Some(ev) => format!(
                "value={} modrev_is_write={}",
                String::from_utf8_lossy(ev.kv().map(|kv| kv.value()).unwrap_or_default()),
                ev.kv().map(|kv| kv.mod_revision()) == Some(put_rev)
            ),
            None => "no_event".to_string(),
        },
    );

    let del = c.delete(wk.as_str(), None).await.unwrap();
    let del_rev = del.header().unwrap().revision();
    let ev = next_event(&mut w, EventType::Delete).await;
    emit(
        "watch.delete_event_mod_revision",
        match ev {
            Some(ev) => format!(
                "kv_modrev_is_delete_rev={} prev_present={}",
                ev.kv().map(|kv| kv.mod_revision()) == Some(del_rev),
                ev.prev_kv().is_some()
            ),
            None => "no_event".to_string(),
        },
    );

    // live delivery for a watch created without a start revision, with the write
    // issued immediately (the client does not wait for `created`)
    let nk = uniq("watch-now");
    let history_ok = c.put(nk.as_str(), "history", None).await.is_ok();
    let mut w2 = c.watch(nk.as_str(), None).await.unwrap();
    c.put(nk.as_str(), "live", None).await.unwrap();
    let ev = next_event(&mut w2, EventType::Put).await;
    emit(
        "watch.now_delivers_batched_write",
        format!(
            "history_put_ok={history_ok} delivered={} value={}",
            ev.is_some(),
            String::from_utf8_lossy(ev.as_ref().and_then(|e| e.kv()).map(|kv| kv.value()).unwrap_or_default())
        ),
    );

    // replay from an explicit revision
    let rk = uniq("watch-replay");
    c.put(rk.as_str(), "r1", None).await.unwrap();
    let rrev = rev(&mut c).await;
    c.put(rk.as_str(), "r2", None).await.unwrap();
    let mut w3 = c
        .watch(rk.as_str(), Some(WatchOptions::new().with_start_revision(rrev)))
        .await
        .unwrap();
    let ev = next_event(&mut w3, EventType::Put).await;
    emit(
        "watch.replay_from_revision",
        format!(
            "value={}",
            String::from_utf8_lossy(ev.as_ref().and_then(|e| e.kv()).map(|kv| kv.value()).unwrap_or_default())
        ),
    );

    // ── lease ─────────────────────────────────────────────────────────
    let l = c.lease_grant(60, None).await.unwrap();
    let id = l.id();
    emit("lease.grant_ttl_echo", format!("ttl={}", l.ttl()));

    let l0 = c.lease_grant(0, None).await;
    emit("lease.grant_ttl_zero", format!("ttl={:?}", l0.map(|l| l.ttl()).map_err(|e| code(&Err::<(), _>(e)))));

    let dup = c.lease_grant(60, Some(LeaseGrantOptions::new().with_id(424242))).await;
    let dup2 = c.lease_grant(60, Some(LeaseGrantOptions::new().with_id(424242))).await;
    emit(
        "lease.grant_duplicate_id",
        format!("first={} second={}", code(&dup), code(&dup2)),
    );

    emit("lease.revoke_missing", code_msg(&c.lease_revoke(999_999_999).await));

    let ak = uniq("lease-key");
    c.put(ak.as_str(), "v", Some(PutOptions::new().with_lease(id))).await.unwrap();
    let ttl = c
        .lease_time_to_live(id, Some(LeaseTimeToLiveOptions::new().with_keys()))
        .await
        .unwrap();
    emit(
        "lease.time_to_live_keys",
        format!("keys={} ttl_positive={}", ttl.keys().len(), ttl.ttl() > 0),
    );

    let (mut keeper, mut stream) = c.lease_keep_alive(id).await.unwrap();
    let _ = keeper.keep_alive().await;
    let ka = tokio::time::timeout(Duration::from_secs(2), stream.message()).await;
    emit(
        "lease.keepalive",
        match ka {
            Ok(Ok(Some(r))) => format!("ttl={}", r.ttl()),
            _ => "no_response".to_string(),
        },
    );

    c.lease_revoke(id).await.unwrap();
    emit(
        "lease.revoke_deletes_keys",
        format!("gone={}", c.get(ak.as_str(), None).await.unwrap().kvs().is_empty()),
    );

    // ── maintenance / cluster / auth ──────────────────────────────────
    let st = c.status().await;
    emit(
        "maintenance.status_shape",
        match st {
            Ok(r) => format!("version_nonempty={} raft_index_positive={}", !r.version().is_empty(), r.raft_index() > 0),
            Err(e) => format!("error={e:?}"),
        },
    );
    emit(
        "cluster.member_count",
        match c.member_list().await {
            Ok(r) => format!("{}", r.members().len()),
            Err(e) => format!("error={e:?}"),
        },
    );
    emit("auth.enable", code(&c.auth_enable().await));

    // ── destructive checks (must stay last) ───────────────────────────
    // etcd clamps a non-positive TTL to its minimum; it must never take the
    // server down.
    let neg = c.lease_grant(-5, None).await;
    emit(
        "lease.grant_ttl_negative",
        format!("ttl={:?}", neg.map(|l| l.ttl()).map_err(|e| code(&Err::<(), _>(e)))),
    );

    let alive = tokio::time::timeout(Duration::from_secs(3), c.get("__conformance_none__", None))
        .await
        .map(|r| r.is_ok())
        .unwrap_or(false);
    emit("server.survives_negative_ttl", format!("alive={alive}"));
}

async fn next_event(w: &mut WatchStream, want: EventType) -> Option<Event> {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let resp = w.message().await.ok()??;
            if let Some(ev) = resp.events().iter().find(|e| e.event_type() == want) {
                return Some(ev.clone());
            }
        }
    })
    .await
    .ok()
    .flatten()
}
