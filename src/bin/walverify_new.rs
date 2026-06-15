//! WalVerify (new format) — Verify a protobuf-native Rudurru WAL.
//! Usage: walverify_new <wal-path>
//!
//! Reads the WAL using KvWalRecord deserialization, validates every CRC,
//! reconstructs in-memory state, and reports statistics.

use std::collections::BTreeMap;
use std::env;
use std::process;

use rudurru::storage::wal::{KvWalRecord, WalFile, IS_CREATE, DELETED};

fn category(key: &str) -> String {
    if key.starts_with("/bootstrap/") {
        "bootstrap"
    } else if key.starts_with("/registry/namespaces/") {
        "namespace"
    } else if key.starts_with("/registry/pods/") {
        "pod"
    } else if key.starts_with("/registry/deployments/") {
        "deployment"
    } else if key.starts_with("/registry/statefulsets/") {
        "statefulset"
    } else if key.starts_with("/registry/daemonsets/") {
        "daemonset"
    } else if key.starts_with("/registry/services/endpoints/") {
        "endpoint"
    } else if key.starts_with("/registry/services/specs/") {
        "service"
    } else if key.starts_with("/registry/configmaps/") {
        "configmap"
    } else if key.starts_with("/registry/secrets/") {
        "secret"
    } else if key.starts_with("/registry/serviceaccounts/") {
        "serviceaccount"
    } else if key.starts_with("/registry/persistentvolumeclaims/") {
        "pvc"
    } else if key.starts_with("/registry/persistentvolumes/") {
        "pv"
    } else if key.starts_with("/registry/nodes/") || key.starts_with("/registry/minions/") {
        "node"
    } else if key.starts_with("/registry/leases/") {
        "lease"
    } else if key.starts_with("/registry/events/") {
        "event"
    } else if key.starts_with("/registry/rbac/") {
        "rbac"
    } else if key.starts_with("/registry/clusterroles/") {
        "clusterrole"
    } else if key.starts_with("/registry/roles/") {
        "role"
    } else if key.starts_with("/registry/rolebindings/") {
        "rolebinding"
    } else if key.starts_with("/registry/clusterrolebindings/") {
        "clusterrolebinding"
    } else if key.starts_with("/registry/cronjobs/") {
        "cronjob"
    } else if key.starts_with("/registry/apiextensions.k8s.io/") {
        "crd"
    } else if key.starts_with("/registry/masterleases/") {
        "masterlease"
    } else if key.starts_with("/registry/peerserverleases/") {
        "peerserverlease"
    } else if key.starts_with("/registry/ranges/") {
        "range"
    } else if key.starts_with("/registry/") {
        "other_registry"
    } else {
        "other"
    }
    .to_string()
}

struct KeyState {
    #[allow(dead_code)]
    value: Vec<u8>,
    create_revision: i64,
    mod_revision: i64,
    version: i64,
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: walverify_new <wal-path>");
        process::exit(1);
    }

    let path = &args[1];
    eprintln!("WalVerify (new format): {path}");
    eprintln!();

    let mut wal = WalFile::open(path).unwrap_or_else(|e| {
        eprintln!("Failed to open WAL: {e}");
        process::exit(1);
    });

    let records = wal.scan_kv_collect().unwrap_or_else(|e| {
        eprintln!("Failed to scan WAL: {e}");
        process::exit(1);
    });

    let total = records.len();
    let mut crc_errors: u64 = 0;
    let mut kv_errors: u64 = 0;
    let mut max_rev: i64 = 0;
    let mut keys: BTreeMap<Vec<u8>, KeyState> = BTreeMap::new();

    for rec in &records {
        // Verify CRC by re-deserializing (already done by scan_kv_collect, but double-check)
        match KvWalRecord::deserialize(&rec.serialize()) {
            Ok(_) => {}
            Err(_) => crc_errors += 1,
        }

        // Verify key() and mod_revision() accessors
        let key_bytes = match rec.key() {
            Some(k) => k.to_vec(),
            None => {
                kv_errors += 1;
                continue;
            }
        };
        let mod_rev = match rec.mod_revision() {
            Some(r) => r,
            None => {
                kv_errors += 1;
                continue;
            }
        };
        if mod_rev > max_rev {
            max_rev = mod_rev;
        }

        if rec.flags & DELETED != 0 {
            keys.remove(&key_bytes);
        } else {
            let entry = keys.entry(key_bytes).or_insert(KeyState {
                value: Vec::new(),
                create_revision: mod_rev,
                mod_revision: mod_rev,
                version: 0,
            });
            if rec.flags & IS_CREATE != 0 {
                entry.create_revision = mod_rev;
            }
            entry.mod_revision = mod_rev;
            entry.version += 1;
        }
    }

    // Categories
    let mut cats: BTreeMap<String, u64> = BTreeMap::new();
    for k in keys.keys() {
        let key_str = String::from_utf8_lossy(k);
        *cats.entry(category(&key_str)).or_insert(0) += 1;
    }

    println!("WAL: records={} keys={} max_revision={}", total, keys.len(), max_rev);
    if crc_errors > 0 || kv_errors > 0 {
        println!("  ERRORS: crc_mismatch={} kv_access_errors={}", crc_errors, kv_errors);
    } else {
        println!("  INTEGRITY: all CRCs valid, all key/mod_revision accessors OK");
    }
    println!();

    println!("Key categories:");
    for (cat, count) in &cats {
        println!("  {:25} {}", cat, count);
    }
    println!();

    // Expected infrastructure keys
    let expected_prefixes = [
        "/registry/namespaces/kube-system",
        "/registry/namespaces/default",
        "/registry/namespaces/kube-public",
        "/registry/namespaces/kube-node-lease",
        "/registry/serviceaccounts/kube-system/default",
        "/registry/clusterroles/admin",
        "/registry/clusterroles/edit",
        "/registry/clusterroles/view",
    ];

    println!("Infrastructure key check:");
    for prefix in &expected_prefixes {
        let found = keys
            .keys()
            .any(|k| String::from_utf8_lossy(k).starts_with(prefix));
        println!("  {}: {}", if found { "OK" } else { "MISSING" }, prefix);
    }

    // Namespace keys summary
    println!();
    println!("Namespace keys:");
    for (k, ks) in &keys {
        let key_str = String::from_utf8_lossy(k);
        if category(&key_str) == "namespace" {
            let val = String::from_utf8_lossy(&ks.value);
            let name = val
                .split("\"name\":\"")
                .nth(1)
                .and_then(|s| s.split('"').next())
                .unwrap_or("?");
            println!("  revision={:<8} name={}", ks.mod_revision, name);
        }
    }
}
