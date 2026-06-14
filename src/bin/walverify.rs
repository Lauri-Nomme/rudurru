//! WalVerify — Verify a Rudurru WAL reconstruction and check key categories.
//! Usage: walverify <wal-path>
//!
//! Reads the WAL, reconstructs state, and reports:
//!   - Total keys
//!   - Key categories (namespaces, pods, deployments, etc.)
//!   - Expected infrastructure keys check
//!   - Consistency checks

use std::collections::BTreeMap;
use std::env;
use std::fs::File;
use std::io::Read;
use std::process;

type Revision = u64;

#[derive(Debug)]
struct KeyState {
    value: Vec<u8>,
    create_revision: Revision,
    mod_revision: Revision,
    version: u64,
}

const MAGIC: u16 = 0x5255;
const HEADER_SIZE: usize = 23;

fn crc32c(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFFFFFF;
    for &byte in data {
        crc ^= byte as u32;
        for _ in 0..8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ 0x82F63B78;
            } else {
                crc >>= 1;
            }
        }
    }
    crc ^ 0xFFFFFFFF
}

type WalEntry = (Revision, Vec<u8>, Vec<u8>, u8);

fn scan_wal(path: &str) -> (Vec<WalEntry>, u64, u64) {
    let mut f = File::open(path).unwrap_or_else(|e| {
        eprintln!("Error: {e}");
        process::exit(1);
    });
    let mut buf = Vec::new();
    f.read_to_end(&mut buf).unwrap_or_else(|e| {
        eprintln!("Error: {e}");
        process::exit(1);
    });

    let mut records = Vec::new();
    let mut pos = 0;
    let mut crc_errors = 0u64;
    let mut total = 0u64;

    while pos + HEADER_SIZE <= buf.len() {
        let record_start = pos;
        let magic = u16::from_le_bytes([buf[pos], buf[pos + 1]]);
        if magic != MAGIC {
            pos += 1;
            continue;
        }
        pos += 2;
        let revision = u64::from_le_bytes(buf[pos..pos + 8].try_into().unwrap());
        pos += 8;
        let stored_crc = u32::from_le_bytes(buf[pos..pos + 4].try_into().unwrap());
        pos += 4;
        let key_len = u32::from_le_bytes(buf[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        let val_len = u32::from_le_bytes(buf[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        let flags = buf[pos];
        let flags_ofs = pos;
        pos += 1;
        if pos + key_len + val_len > buf.len() {
            break;
        }
        let key = buf[pos..pos + key_len].to_vec();
        pos += key_len;
        let value = buf[pos..pos + val_len].to_vec();
        pos += val_len;
        if flags & 0x04 != 0 {
            if pos + 8 > buf.len() {
                break;
            }
            pos += 8;
        }
        if crc32c(&buf[flags_ofs..pos]) != stored_crc {
            crc_errors += 1;
            pos = record_start + 1;
            continue;
        }
        total += 1;
        records.push((revision, key, value, flags));
    }
    (records, total, crc_errors)
}

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

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: walverify <wal-path>");
        process::exit(1);
    }

    let path = &args[1];
    eprintln!("WalVerify: {path}");
    eprintln!();

    let (records, total, crc_errors) = scan_wal(path);

    let mut keys: BTreeMap<Vec<u8>, KeyState> = BTreeMap::new();
    let mut max_rev = 0u64;

    for (rev, key, value, flags) in &records {
        max_rev = max_rev.max(*rev);
        if flags & 0x01 != 0 {
            keys.remove(key);
            continue;
        }
        let entry = keys.entry(key.clone()).or_insert(KeyState {
            value: Vec::new(),
            create_revision: *rev,
            mod_revision: *rev,
            version: 0,
        });
        if flags & 0x02 != 0 {
            entry.create_revision = *rev;
        }
        entry.value = value.clone();
        entry.mod_revision = *rev;
        entry.version += 1;
    }

    // Categories
    let mut cats: BTreeMap<String, u64> = BTreeMap::new();
    for k in keys.keys() {
        let key_str = String::from_utf8_lossy(k);
        *cats.entry(category(&key_str)).or_insert(0) += 1;
    }

    println!(
        "WAL: records={} crc_errors={} keys={} max_revision={}",
        total,
        crc_errors,
        keys.len(),
        max_rev
    );
    println!();

    println!("Key categories:");
    for (cat, count) in &cats {
        println!("  {:25} {}", cat, count);
    }
    println!();

    // Check expected infrastructure
    let expected_prefixes = [
        "/registry/namespaces/kube-system",
        "/registry/namespaces/default",
        "/registry/namespaces/kube-public",
        "/registry/namespaces/kube-node-lease",
        "/registry/serviceaccounts/kube-system/default",
        "/registry/nodes/changwang",
        "/registry/nodes/precision",
        "/registry/clusterroles/admin",
        "/registry/clusterroles/edit",
        "/registry/clusterroles/view",
        "/registry/secrets/cattle-system/serving-cert",
    ];

    println!("Infrastructure key check:");
    for prefix in &expected_prefixes {
        let found = keys
            .keys()
            .any(|k| String::from_utf8_lossy(k).starts_with(prefix));
        println!("  {}: {}", if found { "OK" } else { "MISSING" }, prefix);
    }

    // Recovered keys with JSON value dump for namespace keys
    println!();
    println!("Namespace keys:");
    for (k, ks) in &keys {
        let key_str = String::from_utf8_lossy(k);
        if category(&key_str) == "namespace" {
            let val = String::from_utf8_lossy(&ks.value);
            // Extract the "name" field from the JSON value
            let name = val
                .split("\"name\":\"")
                .nth(1)
                .and_then(|s| s.split('"').next())
                .unwrap_or("?");
            println!("  revision={:<8} name={}", ks.mod_revision, name);
        }
    }
}
