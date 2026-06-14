//! WalDoctor — Read and validate a Rudurru WAL file.
//! Usage: waldoctor <wal-path> [--dump]
//!
//! Reconstructs the in-memory store state by replaying the WAL
//! (same as Rudurru does at startup) and reports:
//!   - Total records scanned
//!   - Unique keys in final state
//!   - Key sample dump (--dump for JSONL)
//!   - Compact revision
//!   - CRC errors vs valid records
//!   - Keys deleted by compact(8000)
//!
//! Since compact(8000) only removed keys from memory (not the WAL),
//! a full replay naturally recovers all data. This tool confirms
//! the WAL is intact and shows the reconstructed state.

use std::collections::BTreeMap;
use std::env;
use std::fs::File;
use std::io::Read;

type Revision = u64;

#[derive(Debug, Clone)]
struct KeyState {
    value: Vec<u8>,
    create_revision: Revision,
    mod_revision: Revision,
    version: u64,
}

#[derive(Debug)]
#[allow(dead_code)]
struct WalRecord {
    revision: u64,
    key: Vec<u8>,
    value: Vec<u8>,
    flags: u8,
    lease_id: Option<i64>,
}

const MAGIC: u16 = 0x5255;
const DELETED: u8 = 0x01;
const IS_CREATE: u8 = 0x02;
const HAS_LEASE: u8 = 0x04;
const HEADER_SIZE: usize = 23; // magic(2) + revision(8) + crc32(4) + key_len(4) + val_len(4) + flags(1)

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

fn scan_wal(path: &str) -> Result<(Vec<WalRecord>, u64, u64), String> {
    let mut f = File::open(path).map_err(|e| format!("open: {e}"))?;
    let mut buf = Vec::new();
    f.read_to_end(&mut buf).map_err(|e| format!("read: {e}"))?;

    let mut records = Vec::new();
    let mut pos = 0;
    let mut crc_errors = 0u64;
    let mut total = 0u64;

    while pos + HEADER_SIZE <= buf.len() {
        let record_start = pos;

        // magic
        let magic = u16::from_le_bytes([buf[pos], buf[pos + 1]]);
        pos += 2;
        if magic != MAGIC {
            // Try next byte in case of misalignment
            pos = record_start + 1;
            continue;
        }

        // revision
        let revision = u64::from_le_bytes(buf[pos..pos + 8].try_into().unwrap());
        pos += 8;

        // stored crc
        let stored_crc = u32::from_le_bytes(buf[pos..pos + 4].try_into().unwrap());
        pos += 4;

        // key_len
        let key_len = u32::from_le_bytes(buf[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;

        // val_len
        let val_len = u32::from_le_bytes(buf[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;

        // flags
        let flags = buf[pos];
        let flags_ofs = pos;
        pos += 1;

        // payload
        if pos + key_len + val_len > buf.len() {
            break;
        }
        let key = buf[pos..pos + key_len].to_vec();
        pos += key_len;
        let value = buf[pos..pos + val_len].to_vec();
        pos += val_len;

        let lease_id = if flags & HAS_LEASE != 0 {
            if pos + 8 > buf.len() {
                break;
            }
            let lid = i64::from_le_bytes(buf[pos..pos + 8].try_into().unwrap());
            pos += 8;
            Some(lid)
        } else {
            None
        };

        // CRC covers flags byte through end of payload (including lease_id)
        let computed = crc32c(&buf[flags_ofs..pos]);
        if computed != stored_crc {
            crc_errors += 1;
            pos = record_start + 1;
            continue;
        }

        total += 1;
        records.push(WalRecord {
            revision,
            key,
            value,
            flags,
            lease_id,
        });
    }

    eprintln!(
        "WAL: file_size={} records={} crc_errors={} trailing={}",
        buf.len(),
        total,
        crc_errors,
        buf.len() - pos
    );
    Ok((records, crc_errors, total))
}

fn reconstruct_state(records: &[WalRecord]) -> (BTreeMap<Vec<u8>, KeyState>, Vec<Vec<u8>>) {
    let mut keys: BTreeMap<Vec<u8>, KeyState> = BTreeMap::new();

    for rec in records {
        if rec.flags & DELETED != 0 {
            keys.remove(&rec.key);
            continue;
        }

        let entry = keys.entry(rec.key.clone()).or_insert(KeyState {
            value: Vec::new(),
            create_revision: rec.revision,
            mod_revision: rec.revision,
            version: 0,
        });

        // IS_CREATE flag means this is a creation (keep create_revision)
        if rec.flags & IS_CREATE != 0 {
            entry.create_revision = rec.revision;
        }

        entry.value = rec.value.clone();
        entry.mod_revision = rec.revision;
        entry.version += 1;
    }

    let compact_rev: Revision = 8000;
    let lost: Vec<_> = keys
        .iter()
        .filter(|(_, ks)| ks.mod_revision < compact_rev && ks.create_revision < compact_rev)
        .map(|(k, _)| k.clone())
        .collect();

    eprintln!();
    eprintln!(
        "Reconstructed state: total_keys={} lost_by_compact(8000)={}",
        keys.len(),
        lost.len()
    );
    eprintln!();

    (keys, lost)
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 || args.len() > 3 {
        eprintln!("Usage: waldoctor <wal-path> [--dump]");
        eprintln!("  Reads and validates a Rudurru WAL file");
        eprintln!("  --dump  prints all reconstructed keys as JSONL");
        return;
    }

    let path = &args[1];
    let do_dump = args.get(2).map(|s| s == "--dump").unwrap_or(false);

    let (records, crc_errors, total) = match scan_wal(path) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("Error: {e}");
            return;
        }
    };

    let (state, lost_keys) = reconstruct_state(&records);

    println!(
        "RESULT records={} crc_errors={} keys={} lost={}",
        total,
        crc_errors,
        state.len(),
        lost_keys.len()
    );

    if do_dump {
        for (k, ks) in &state {
            let val_sample = String::from_utf8_lossy(&ks.value[..ks.value.len().min(100)]);
            println!(
                "{{\"key\":{}, \"cr\":{}, \"mr\":{}, \"v\":{}, \"val\":{}}}",
                serde_json::to_string(&String::from_utf8_lossy(k)).unwrap(),
                ks.create_revision,
                ks.mod_revision,
                ks.version,
                serde_json::to_string(&val_sample).unwrap()
            );
        }
    }
}
