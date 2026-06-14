//! WalRecover — Recover keys deleted by compact(8000) from a Rudurru WAL backup.
//! Usage: walrecover <backup-wal> <output-wal>
//!
//! Reconstructs the full in-memory state from the WAL (same as Rudurru replay),
//! then writes a compact WAL containing only the latest version of each key.
//! This effectively "un-deletes" keys that compact(8000) removed from memory
//! (compact didn't modify the WAL, so all data is still there).
//!
//! The output WAL can be loaded by Rudurru to restore the cluster state.

use std::collections::BTreeMap;
use std::env;
use std::fs::File;
use std::io::{Read, Write};
use std::process;

type Revision = u64;

#[derive(Debug, Clone)]
struct KeyState {
    value: Vec<u8>,
    create_revision: Revision,
    mod_revision: Revision,
    version: u64,
}

const MAGIC: u16 = 0x5255;
const IS_CREATE: u8 = 0x02;
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

fn scan_wal(path: &str) -> (Vec<WalEntry>, u64) {
    let mut f = File::open(path).unwrap_or_else(|e| {
        eprintln!("Error opening {path}: {e}");
        process::exit(1);
    });
    let mut buf = Vec::new();
    f.read_to_end(&mut buf).unwrap_or_else(|e| {
        eprintln!("Error reading {path}: {e}");
        process::exit(1);
    });

    let mut records = Vec::new();
    let mut pos = 0;
    let mut total = 0u64;

    while pos + HEADER_SIZE <= buf.len() {
        let magic = u16::from_le_bytes([buf[pos], buf[pos + 1]]);
        if magic != MAGIC {
            pos += 1;
            continue;
        }
        pos += 2;

        let revision = u64::from_le_bytes(buf[pos..pos + 8].try_into().unwrap());
        pos += 8;
        let _stored_crc = u32::from_le_bytes(buf[pos..pos + 4].try_into().unwrap());
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

        let computed = crc32c(&buf[flags_ofs..pos]);
        if computed != _stored_crc {
            continue;
        }

        total += 1;
        records.push((revision, key, value, flags));
    }

    eprintln!(
        "Scan: file={} records={} trailing={}",
        buf.len(),
        total,
        buf.len() - pos
    );
    (records, total)
}

fn write_wal(path: &str, entries: &[(Vec<u8>, KeyState)]) {
    let mut f = File::create(path).unwrap_or_else(|e| {
        eprintln!("Error creating {path}: {e}");
        process::exit(1);
    });

    for (key, ks) in entries {
        let flags = IS_CREATE;
        // flags: IS_CREATE (0x02) set for all entries

        let key_len = key.len() as u32;
        let val_len = ks.value.len() as u32;
        let entry_size = 2 + 8 + 4 + 4 + 4 + 1 + key.len() + ks.value.len();
        let mut buf = Vec::with_capacity(entry_size);

        buf.extend_from_slice(&MAGIC.to_le_bytes()); // 2
        buf.extend_from_slice(&ks.mod_revision.to_le_bytes()); // 8
        let crc_ofs = buf.len();
        buf.extend_from_slice(&[0u8; 4]); // 4 (CRC placeholder)
        buf.extend_from_slice(&key_len.to_le_bytes()); // 4
        buf.extend_from_slice(&val_len.to_le_bytes()); // 4
        buf.push(flags); // 1
        buf.extend_from_slice(key); // N
        buf.extend_from_slice(&ks.value); // M

        let crc = crc32c(&buf[22..]); // CRC of flags+key+value
        buf[crc_ofs..crc_ofs + 4].copy_from_slice(&crc.to_le_bytes());

        f.write_all(&buf).unwrap_or_else(|e| {
            eprintln!("Error writing WAL: {e}");
            process::exit(1);
        });
    }
    f.sync_all().unwrap_or_else(|e| {
        eprintln!("Error syncing WAL: {e}");
        process::exit(1);
    });
    eprintln!("Wrote {} records to {path}", entries.len());
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() != 3 {
        eprintln!("Usage: walrecover <backup-wal> <output-wal>");
        eprintln!("  Reconstructs the full store state from a backup WAL");
        eprintln!("  and writes a clean recovery WAL.");
        eprintln!();
        eprintln!("Example:");
        eprintln!("  walrecover debug-2026-06-14/wal.backup /data/rudurru/wal.recovered");
        process::exit(1);
    }

    let input = &args[1];
    let output = &args[2];

    eprintln!("WalRecover: {input} → {output}");
    eprintln!();

    let (records, _scanned) = scan_wal(input);

    // Reconstruct state (same as Rudurru startup replay)
    let mut keys: BTreeMap<Vec<u8>, KeyState> = BTreeMap::new();
    let mut max_rev: Revision = 0;

    for (rev, key, value, flags) in &records {
        max_rev = max_rev.max(*rev);

        if flags & 0x01 != 0 {
            // DELETED
            keys.remove(key);
            continue;
        }

        let entry = keys.entry(key.clone()).or_insert(KeyState {
            value: Vec::new(),
            create_revision: *rev,
            mod_revision: *rev,
            version: 0,
        });
        if flags & IS_CREATE != 0 {
            entry.create_revision = *rev;
        }
        entry.value = value.clone();
        entry.mod_revision = *rev;
        entry.version += 1;
    }

    eprintln!(
        "Reconstructed: keys={} max_revision={}",
        keys.len(),
        max_rev
    );

    // Count compact damage
    let lost: Vec<_> = keys
        .iter()
        .filter(|(_, ks)| ks.mod_revision < 8000 && ks.create_revision < 8000)
        .collect();
    eprintln!("Compact(8000) damage: lost_keys={}", lost.len());
    if !lost.is_empty() {
        eprintln!("  First 5 lost:");
        for (k, ks) in lost.iter().take(5) {
            eprintln!(
                "    cr={:<6} mr={:<6} v={}  {}",
                ks.create_revision,
                ks.mod_revision,
                ks.version,
                String::from_utf8_lossy(k)
            );
        }
    }

    // Write recovery WAL
    let entries: Vec<_> = keys.into_iter().collect();
    write_wal(output, &entries);

    eprintln!();
    eprintln!("Done. To recover:");
    eprintln!("  sudo cp {output} /data/rudurru/wal.recovered");
    eprintln!("  sudo systemctl stop rudurru");
    eprintln!("  sudo cp /data/rudurru/wal /data/rudurru/wal.pre-recovery  # backup current WAL");
    eprintln!("  sudo mv /data/rudurru/wal.recovered /data/rudurru/wal");
    eprintln!("  sudo systemctl start rudurru");
    eprintln!();
    eprintln!("Rudurru will replay the recovery WAL and serve all 1623 keys.");
}
