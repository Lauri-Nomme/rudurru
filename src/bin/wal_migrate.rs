use std::collections::BTreeMap;
use std::io::{self, Read};
use std::path::Path;

use rudurru::storage::wal;

fn read_old_records(path: &Path) -> io::Result<Vec<wal::WalRecord>> {
    let mut file = std::fs::File::open(path)?;
    let mut buf = Vec::new();
    file.read_to_end(&mut buf)?;
    let mut records = Vec::new();
    let mut ofs = 0;
    while ofs < buf.len() {
        match wal::WalRecord::deserialize(&buf[ofs..]) {
            Ok((rec, consumed)) => {
                records.push(rec);
                ofs += consumed;
            }
            Err(e) => {
                eprintln!("error at offset {ofs}: {e}");
                break;
            }
        }
    }
    Ok(records)
}

fn migrate(
    _old_path: &Path,
    new_path: &Path,
    records: &[wal::WalRecord],
) -> io::Result<()> {
    let mut wal = wal::WalFile::open(new_path)?;
    let mut state: BTreeMap<Vec<u8>, (u64, i64, Vec<u8>, i64)> = BTreeMap::new();
    let total = records.len();

    for (i, rec) in records.iter().enumerate() {
        if (i + 1) % 10000 == 0 || i == 0 || i == total - 1 {
            eprintln!("migrating record {}/{} (rev {})", i + 1, total, rec.revision);
        }

        let deleted = (rec.flags & wal::DELETED) != 0;
        let has_lease = (rec.flags & wal::HAS_LEASE) != 0;
        let lease = if has_lease { rec.lease_id.unwrap_or(0) } else { 0 };

        if deleted {
            if let Some((create_revision, version, ref value, prev_lease)) =
                state.remove(&rec.key)
            {
                let flags = wal::DELETED | if prev_lease != 0 { wal::HAS_LEASE } else { 0 };
                let kv = wal::KvWalRecord::new(
                    flags,
                    &rec.key,
                    value,
                    create_revision as i64,
                    rec.revision as i64,
                    version,
                    prev_lease,
                );
                wal.append_kv(&kv)?;
            } else {
                let kv = wal::KvWalRecord::new(
                    wal::DELETED,
                    &rec.key,
                    b"",
                    0,
                    rec.revision as i64,
                    0,
                    0,
                );
                wal.append_kv(&kv)?;
            }
        } else {
            let (create_revision, version) = match state.get(&rec.key) {
                Some((cr, v, _, _)) => (*cr, *v + 1),
                None => (rec.revision, 1),
            };

            let flags = wal::IS_CREATE | if lease != 0 { wal::HAS_LEASE } else { 0 };
            let kv = wal::KvWalRecord::new(
                flags,
                &rec.key,
                &rec.value,
                create_revision as i64,
                rec.revision as i64,
                version,
                lease,
            );
            wal.append_kv(&kv)?;

            state.insert(rec.key.clone(), (create_revision, version, rec.value.clone(), lease));
        }
    }

    Ok(())
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <old_wal_path> [new_wal_path]", args[0]);
        eprintln!("  Reads old-format WAL and writes new-format WAL.");
        eprintln!("  If new_wal_path is omitted, writes to <old_wal_path>.new");
        std::process::exit(1);
    }

    let old_path = Path::new(&args[1]);
    let new_path = if args.len() > 2 {
        Path::new(&args[2]).to_path_buf()
    } else {
        let name = format!("{}.new", args[1]);
        Path::new(&name).to_path_buf()
    };

    eprintln!("Reading old-format WAL from: {}", old_path.display());
    let records = read_old_records(old_path).unwrap_or_else(|e| {
        eprintln!("Failed to read old WAL: {e}");
        std::process::exit(1);
    });
    eprintln!("Read {} old-format records", records.len());

    eprintln!("Writing new-format WAL to: {}", new_path.display());
    migrate(old_path, &new_path, &records).unwrap_or_else(|e| {
        eprintln!("Migration failed: {e}");
        std::process::exit(1);
    });
    eprintln!("Migration complete");
}
