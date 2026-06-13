use etcd_client::*;
use rusqlite::Connection;

fn usage() {
    eprintln!("Usage: rudurru-migrate <k3s-state.db> [etcd-endpoint]");
    eprintln!("  Reads all non-deleted keys from k3s's kine SQLite database");
    eprintln!("  and writes them into Rudurru (default endpoint: http://127.0.0.1:2379)");
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 || args.len() > 3 {
        usage();
        std::process::exit(1);
    }

    let db_path = &args[1];
    let endpoint = args.get(2).map(|s| s.as_str()).unwrap_or("http://127.0.0.1:2379");

    println!("Rudurru K3s Migration Tool");
    println!("  Source: {db_path}");
    println!("  Target: {endpoint}");
    println!();

    // Open k3s SQLite database
    let conn = Connection::open(db_path).expect("open k3s state.db");

    // Find the kine table — k3s may use table names like "kine" or "kine_<hash>"
    let tables: Vec<String> = conn
        .prepare("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE '%kine%'")
        .expect("query tables")
        .query_map([], |row| row.get(0))
        .expect("map tables")
        .filter_map(|r| r.ok())
        .collect();

    let kine_table = match tables.first() {
        Some(t) => t.clone(),
        None => {
            // Try the old kine schema (separate namespace tables)
            let any_table: Vec<String> = conn
                .prepare("SELECT name FROM sqlite_master WHERE type='table' LIMIT 1")
                .expect("query any table")
                .query_map([], |row| row.get(0))
                .expect("map")
                .filter_map(|r| r.ok())
                .collect();
            if let Some(t) = any_table.first() {
                eprintln!("Found table '{t}' — trying kine schema detection");
                t.clone()
            } else {
                panic!("No tables found in database");
            }
        }
    };

    println!("Found kine table: '{kine_table}'");

    // Count total non-deleted keys
    let total_keys: i64 = conn
        .prepare(&format!(
            "SELECT COUNT(DISTINCT name) FROM \"{kine_table}\" WHERE deleted = 0"
        ))
        .expect("count keys")
        .query_row([], |row| row.get(0))
        .expect("read count");
    println!("Non-deleted keys: {total_keys}");

    if total_keys == 0 {
        println!("Nothing to migrate.");
        return;
    }

    // Get latest revision of each non-deleted key
    // kine stores all revisions; we take the highest `id` per `name`
    let mut stmt = conn
        .prepare(&format!(
            "SELECT k.name, k.value, k.id
             FROM \"{kine_table}\" k
             JOIN (
               SELECT name, MAX(id) AS max_id
               FROM \"{kine_table}\"
               WHERE deleted = 0
               GROUP BY name
             ) latest ON k.name = latest.name AND k.id = latest.max_id
             ORDER BY k.id ASC"
        ))
        .expect("prepare query");

    let rows: Vec<(String, Vec<u8>, i64)> = stmt
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?, row.get::<_, i64>(2)?))
        })
        .expect("query rows")
        .filter_map(|r| r.ok())
        .collect();

    println!("Latest revisions to migrate: {}", rows.len());

    // Connect to Rudurru
    let mut client = Client::connect([endpoint], None)
        .await
        .expect("connect to Rudurru");
    println!("Connected to {endpoint}");

    // Put each key
    let start = std::time::Instant::now();
    let mut count = 0usize;
    for (name, value, _rev) in &rows {
        if name.is_empty() || value.is_empty() {
            continue;
        }

        // Use a Txn with create (version=0) to avoid overwriting existing data
        // If the key already exists (e.g. partial migration), skip it
        let cmp = Compare::version(name.as_str(), CompareOp::Equal, 0);
        let txn = Txn::new()
            .when(vec![cmp])
            .and_then(vec![TxnOp::put(name.as_str(), value.as_slice(), None)]);

        match client.txn(txn).await {
            Ok(resp) => {
                if resp.succeeded() {
                    count += 1;
                    if count % 500 == 0 {
                        let elapsed = start.elapsed().as_secs_f64();
                        println!("  {count}/{} ({:.0} keys/s)", rows.len(), count as f64 / elapsed);
                    }
                }
                // else: key already exists, skip
            }
            Err(e) => {
                eprintln!("  Error putting key '{name}': {e}");
            }
        }
    }

    let elapsed = start.elapsed().as_secs_f64();
    println!();
    println!("Migration complete:");
    println!("  Keys migrated:     {count}/{}", rows.len());
    println!("  Skipped (exists):  {}", rows.len() - count);
    println!("  Time:              {elapsed:.1}s");
    println!("  Throughput:        {:.0} keys/s", count as f64 / elapsed.max(0.001));
}
