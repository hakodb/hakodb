use std::io::Read;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{anyhow, bail, Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use firelite::config::{DurabilityMode, FireLiteConfig};
use firelite::document::firelite_doc::FireLiteDoc;
use firelite::document::value::Value;
use firelite::engine::{BatchMutation, FireLite};
use firelite::index::composite::definition::SortDirection;
use firelite::query::filter::Operator;
use firelite::query::query::{AggregateOp, Query};
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use serde_json::{json, Map, Value as JsonValue};

#[cfg(feature = "net-sync")]
use firelite::net_sync::{NetSyncer, SyncStatus, DiscoveryMode};

#[cfg(feature = "cloud-sync")]
use firelite::cloud_sync::CloudSync;

#[derive(Parser, Debug)]
#[command(name = "firelite")]
#[command(version, about = "FireLite v0.6.17 command-line database manager")]
struct Cli {
    /// Database path (default: ./firelite.db)
    #[arg(long, global = true, default_value = "./firelite.db")]
    db: String,

    /// Durability mode (always | interval | manual | on-commit)
    #[arg(long, global = true, default_value = "on-commit")]
    durability: DurabilityArg,

    /// Encryption secret key (enables storage encryption)
    #[arg(long, global = true)]
    encryption_key: Option<String>,

    /// Comma-separated list of collections to encrypt (if empty, all are encrypted)
    #[arg(long, global = true)]
    encrypted_cols: Option<String>,

    /// Show execution time for the operation
    #[arg(long, global = true)]
    time: bool,

    /// Show count of results (for queries and collections)
    #[arg(long, global = true)]
    count: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum DurabilityArg {
    Always,
    Interval,
    Manual,
    OnCommit,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// List collections
    Collections,
    /// Mark a collection local-only (never syncs) or rejoin it with --off
    CollectionLocal {
        collection: String,
        /// Rejoin the collection to sync instead of marking local-only
        #[arg(long)]
        off: bool,
    },
    /// Vacuum a collection: purge tombstones (no sync traffic); next
    /// handshake pulls peer state. The restore half of a local reset.
    Vacuum { collection: String },
    /// Get one document by path: <collection>/<doc_id>
    Get {
        path: String,
        /// Write JSON output to file path
        #[arg(long)]
        output: Option<String>,
        /// update/patch the result
        #[arg(long)]
        set: bool,
        /// Data for the update (JSON object)
        #[arg(long)]
        data: Option<String>,
        /// Read JSON payload from file
        /// Batch Write
        #[arg(long)]
        fromfile: Option<String>,
    },
    /// Create/replace a document from JSON object
    Set {
        path: String,
        /// Inline JSON payload
        #[arg(long)]
        data: Option<String>,
        /// Read JSON payload from file
        #[arg(long)]
        fromfile: Option<String>,
        /// Batch Write
        #[arg(long)]
        batch: Option<bool>,
    },
    /// Patch/update fields of an existing document (or create if missing)
    Update {
        path: String,
        /// Inline JSON payload
        #[arg(long)]
        data: Option<String>,
        /// Read JSON payload from file
        #[arg(long)]
        fromfile: Option<String>,
        /// Batch Write
        #[arg(long)]
        batch: Option<bool>,
    },
    /// Delete one document by path: <collection>/<doc_id>
    Delete {
        path: String,
        /// Batch Write
        #[arg(long)]
        batch: Option<bool>,
        #[arg(long)]
        data: Option<String>,
        /// Local-only: mark so no sync tailer or handshake ever transmits it
        #[arg(long)]
        local: bool,
    },
    /// Query documents in a collection
    Query {
        collection: String,
        /// Repeated filter: field:op:value (e.g. age:gte:21, tags:in:[\"a\",\"b\"])
        #[arg(long = "where")]
        filters: Vec<String>,
        /// Repeated AND filter alias: field:op:value
        #[arg(long = "and")]
        and_filters: Vec<String>,
        /// Repeated OR filter: field:op:value
        #[arg(long = "or")]
        or_filters: Vec<String>,
        /// Full-text search shortcut: field:text (equivalent to field:match:text)
        #[arg(long)]
        fts: Option<String>,
        /// order format: field[:asc|desc]
        #[arg(long = "order")] // Explicitly name it for clarity
        order_by: Vec<String>,
        #[arg(long)]
        limit: Option<usize>,
        #[arg(long)]
        offset: Option<usize>,
        /// Cursor start_at values (comma-separated literals)
        #[arg(long)]
        start_at: Option<String>,
        /// Cursor start_after values (comma-separated literals)
        #[arg(long)]
        start_after: Option<String>,
        /// Cursor end_at values (comma-separated literals)
        #[arg(long)]
        end_at: Option<String>,
        /// Cursor end_before values (comma-separated literals)
        #[arg(long)]
        end_before: Option<String>,
        /// comma separated projection fields
        #[arg(long)]
        select: Option<String>,
        /// return blob-backed fields as __blob__ placeholders (no blob reads);
        /// resolve later with `get`. List views over image docs stay tiny.
        #[arg(long)]
        defer_blobs: bool,
        /// Repeated aggregate: count | sum:<field> | avg:<field>
        #[arg(long = "aggregate")]
        aggregates: Vec<String>,
        /// Write JSON output to file path
        #[arg(long)]
        output: Option<String>,
        /// Mass delete the results of this query
        #[arg(long)]
        delete: bool,
        /// Local-only mass delete: matched docs never leave this device
        #[arg(long)]
        local: bool,
        /// Mass update/patch the results of this query
        #[arg(long)]
        set: bool,
        /// Data for the mass update (JSON object)
        #[arg(long)]
        data: Option<String>,
        /// Read JSON payload from file
        #[arg(long)]
        fromfile: Option<String>,
    },
    /// Aggregations: count | sum | avg
    Aggregate {
        collection: String,
        #[arg(value_enum)]
        kind: AggregateKindArg,
        #[arg(long)]
        field: Option<String>,
        #[arg(long = "where")]
        filters: Vec<String>,
    },
    /// Watch changes in a collection
    Watch { collection: String },
    /// Seed a collection with random-ish complex JSON docs (max: 500)
    Seed { collection: String, docsize: usize },
    /// Index operations
    Index {
        #[command(subcommand)]
        command: IndexCommands,
    },
    /// Serializable transaction helper (single-doc set)
    TxSet {
        path: String,
        #[arg(long)]
        data: Option<String>,
        #[arg(long)]
        fromfile: Option<String>,
    },
    /// Print internal stats
    Stats,
    /// Force compaction
    Compact,
    /// REST-like command surface: METHOD + PATH + optional --data JSON
    Rest {
        method: String,
        path: String,
        #[arg(long)]
        data: Option<String>,
        #[arg(long = "where")]
        filters: Vec<String>,
    },
    /// Network Peer Management (Only in Serve mode)
    Peers,
    /// Exit the shell
    Exit,
    /// Exit the shell
    Quit,
    /// main serve
    Serve {
        #[arg(long)]
        port: u16,

        #[arg(long)]
        node_id: String,

        #[arg(long, default_value = "default_key")]
        key: String,

        /// LAN discovery transports: mdns (desktop default), broadcast
        /// (mobile default, no multicast), or both (mixed groups — a desktop
        /// joining mobile peers must opt into both or broadcast).
        #[arg(long, value_enum, default_value = "mdns")]
        discovery: DiscoveryModeArg,

        /// Cloud Sync Server bind address (e.g. 0.0.0.0:8080)
        #[arg(long)]
        bind: Option<String>,

        /// Cloud Sync Client target server URL (e.g. ws://127.0.0.1:8080)
        #[arg(long)]
        server: Option<String>,

        /// Room name for Cloud Sync (used by the client to join a room).
        /// The server stores room collections as <room_name>_<collection>.
        #[arg(long)]
        room_name: Option<String>,

        /// Auth Token for Cloud Sync
        #[arg(long, default_value = "default_token")]
        token: String,
    },
}

#[derive(Subcommand, Debug)]
enum IndexCommands {
    Create {
        collection: String,
        field: String,
    },
    CreateComposite {
        collection: String,
        /// Comma separated list: field[:asc|desc],field[:asc|desc]
        #[arg(long)]
        fields: String,
    },
    CreateFts {
        collection: String,
        field: String,
    },
    List {
        /// Optional collection filter
        collection: Option<String>,
    },
    /// list index of collection
    Keys { collection: String },
    /// Inspec Index of a field 
    Inspect {
        collection: String,
        field: String,
    }
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum DiscoveryModeArg {
    Mdns,
    Broadcast,
    Both,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum AggregateKindArg {
    Count,
    Sum,
    Avg,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    if let Commands::Serve {
        port,
        node_id,
        key,
        discovery,
        bind,
        server,
        room_name,
        token, } = &cli.command {
        return run_server(&cli, Some(*port), node_id, key, *discovery, bind.as_deref(), server.as_deref(), room_name.as_deref(), token);
    }

    let db = open_db(&cli)?;
    execute_command(&db, cli.command, &cli.db, cli.durability, None, cli.time, cli.count)
}

fn open_db(cli: &Cli) -> Result<FireLite> {
    let mut cfg = FireLiteConfig::default();

    cfg.durability_mode = match cli.durability {
        DurabilityArg::Always => DurabilityMode::Always,
        DurabilityArg::Interval => DurabilityMode::Interval,
        DurabilityArg::Manual => DurabilityMode::Manual,
        DurabilityArg::OnCommit => DurabilityMode::OnCommit,
    };

        // 2. Set Encryption Key
    cfg.encryption_key = cli.encryption_key.clone();

    // 3. Set Encrypted Collections (Convert comma-string to HashSet)
    if let Some(cols_str) = &cli.encrypted_cols {
        let set: std::collections::HashSet<String> = cols_str
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        cfg.encrypted_cols = Some(set);
    }


    FireLite::open(&cli.db, cfg)
        .with_context(|| format!("failed to open db at {}", &cli.db))
        .map(|db| {
            // ponytail: open() returns while index recovery still runs, and
            // queries issued first silently plan FullCollection (cursor
            // bounds ignored, pages repeat) — so wait for READINESS here.
            // Deliberately NOT full quiescence (blob drain/maintenance don't
            // affect read correctness and could take minutes on big DBs).
            let t0 = std::time::Instant::now();
            while !db.is_indexes_ready() {
                if t0.elapsed() > std::time::Duration::from_secs(30) {
                    eprintln!("warning: indexes not ready after 30s, continuing degraded");
                    break;
                }
                std::thread::sleep(std::time::Duration::from_millis(20));
            }
            db
        })
}

fn read_payload_input(data: Option<&str>, fromfile: Option<&str>) -> Result<String> {
    if let Some(path) = fromfile {
        return std::fs::read_to_string(path)
            .with_context(|| format!("failed to read input file: {path}"));
    }
    if let Some(inline) = data {
        return Ok(inline.to_string());
    }
    let mut buf = String::new();
    std::io::stdin()
        .read_to_string(&mut buf)
        .context("failed to read stdin payload")?;
    if buf.trim().is_empty() {
        bail!("no input data provided (use --data, --fromfile, or stdin)");
    }
    Ok(buf)
}

fn emit_json(value: &JsonValue, output: Option<&str>) -> Result<()> {
    let rendered = serde_json::to_string_pretty(value)?;
    if let Some(path) = output {
        std::fs::write(path, rendered)
            .with_context(|| format!("failed to write output file: {path}"))?;
    } else {
        println!("{rendered}");
    }
    Ok(())
}

fn list_collections(db: &FireLite) -> Result<()> {
    let cols = db.list_collections()?;
    let marked: Vec<String> = cols.into_iter()
        .map(|c| if db.is_collection_local(&c) { format!("{c} (local-only)") } else { c })
        .collect();
    println!("{}", serde_json::to_string_pretty(&marked)?);
    Ok(())
}

fn split_doc_path(path: &str) -> Result<(&str, &str)> {
    let (collection, doc_id, fields) = split_doc_path_with_fields(path)?;
    if !fields.is_empty() {
        bail!("expected path format: <collection>/<doc_id>");
    }
    Ok((collection, doc_id))
}

fn split_doc_path_with_fields(path: &str) -> Result<(&str, &str, Vec<String>)> {
    let parts: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();

    if parts.len() == 1 {
        return Ok((path, "", Vec::new()));
    }

    Ok((
        parts[0],
        parts[1],
        parts[2..].iter().map(|s| s.to_string()).collect(),
    ))
}

fn get_doc(
    db: &FireLite,
    path: &str,
    output: Option<&str>,
    is_set: bool,
    data: Option<&str>,
    show_time: bool
) -> Result<()> {
    let start_time = Instant::now();
    let (collection, doc_id, fields) = split_doc_path_with_fields(path)?;
    let out = if fields.is_empty() {
        db.get(collection, doc_id)?
            .map(|doc| doc_to_json(doc_id, &doc))
            .unwrap_or(JsonValue::Null)
    } else {
        db.get(collection, doc_id)?
            .map(|doc| {
                let selected: Vec<(String, Value)> = fields
                    .iter()
                    .filter_map(|f| doc.get(f).cloned().map(|v| (f.clone(), v)))
                    .collect();
                projected_to_json(doc_id, selected)
            })
            .unwrap_or(JsonValue::Null)
    };
    if is_set {
        set_doc(db, path, data.expect("Should not empty"), true, false, false)?;
    }
    let elapsed = start_time.elapsed();
    emit_json(&out, output)?;
    if show_time {
        eprintln!("Execution time: {:?}", elapsed);
    }
    Ok(())
}

fn set_doc(db: &FireLite, path: &str, data: &str, merge: bool, is_batch: bool, show_time: bool) -> Result<()> {
    let start_time = Instant::now();
    if is_batch {
        let collection = path; // In batch mode, path is just the collection name
        let array: JsonValue =
            serde_json::from_str(data).context("Batch data must be a JSON array")?;
        let items = array
            .as_array()
            .ok_or_else(|| anyhow!("--batch requires a JSON array of objects"))?;

        let mut mutations = Vec::with_capacity(items.len());
        let mut results = Vec::new();

        for (idx, item) in items.iter().enumerate() {
            let obj = item
                .as_object()
                .ok_or_else(|| anyhow!("Item at index {} is not an object", idx))?;

            // Generate ID if missing
            let doc_id = obj
                .get("id")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .unwrap_or_else(generate_id);

            // Handle data (might be inside a "data" field or the object itself)
            let raw_data = obj.get("data").unwrap_or(item);
            let mut doc = if merge {
                db.get(collection, &doc_id)?.unwrap_or_default()
            } else {
                FireLiteDoc::default()
            };

            for (k, v) in raw_data
                .as_object()
                .ok_or_else(|| anyhow!("Data for item {} is not an object", idx))?
            {
                if k == "id" && obj.get("data").is_none() {
                    continue;
                } // Skip ID if it's top level
                doc.insert(k.clone(), json_to_fire(v.clone())?);
            }

            mutations.push(firelite::engine::BatchMutation::Put {
                collection: collection.to_string(),
                doc_id: doc_id.clone(),
                doc,
            });
            results.push(doc_id);
        }

        db.write_batch(mutations)?;
        println!("Ok: {collection}/[{}]", results.join(", "));
    } else {
        let (collection, doc_id, fields) = split_doc_path_with_fields(path)?;
        let payload: JsonValue = serde_json::from_str(data).context("data must be valid JSON")?;
        let mut doc = if merge {
            db.get(collection, doc_id)?.unwrap_or_default()
        } else {
            FireLiteDoc::default()
        };

        if fields.is_empty() {
            let obj = payload
                .as_object()
                .ok_or_else(|| anyhow!("data must be a JSON object"))?;
            for (k, v) in obj {
                doc.insert(k.clone(), json_to_fire(v.clone())?);
            }
        } else if fields.len() == 1 && !payload.is_object() {
            doc.insert(fields[0].clone(), json_to_fire(payload)?);
        } else {
            let obj = payload.as_object().ok_or_else(|| {
                anyhow!("data must be a JSON object when multiple path fields are provided")
            })?;
            for field in fields {
                if let Some(v) = obj.get(&field) {
                    doc.insert(field, json_to_fire(v.clone())?);
                }
            }
        }

        let id = db.put(collection, doc_id, &doc)?;
        println!("OK: {collection}/{id}");
    }
    if show_time {
        eprintln!("Execution time: {:?}", start_time.elapsed());
    }
    Ok(())
}

fn delete_doc(db: &FireLite, path: &str, is_batch: bool, data: Option<&str>, show_time: bool, local_only: bool) -> Result<()> {
    let start_time = Instant::now();
    if is_batch {
        let collection = path;
        let id_input =
            data.ok_or_else(|| anyhow!("Batch delete requires --data with an array of IDs"))?;
        let array: JsonValue = serde_json::from_str(id_input)?;
        let ids = array
            .as_array()
            .ok_or_else(|| anyhow!("--data must be an array of ID strings"))?;

        let id_list: Vec<String> = ids.iter().filter_map(|v| v.as_str().map(str::to_string)).collect();
        let count = if local_only {
            db.delete_ids_local(collection, &id_list)?
        } else {
            let mutations: Vec<_> = id_list.iter()
                .map(|id| firelite::engine::BatchMutation::Delete {
                    collection: collection.to_string(),
                    doc_id: id.clone(),
                })
                .collect();
            let count = mutations.len();
            db.write_batch(mutations)?;
            count
        };
        println!("OK: deleted {} documents from {}", count, collection);
    } else {
        let (collection, doc_id) = split_doc_path(path)?;
        if local_only {
            let id = db.delete_local(collection, doc_id)?;
            println!("OK: locally deleted {collection}/{id} (not synced)");
        } else {
            let id = db.delete(collection, doc_id)?;
            println!("OK: deleted {collection}/{id}");
        }
    }
    if show_time {
        eprintln!("Execution time: {:?}", start_time.elapsed());
    }
    Ok(())
}

fn run_tx_set(db: &FireLite, path: &str, data: &str) -> Result<()> {
    let (collection, doc_id) = split_doc_path(path)?;
    let payload: JsonValue = serde_json::from_str(data).context("data must be valid JSON")?;
    let obj = payload
        .as_object()
        .ok_or_else(|| anyhow!("data must be a JSON object"))?;

    let mut doc = FireLiteDoc::default();
    for (k, v) in obj {
        doc.insert(k.clone(), json_to_fire(v.clone())?);
    }

    let mut tx = db.begin_serializable_transaction();
    tx.get(db, collection, doc_id)?;
    tx.put(collection, doc_id, doc);
    let ids = tx.commit(db)?;
    println!(
        "OK: transaction committed for {collection}/{}",
        ids.into_iter().next().unwrap_or_default()
    );
    Ok(())
}

fn parse_composite_fields(input: &str) -> Result<Vec<(String, SortDirection)>> {
    let mut out = Vec::new();
    for raw in input.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        let mut parts = raw.split(':');
        let field = parts
            .next()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| anyhow!("invalid composite field spec: {raw}"))?;
        let direction = match parts.next().map(str::trim).unwrap_or("asc") {
            "asc" => SortDirection::Asc,
            "desc" => SortDirection::Desc,
            other => bail!("invalid direction '{other}' in composite field spec: {raw}"),
        };
        if parts.next().is_some() {
            bail!("invalid composite field spec: {raw}");
        }
        out.push((field.to_string(), direction));
    }
    if out.is_empty() {
        bail!("composite fields cannot be empty");
    }
    Ok(out)
}

fn run_query(
    db: &FireLite,
    collection: &str,
    filters: &[String],
    and_filters: &[String],
    or_filters: &[String],
    fts: Option<&str>,
    order_by: &[String],
    limit: Option<usize>,
    offset: Option<usize>,
    start_at: Option<&str>,
    start_after: Option<&str>,
    end_at: Option<&str>,
    end_before: Option<&str>,
    select: Option<&str>,
    defer_blobs: bool,
    aggregates: &[String],
    output: Option<&str>,
    delete_action: bool,
    local_only: bool,
    set_action: bool,
    action_data: Option<&str>,
    show_count: bool,
    show_time: bool
) -> Result<()> {
    let start_time = Instant::now();
    let mut q = Query::new(collection);
    // ponytail: deferred blob fields come back as __blob__ placeholders.
    q.defer_blobs = defer_blobs;

    // --- Build Query using new Fluent logic ---
    for f in filters.iter().chain(and_filters.iter()) {
        let parsed = parse_filter(f)?;
        q = q.where_filter(&parsed.field, parsed.op, parsed.value);
    }
    for f in or_filters {
        let parsed = parse_filter(f)?;
        q = q.or_where(&parsed.field, parsed.op, parsed.value);
    }
    if let Some(fts) = fts {
        let (field, text) = parse_fts(fts)?;
        q = q.where_filter(field, Operator::Match, Value::String(text.to_string()));
    }
    for order_spec in order_by {
        let (field, asc) = parse_order(&order_spec)?;
        q = q.order_by(field, asc);
    }
    if let Some(limit) = limit { q = q.limit(limit); }
    if let Some(offset) = offset { q = q.offset(offset); }
    
    // Cursor handling
    if let Some(v) = start_at { q.start_at = Some(parse_cursor_values(v)?); }
    if let Some(v) = start_after { q.start_after = Some(parse_cursor_values(v)?); }
    if let Some(v) = end_at { q.end_at = Some(parse_cursor_values(v)?); }
    if let Some(v) = end_before { q.end_before = Some(parse_cursor_values(v)?); }

    // --- CORE CHAINING: Mass Delete ---
    if delete_action {
        let count = if local_only {
            db.delete_where_local(q.clone())
        } else {
            db.delete_where(q.clone())
        }
        .context("Failed to execute chained delete")?;
        let scope = if local_only { " (local-only, not synced)" } else { "" };
        println!("OK: mass deleted {count} documents from {collection}{scope}");
        if show_time {
            eprintln!("Execution time: {:?}", start_time.elapsed());
        }
        return Ok(());
    }

    // --- CORE CHAINING: Mass Update/Patch ---
    if set_action {
        let raw_json = action_data.ok_or_else(|| anyhow!("--set requires --data with a JSON object"))?;
        let payload: JsonValue = serde_json::from_str(raw_json).context("Action data must be valid JSON")?;
        let update_map = payload.as_object().ok_or_else(|| anyhow!("Action data must be an object"))?;

        let mut updates = Vec::new();
        for (k, v) in update_map {
            updates.push((k.clone(), json_to_fire(v.clone())?));
        }

        let count = db.patch_where(q.clone(), updates)
            .context("Failed to execute chained patch")?;
        println!("OK: mass updated {count} documents in {collection}");
        if show_time {
            eprintln!("Execution time: {:?}", start_time.elapsed());
        }
        return Ok(());
    }

    // --- Aggregates (using improved core aggregate executor) ---
    if !aggregates.is_empty() {
        let mut agg_results = Vec::new();
        for spec in aggregates {
            let op = parse_aggregate_spec(spec)?;
            let mut agg_query = q.clone();
            agg_query = match op {
                AggregateOp::Count => agg_query.aggregate(AggregateOp::Count),
                AggregateOp::Sum(field) => agg_query.aggregate(AggregateOp::Sum(field)),
                AggregateOp::Avg(field) => agg_query.aggregate(AggregateOp::Avg(field)),
            };
            let out = db.execute_aggregation(agg_query)?;
            agg_results.push(serde_json::to_value(out)?);
        }
        let elapsed = start_time.elapsed();
        emit_json(&JsonValue::Array(agg_results), output)?;
        if show_time {
            eprintln!("Execution time: {:?}", elapsed);
        }
        return Ok(());
    }

    // --- Standard Data Retrieval ---
    if let Some(select_str) = select {
        let fields: Vec<String> = select_str.split(',').map(|s| s.trim().to_string()).collect();
        let rows = db.query_projected_zero_copy(q, &fields)?;
        let elapsed = start_time.elapsed();
        let cnt = rows.len();
        let json_rows: Vec<JsonValue> = rows.into_iter()
        .map(|(id, fields)| projected_to_json(&id, fields))
        .collect();
        emit_json(&JsonValue::Array(json_rows), output)?;
        if show_count { eprintln!("Results count: {}", cnt); }
        if show_time {
            eprintln!("Execution time: {:?}", elapsed);
        }
    } else {
        let rows = db.query(q)?;
        let elapsed = start_time.elapsed();
        let cnt = rows.len();
        let json_rows: Vec<JsonValue> = rows.into_iter()
        .map(|(id, doc)| doc_to_json(&id, &doc))
        .collect();
        emit_json(&JsonValue::Array(json_rows), output)?;
        if show_count { eprintln!("Results count: {}", cnt); }
        if show_time {
            eprintln!("Execution time: {:?}", elapsed);
        }
    }

    Ok(())
}

fn run_aggregate(
    db: &FireLite,
    collection: &str,
    kind: AggregateKindArg,
    field: Option<&str>,
    filters: &[String],
    show_time: bool
) -> Result<()> {
    let start_time = Instant::now();
    let mut q = Query::new(collection);
    for f in filters {
        let parsed = parse_filter(f)?;
        q = q.where_filter(&parsed.field, parsed.op, parsed.value);
    }
    
    // Use core AggregateOp definitions
    q = match kind {
        AggregateKindArg::Count => q.aggregate(AggregateOp::Count),
        AggregateKindArg::Sum => {
            let f = field.ok_or_else(|| anyhow!("--field required for sum"))?;
            q.aggregate(AggregateOp::Sum(f.to_string()))
        },
        AggregateKindArg::Avg => {
            let f = field.ok_or_else(|| anyhow!("--field required for avg"))?;
            q.aggregate(AggregateOp::Avg(f.to_string()))
        },
    };

    // The core now handles the O(N) scan automatically if no index matches
    let out = db.execute_aggregation(q)?;
    let elapsed = start_time.elapsed();
    println!("{}", serde_json::to_string_pretty(&out)?);
    if show_time {
        eprintln!("Execution time: {:?}", elapsed);
    }
    Ok(())
}

fn parse_aggregate_spec(input: &str) -> Result<AggregateOp> {
    let raw = input.trim();
    if raw.eq_ignore_ascii_case("count") {
        return Ok(AggregateOp::Count);
    }

    let (kind, field) = raw.split_once(':').ok_or_else(|| {
        anyhow!("invalid aggregate '{input}', expected count or sum:<field>/avg:<field>")
    })?;
    let field = field.trim();
    if field.is_empty() {
        bail!("aggregate field cannot be empty in '{input}'");
    }
    match kind.to_ascii_lowercase().as_str() {
        "sum" => Ok(AggregateOp::Sum(field.to_string())),
        "avg" => Ok(AggregateOp::Avg(field.to_string())),
        _ => bail!("unsupported aggregate kind in '{input}'"),
    }
}

fn watch_collection(db: &FireLite, collection: &str) -> Result<()> {
    println!("Watching [{collection}] ... Ctrl+C to exit");
    let rx = db.watch_collection(collection);
    while let Ok(event) = rx.recv() {
        println!(
            "{} {}",
            format!("{:?}", event.kind).to_uppercase(),
            event.path
        );
    }
    Ok(())
}

fn seed_collection(db: &FireLite, collection: &str, docsize: usize) -> Result<()> {
    if docsize == 0 {
        bail!("docsize must be > 0");
    }
    if docsize > 500 {
        bail!("docsize max is 500");
    }

    // Example indexes for all 3 index modes
    db.create_index(collection, "data")?;
    db.create_fts_index(collection, "description")?;
    let _ = db.create_composite_index(
        collection,
        vec![
            ("data".to_string(), SortDirection::Asc),
            ("score".to_string(), SortDirection::Desc),
        ],
    );

    let mut mutations = Vec::with_capacity(docsize);
    for i in 0..docsize {
        let mut doc = FireLiteDoc::default();
        let valid = i % 2 == 0;
        let status = if i % 3 == 0 { "active" } else { "idle" };
        let score = ((i * 37) % 1000) as i64;

        doc.insert(
            "data".to_string(),
            Value::String(if valid { "valid" } else { "invalid" }.to_string()),
        );
        doc.insert("status".to_string(), Value::String(status.to_string()));
        doc.insert("score".to_string(), Value::Int(score));
        doc.insert(
            "description".to_string(),
            Value::String(format!(
                "seeded firelite document {} with {} state and score {}",
                i, status, score
            )),
        );
        doc.insert(
            "tags".to_string(),
            Value::Array(vec![
                Value::String(format!("group_{}", i % 10)),
                Value::String(if valid { "valid" } else { "invalid" }.to_string()),
                Value::String(status.to_string()),
            ]),
        );
        doc.insert(
            "profile".to_string(),
            Value::Map(vec![
                ("level".into(), Value::Int((i % 7) as i64)),
                (
                    "country".into(),
                    Value::String(if i % 2 == 0 { "US" } else { "CA" }.to_string()),
                ),
                (
                    "flags".into(),
                    Value::Array(vec![Value::Bool(valid), Value::Bool(i % 5 == 0)]),
                ),
            ]),
        );

        mutations.push(BatchMutation::Put {
            collection: collection.to_string(),
            doc_id: format!("{}", 19800429 + i as i64),
            doc,
        });
        // db.put(collection, &id, &doc)?;
    }

    db.write_batch(mutations)?;

    println!("OK: seeded {docsize} docs into '{collection}'");
    println!("OK: created sample indexes:");
    println!("  - simple/secondary: {collection}.data");
    println!("  - fts: {collection}.description");
    println!("  - composite: (data asc, score desc)");
    println!("Try:");
    println!("  firelite-cli --db <db> query {collection} --where data:eq:valid");
    println!("  firelite-cli --db <db> query {collection} --where description:match:seeded");
    println!("  firelite-cli --db <db> query {collection} --where data:eq:valid --order score:desc --limit 5");
    Ok(())
}

fn run_rest(
    db: &FireLite,
    method: &str,
    path: &str,
    data: Option<&str>,
    filters: &[String],
) -> Result<()> {
    match method.to_ascii_uppercase().as_str() {
        "GET" => {
            let parts: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
            match parts.len() {
                0 => list_collections(db),
                1 => run_query(
                    db,
                    parts[0],
                    filters,
                    &[],
                    &[],
                    None,
                    &[],
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    None,
                    false,
                    &[],
                    None,
                    false,
                    false,
                    false,
                    None,
                    false,
                    false
                ),
                2 => get_doc(db, path, None, false, None, false),
                _ => bail!("unsupported path depth for GET"),
            }
        }
        "POST" | "PUT" => set_doc(
            db,
            path,
            data.ok_or_else(|| anyhow!("--data required for {method}"))?,
            false,
            false,
            false
        ),
        "PATCH" => set_doc(
            db,
            path,
            data.ok_or_else(|| anyhow!("--data required for PATCH"))?,
            true,
            false,
            false
        ),
        "DELETE" => delete_doc(db, path, false, Some(" "), false, false),
        other => bail!("unsupported REST method: {other}"),
    }
}

struct ParsedFilter {
    field: String,
    op: Operator,
    value: Value,
}

fn parse_filter(input: &str) -> Result<ParsedFilter> {
    let parts: Vec<&str> = input.splitn(3, ':').collect();
    if parts.len() != 3 {
        bail!("invalid filter '{input}', expected field:op:value");
    }
    let field = strip_wrapping_quotes(parts[0]).trim().to_string();
    let op = parse_operator(parts[1])?;
    
    let value = if matches!(op, Operator::In | Operator::NotIn) {
        let val_str = parts[2].trim();
        
        // Ensure it has brackets
        if !val_str.starts_with('[') || !val_str.ends_with(']') {
            bail!("IN operator requires array format: [item1,item2]");
        }

        let inner = &val_str[1..val_str.len()-1];
        if inner.trim().is_empty() {
            Value::Array(vec![])
        } else {
            let mut items = Vec::new();
            // Split by comma, but respect potential JSON inside
            for raw_item in inner.split(',') {
                let trimmed = raw_item.trim();
                // Use parse_literal but fallback to String if parsing fails (for pv1, etc)
                items.push(parse_literal(trimmed).unwrap_or(Value::String(trimmed.to_string())));
            }
            Value::Array(items)
        }
    } else {
        parse_literal_for_operator(&op, parts[2].trim())?
    };
    Ok(ParsedFilter { field, op, value })
}

fn parse_order(input: &str) -> Result<(&str, bool)> {
    let mut parts = input.splitn(2, ':');
    let field = parts.next().unwrap_or_default();
    if field.is_empty() {
        bail!("order field cannot be empty");
    }
    let asc = match parts.next().unwrap_or("asc").to_ascii_lowercase().as_str() {
        "asc" => true,
        "desc" => false,
        other => bail!("invalid order direction: {other}"),
    };
    Ok((field, asc))
}

fn parse_operator(input: &str) -> Result<Operator> {
    match input.to_ascii_lowercase().as_str() {
        "eq" | "==" => Ok(Operator::Eq),
        "ne" | "!=" => Ok(Operator::Ne),
        "gt" | ">" => Ok(Operator::Gt),
        "gte" | ">=" => Ok(Operator::Gte),
        "lt" | "<" => Ok(Operator::Lt),
        "lte" | "<=" => Ok(Operator::Lte),
        "match" => Ok(Operator::Match),
        "matchprefix" | "match_prefix" => Ok(Operator::MatchPrefix),
        "contains" => Ok(Operator::Contains),
        "startswith" | "starts_with" => Ok(Operator::StartsWith),
        "in" => Ok(Operator::In),
        "notin" | "not_in" => Ok(Operator::NotIn),
        "arraycontains" | "array_contains" => Ok(Operator::ArrayContains),
        "arraycontainsany" | "array_contains_any" => Ok(Operator::ArrayContainsAny),
        other => bail!("unsupported operator: {other}"),
    }
}

fn parse_fts(input: &str) -> Result<(&str, &str)> {
    let mut parts = input.splitn(2, ':');
    let field = parts.next().unwrap_or_default().trim();
    let text = parts.next().unwrap_or_default().trim();
    if field.is_empty() || text.is_empty() {
        bail!("invalid --fts format, expected field:text");
    }
    Ok((field, strip_wrapping_quotes(text)))
}

fn parse_cursor_values(input: &str) -> Result<Vec<Value>> {
    let mut out = Vec::new();
    // for token in input.split(',').map(str::trim).filter(|v| !v.is_empty()) {
    //     out.push(parse_literal(strip_wrapping_quotes(token))?);
    // }
    for token in input.split(',').map(str::trim).filter(|v| !v.is_empty()) {
        out.push(parse_literal(token)?);
    }
    if out.is_empty() {
        bail!("cursor values cannot be empty");
    }
    Ok(out)
}

fn parse_literal_for_operator(op: &Operator, input: &str) -> Result<Value> {
    match op {
        Operator::Contains | Operator::StartsWith | Operator::Match => {
            // Because we bypassed parse_literal, we manually strip quotes here
            Ok(Value::String(strip_wrapping_quotes(input).to_string()))
        }
        _ => parse_literal(input),
    }
}

fn parse_literal(input: &str) -> Result<Value> {
    let trimmed = input.trim();
    
    // 1. Explicit String via Quotes (if they survived the shell)
    if (trimmed.starts_with('"') && trimmed.ends_with('"')) || 
       (trimmed.starts_with('\'') && trimmed.ends_with('\'')) {
        return Ok(Value::String(trimmed[1..trimmed.len()-1].to_string()));
    }

    // 2. Keywords
    let lower = trimmed.to_ascii_lowercase();
    match lower.as_str() {
        "null" => return Ok(Value::Null),
        "true" => return Ok(Value::Bool(true)),
        "false" => return Ok(Value::Bool(false)),
        _ => {}
    }
    
    // 3. Numbers (Standard parsing)
    if let Ok(i) = i64::from_str(trimmed) { return Ok(Value::Int(i)); }
    if let Ok(f) = f64::from_str(trimmed) { return Ok(Value::Float(f)); }
    
    // 4. JSON fallback (for Maps/complex Arrays)
    if (trimmed.starts_with('{') && trimmed.ends_with('}')) || 
       (trimmed.starts_with('[') && trimmed.ends_with(']')) {
        if let Ok(json) = serde_json::from_str::<JsonValue>(trimmed) {
            return json_to_fire(json);
        }
    }
    
    // 5. Default to String (Catch-all for pv1, status codes, etc)
    Ok(Value::String(trimmed.to_string()))
}

fn strip_wrapping_quotes(input: &str) -> &str {
    let trimmed = input.trim();
    if trimmed.len() >= 2 {
        let b = trimmed.as_bytes();
        let first = b[0];
        let last = b[trimmed.len() - 1];
        if (first == b'"' && last == b'"') || (first == b'\'' && last == b'\'') {
            return &trimmed[1..trimmed.len() - 1];
        }
    }
    trimmed
}

fn json_to_fire(v: JsonValue) -> Result<Value> {
    Value::from_json(v).map_err(|e| anyhow!(e))
}

fn fire_to_json(v: &Value) -> JsonValue {
    v.to_json()
}

fn doc_to_json(id: &str, doc: &FireLiteDoc) -> JsonValue {
    let mut json = doc.to_json();
    if let Some(obj) = json.as_object_mut() {
        obj.insert("id".to_string(), serde_json::json!(id));
    }
    json
}

fn projected_to_json(id: &str, fields: Vec<(String, Value)>) -> JsonValue {
    let mut map = Map::new();
    map.insert("id".to_string(), json!(id));
    for (k, v) in fields {
        map.insert(k, fire_to_json(&v));
    }
    JsonValue::Object(map)
}

fn generate_id() -> String {
    // A simple, fast ID based on hex-encoded nanoseconds
    use std::time::{SystemTime, UNIX_EPOCH};
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{:x}", now)
}

// 1. Move the match logic into a reusable function
fn execute_command(
    db: &FireLite,
    command: Commands,
    _db_path: &str,
    _durability: DurabilityArg,
    net: Option<&NetSyncer>,
    show_time: bool,   // New parameter
    show_count: bool,
) -> Result<()> {
    // let start_time = Instant::now();
    match command {
        Commands::Collections => list_collections(db)?,
        Commands::CollectionLocal { collection, off } => {
            db.set_collection_local(&collection, !off);
            if off {
                println!("OK: {collection} rejoined sync (newer remote ops apply per LWW)");
            } else {
                println!("OK: {collection} is now local-only (never syncs)");
            }
        }
        Commands::Vacuum { collection } => {
            let n = db.vacuum_collection(&collection)?;
            println!("OK: vacuumed {n} tombstones from {collection} (not synced)");
        }
        Commands::Get {
            path,
            output,
            set,
            fromfile,
            data,
        } => {
            let payload = if set {
                Some(read_payload_input(data.as_deref(), fromfile.as_deref())?)
            } else {
                None
            };
            get_doc(db, &path, output.as_deref(), set, payload.as_deref(), show_time)?
        }
        Commands::Set {
            path,
            data,
            fromfile,
            batch,
        } => {
            let payload = read_payload_input(data.as_deref(), fromfile.as_deref())?;
            set_doc(db, &path, &payload, false, batch.unwrap_or(false), show_time)?
        }
        Commands::Update {
            path,
            data,
            fromfile,
            batch,
        } => {
            let payload = read_payload_input(data.as_deref(), fromfile.as_deref())?;
            set_doc(db, &path, &payload, true, batch.unwrap_or(false), show_time)?
        }
        Commands::Delete { path, batch, data, local } => {
            delete_doc(db, &path, batch.unwrap_or(false), data.as_deref(), show_time, local)?
        }
        Commands::Query {
            collection,
            filters,
            and_filters,
            or_filters,
            fts,
            order_by,
            limit,
            offset,
            start_at,
            start_after,
            end_at,
            end_before,
            select,
            aggregates,
            output,
            delete,
            local,
            set,
            data,
            fromfile,
            defer_blobs,
        } => {
            let payload = if set {
                Some(read_payload_input(data.as_deref(), fromfile.as_deref())?)
            } else {
                None
            };
            run_query(
                db,
                &collection,
                &filters,
                &and_filters,
                &or_filters,
                fts.as_deref(),
                &order_by,
                limit,
                offset,
                start_at.as_deref(),
                start_after.as_deref(),
                end_at.as_deref(),
                end_before.as_deref(),
                select.as_deref(),
                defer_blobs,
                &aggregates,
                output.as_deref(),
                delete,
                local,
                set,
                payload.as_deref(),
                show_count,
                show_time
            )?
        }
        Commands::Aggregate {
            collection,
            kind,
            field,
            filters,
        } => run_aggregate(db, &collection, kind, field.as_deref(), &filters, show_time)?,
        Commands::Watch { collection } => watch_collection(db, &collection)?,
        Commands::Seed {
            collection,
            docsize,
        } => seed_collection(db, &collection, docsize)?,
        Commands::Index { command } => match command {
            IndexCommands::Create { collection, field } => {
                db.create_index(&collection, &field)?;
                println!("OK: created index");
            }
            IndexCommands::CreateComposite { collection, fields } => {
                let parts = parse_composite_fields(&fields)?;
                db.create_composite_index(&collection, parts)?;
            }
            IndexCommands::CreateFts { collection, field } => {
                db.create_fts_index(&collection, &field)?
            }
            IndexCommands::List { collection } => {
                let out = db.list_indexes(collection.as_deref());
                println!("{}", serde_json::to_string_pretty(&out)?);
            }
            IndexCommands::Keys { collection } => {
                let keys = db.list_storage_keys(&collection)?;
                println!("{}", serde_json::to_string_pretty(&keys)?);
            }
            IndexCommands::Inspect { collection, field } => {
                let entries = db.inspect_index(&collection, &field);
                for entry in entries {
                    println!("{}", entry);
                }
            }
        },
        Commands::TxSet {
            path,
            data,
            fromfile,
        } => {
            let payload = read_payload_input(data.as_deref(), fromfile.as_deref())?;
            run_tx_set(db, &path, &payload)?
        }
        Commands::Stats => println!("{}", serde_json::to_string_pretty(&db.get_stats())?),
        Commands::Compact => db.compact()?,
        Commands::Rest {
            method,
            path,
            data,
            filters,
        } => run_rest(db, &method, &path, data.as_deref(), &filters)?,
        // For Serve, we handle it separately to avoid infinite recursion
        Commands::Peers => {
            #[cfg(feature = "net-sync")]
            if let Some(_n) = net {
                let status = net
                    .ok_or_else(|| anyhow!("Networking not active."))?
                    .status();
                println!("\n--- Mesh Network ---");
                println!("Status: {}", format_sync_status(status.status)); // <--- Used here too
                println!("Peers:  {}", status.peer_count);
                println!("\n{:<20} | {:<10}", "PEER ID", "NETWORK");
                println!("{}", "-".repeat(35));
                for id in status.known_peers {
                    println!("{:<20} | ONLINE", id);
                }
                println!();
            } else {
                println!("LAN Net Sync not enabled for this session.");
            }
        }
        Commands::Exit | Commands::Quit => {
            // Handled by the loop break
        }
        Commands::Serve { .. } => bail!("Server already running"),
        // _ => bail!("Command not supported in this mode"),
    }

    // if show_time {
    //     eprintln!("Execution time: {:?}", start_time.elapsed());
    // }

    Ok(())
}

fn format_sync_status(status: SyncStatus) -> &'static str {
    match status {
        SyncStatus::Idle => "Idle",
        SyncStatus::Connected => "Online",
        SyncStatus::Syncing => "Syncing",
    }
}

fn run_server(
    cli: &Cli,
    port: Option<u16>,
    node_id: &str,
    key: &str,
    discovery: DiscoveryModeArg,
    bind_addr: Option<&str>,
    server_url: Option<&str>,
    room_name: Option<&str>,
    token: &str,
) -> Result<()> {
    let rt = tokio::runtime::Runtime::new()?;

    rt.block_on(async move {
        let db = Arc::new(open_db(cli)?);

        // 1. Initialize LAN Net Sync (if --port provided)
        #[cfg(feature = "net-sync")]
        let net_syncer = if let Some(p) = port {
            let mode = match discovery {
                DiscoveryModeArg::Mdns => DiscoveryMode::Mdns,
                DiscoveryModeArg::Broadcast => DiscoveryMode::Broadcast,
                DiscoveryModeArg::Both => DiscoveryMode::Both,
            };
            let syncer = NetSyncer::new(db.clone(), node_id, key, vec!["app_state".to_string()])
                .with_discovery(mode);
            syncer.start(p).await.map_err(|e| anyhow!(e.to_string()))?;
            Some(syncer)
        } else {
            None
        };

        // 2. Initialize Cloud Sync (if --bind or --server provided)
        #[cfg(feature = "cloud-sync")]
        let cloud_syncer = if let Some(b) = bind_addr {
            // Server mode is room-agnostic: it hosts any room. Clients pick the
            // room (and this server); the server stores each room under its own
            // storage prefix. Room name/key are not needed here.
            let cs = CloudSync::server(db.clone(), node_id, token);
            cs.start(b).await.map_err(|e| anyhow!(e.to_string()))?;
            Some((cs, format!("Server ({})", b)))
        } else if let Some(s) = server_url {
            let room = room_name.unwrap_or("default");
            let cs = CloudSync::client(db.clone(), node_id, room, key, token);
            cs.start(s).await.map_err(|e| anyhow!(e.to_string()))?;
            Some((cs, format!("Client -> {} (room: {})", s, room)))
        } else {
            None
        };

        println!("🔥 FireLite v0.7.1 Server Active");
        println!("🆔 Node ID: {}", node_id);

        #[cfg(feature = "net-sync")]
        if let Some(p) = port {
            println!("🌐 LAN Net Sync: Active on port {}", p);
        }

        #[cfg(feature = "cloud-sync")]
        if let Some((_, ref desc)) = cloud_syncer {
            println!("☁️  Cloud Sync: Active [{}]", desc);
        }

        let mut rl = DefaultEditor::new().map_err(|e| anyhow!("Readline error: {}", e))?;

        loop {
            let mut status_str = String::from("Standalone");

            #[cfg(feature = "net-sync")]
            if let Some(ref net) = net_syncer {
                let status = net.status();
                status_str = format!("LAN:{} (Peers:{})", format_sync_status(status.status), status.peer_count);
            }

            #[cfg(feature = "cloud-sync")]
            if let Some((_, ref desc)) = cloud_syncer {
                if status_str == "Standalone" {
                    status_str = format!("Cloud:{}", desc);
                } else {
                    status_str = format!("Hybrid [{}+Cloud]", status_str);
                }
            }

            let prompt = format!("firelite({} | {}) > ", node_id, status_str);

            match rl.readline(&prompt) {
                Ok(line) => {
                    let line = line.trim();
                    if line.is_empty() {
                        continue;
                    }
                    if line == "exit" || line == "quit" {
                        break;
                    }

                    let _ = rl.add_history_entry(line);
                    let cmd_str = format!("firelite {}", line);
                    let args = shlex::split(&cmd_str).unwrap_or_default();

                    match Cli::try_parse_from(args) {
                        Ok(repl_cli) => {
                            #[cfg(feature = "net-sync")]
                            let net_ref = net_syncer.as_ref();
                            #[cfg(not(feature = "net-sync"))]
                            let net_ref = None;

                            if let Err(e) = execute_command(
                                &db,
                                repl_cli.command,
                                &cli.db,
                                cli.durability,
                                net_ref,
                                repl_cli.time || cli.time,
                                repl_cli.count || cli.count,
                            ) {
                                println!("❌ Error: {}", e);
                            }
                        }
                        Err(e) => println!("{}", e),
                    }
                }
                Err(ReadlineError::Interrupted) | Err(ReadlineError::Eof) => break,
                Err(err) => {
                    println!("Readline Error: {:?}", err);
                    break;
                }
            }
        }

        // #[cfg(feature = "net-sync")]
        if let Some(net) = net_syncer {
            net.stop();
        }

        // #[cfg(feature = "cloud-sync")]
        if let Some((cs, _)) = cloud_syncer {
            cs.stop();
        }

        Ok(())
    })
}

