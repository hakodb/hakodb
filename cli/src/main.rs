use std::str::FromStr;
use std::io::Read;
use std::sync::Arc;

use anyhow::{anyhow, bail, Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use firelite::config::{DurabilityMode, FireLiteConfig};
use firelite::document::firelite_doc::FireLiteDoc;
use firelite::document::value::Value;
use firelite::engine::{FireLite, BatchMutation};
use firelite::index::composite::definition::SortDirection;
use firelite::query::filter::Operator;
use firelite::query::query::{AggregateOp, Query};
use firelite::net_sync::{NetSyncer, SyncStatus};
use serde_json::{json, Map, Value as JsonValue};
use rustyline::DefaultEditor;
use rustyline::error::ReadlineError;

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
        data: Option<String>,/// Read JSON payload from file
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
        #[arg(long)]
        order: Option<String>,
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
        /// Write JSON output to file path
        #[arg(long)]
        output: Option<String>,
        /// Mass delete the results of this query
        #[arg(long)]
        delete: bool,
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
    Seed {
        collection: String,
        docsize: usize,
    },
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
    }
}

#[derive(Subcommand, Debug)]
enum IndexCommands {
    Create { collection: String, field: String },
    CreateComposite {
        collection: String,
        /// Comma separated list: field[:asc|desc],field[:asc|desc]
        #[arg(long)]
        fields: String,
    },
    CreateFts { collection: String, field: String },
    List {
        /// Optional collection filter
        collection: Option<String>,
    },
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum AggregateKindArg {
    Count,
    Sum,
    Avg,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    
    if let Commands::Serve { port, node_id, key } = cli.command {
        return run_server(cli.db, cli.durability, port, node_id, key);
    }

    let db = open_db(&cli.db, cli.durability)?;
    execute_command(&db, cli.command, &cli.db, cli.durability, None)
}

fn open_db(path: &str, durability: DurabilityArg) -> Result<FireLite> {
    let mut cfg = FireLiteConfig::default();
    cfg.durability_mode = match durability {
        DurabilityArg::Always => DurabilityMode::Always,
        DurabilityArg::Interval => DurabilityMode::Interval,
        DurabilityArg::Manual => DurabilityMode::Manual,
        DurabilityArg::OnCommit => DurabilityMode::OnCommit,
    };
    FireLite::open(path, cfg).with_context(|| format!("failed to open db at {path}"))
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
        std::fs::write(path, rendered).with_context(|| format!("failed to write output file: {path}"))?;
    } else {
        println!("{rendered}");
    }
    Ok(())
}

fn list_collections(db: &FireLite) -> Result<()> {
    let cols = db.list_collections()?;
    println!("{}", serde_json::to_string_pretty(&cols)?);
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

fn get_doc(db: &FireLite, path: &str, output: Option<&str>, is_set: bool, data: Option<&str>) -> Result<()> {
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
    if is_set  {
        set_doc(db, path, data.expect("Should not empty"), true, false)?;
    }
    emit_json(&out, output)?;
    Ok(())
}

fn set_doc(db: &FireLite, path: &str, data: &str, merge: bool, is_batch: bool ) -> Result<()> {
    if is_batch {
        let collection = path; // In batch mode, path is just the collection name
        let array: JsonValue = serde_json::from_str(data).context("Batch data must be a JSON array")?;
        let items = array.as_array().ok_or_else(|| anyhow!("--batch requires a JSON array of objects"))?;
        
        let mut mutations = Vec::with_capacity(items.len());
        let mut results = Vec::new();

        for (idx, item) in items.iter().enumerate() {
            let obj = item.as_object().ok_or_else(|| anyhow!("Item at index {} is not an object", idx))?;
            
            // Generate ID if missing
            let doc_id = obj.get("id")
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

            for (k, v) in raw_data.as_object().ok_or_else(|| anyhow!("Data for item {} is not an object", idx))? {
                if k == "id" && obj.get("data").is_none() { continue; } // Skip ID if it's top level
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
            let obj = payload
                .as_object()
                .ok_or_else(|| anyhow!("data must be a JSON object when multiple path fields are provided"))?;
            for field in fields {
                if let Some(v) = obj.get(&field) {
                    doc.insert(field, json_to_fire(v.clone())?);
                }
            }
        }
    
        let id = db.put(collection, doc_id, &doc)?;
        println!("OK: {collection}/{id}");
    }
    Ok(())
}

// fn delete_doc(db: &FireLite, path: &str) -> Result<()> {
//     let (collection, doc_id) = split_doc_path(path)?;
//     db.delete(collection, doc_id)?;
//     println!("OK: deleted {collection}/{doc_id}");
//     Ok(())
// }

fn delete_doc(db: &FireLite, path: &str, is_batch: bool, data: Option<&str>) -> Result<()> {
    if is_batch {
        let collection = path;
        let id_input = data.ok_or_else(|| anyhow!("Batch delete requires --data with an array of IDs"))?;
        let array: JsonValue = serde_json::from_str(id_input)?;
        let ids = array.as_array().ok_or_else(|| anyhow!("--data must be an array of ID strings"))?;

        let mutations: Vec<_> = ids.iter().filter_map(|v| v.as_str()).map(|id| {
            firelite::engine::BatchMutation::Delete {
                collection: collection.to_string(),
                doc_id: id.to_string(),
            }
        }).collect();

        let count = mutations.len();
        db.write_batch(mutations)?;
        println!("OK: deleted {} documents from {}", count, collection);
    } else {
        let (collection, doc_id) = split_doc_path(path)?;
        let id = db.delete(collection, doc_id)?;
        println!("OK: deleted {collection}/{id}");
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
    println!("OK: transaction committed for {collection}/{}", ids.into_iter().next().unwrap_or_default());
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
    order: Option<&str>,
    limit: Option<usize>,
    offset: Option<usize>,
    start_at: Option<&str>,
    start_after: Option<&str>,
    end_at: Option<&str>,
    end_before: Option<&str>,
    select: Option<&str>,
    output: Option<&str>,
    delete_action: bool,
    set_action: bool,
    action_data: Option<&str>,
) -> Result<()> {
    let mut q = Query::new(collection);

    for f in filters {
        let parsed = parse_filter(f)?;
        q = q.where_filter(&parsed.field, parsed.op, parsed.value);
    }
    for f in and_filters {
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

    if let Some(order) = order {
        let (field, asc) = parse_order(order)?;
        q = q.order_by(field, asc);
    }
    if let Some(limit) = limit {
        q = q.limit(limit);
    }
    if let Some(offset) = offset {
        q = q.offset(offset);
    }
    if let Some(v) = start_at {
        q.start_at = Some(parse_cursor_values(v)?);
    }
    if let Some(v) = start_after {
        q.start_after = Some(parse_cursor_values(v)?);
    }
    if let Some(v) = end_at {
        q.end_at = Some(parse_cursor_values(v)?);
    }
    if let Some(v) = end_before {
        q.end_before = Some(parse_cursor_values(v)?);
    }


    if delete_action || set_action {

        let rows = db.query(q.clone())?;
    
        if delete_action {
            if rows.is_empty() {
                println!("No documents found matching the criteria. Nothing deleted.");
                return Ok(());
            }
            
            let mutations: Vec<_> = rows.iter().map(|(id, _)| firelite::engine::BatchMutation::Delete {
                collection: collection.to_string(),
                doc_id: id.clone(),
            }).collect();
    
            let count = mutations.len();
            db.write_batch(mutations)?;
            println!("OK: mass deleted {} documents from {}", count, collection);
            return Ok(());
        }
    
        if set_action {
            if rows.is_empty() {
                println!("No documents found matching the criteria. Nothing updated.");
                return Ok(());
            }
    
            let raw_json = action_data.ok_or_else(|| anyhow!("--set requires --data with a JSON object"))?;
            let payload: JsonValue = serde_json::from_str(raw_json).context("Action data must be a valid JSON object")?;
            let update_map = payload.as_object().ok_or_else(|| anyhow!("Action data must be a JSON object"))?;
            
            let mut updates = Vec::new();
            for (k, v) in update_map {
                updates.push((k.clone(), json_to_fire(v.clone())?));
            }
    
            let mutations: Vec<_> = rows.iter().map(|(id, _)| firelite::engine::BatchMutation::Patch {
                collection: collection.to_string(),
                doc_id: id.clone(),
                updates: updates.clone(),
            }).collect();
    
            let count = mutations.len();
            db.write_batch(mutations)?;
            println!("OK: mass updated {} documents in {}", count, collection);
            return Ok(());
        }
    }


    if let Some(select) = select {
        let fields: Vec<String> = select
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(ToString::to_string)
            .collect();
        let rows = db.query_projected_zero_copy(q, &fields)?;
        let json_rows: Vec<JsonValue> = rows
            .into_iter()
            .map(|(id, fields)| projected_to_json(&id, fields))
            .collect();
        emit_json(&JsonValue::Array(json_rows), output)?;
    } else {
        let rows = db.query(q)?;
        let json_rows: Vec<JsonValue> = rows
            .into_iter()
            .map(|(id, doc)| doc_to_json(&id, &doc))
            .collect();
        emit_json(&JsonValue::Array(json_rows), output)?;
    }
    Ok(())
}

fn run_aggregate(
    db: &FireLite,
    collection: &str,
    kind: AggregateKindArg,
    field: Option<&str>,
    filters: &[String],
) -> Result<()> {
    let mut q = Query::new(collection);
    for f in filters {
        let parsed = parse_filter(f)?;
        q = q.where_filter(&parsed.field, parsed.op, parsed.value);
    }
    q = match kind {
        AggregateKindArg::Count => q.aggregate(AggregateOp::Count),
        AggregateKindArg::Sum => q.aggregate(AggregateOp::Sum(
            field.ok_or_else(|| anyhow!("--field is required for sum"))?.to_string(),
        )),
        AggregateKindArg::Avg => q.aggregate(AggregateOp::Avg(
            field.ok_or_else(|| anyhow!("--field is required for avg"))?.to_string(),
        )),
    };
    let out = db.execute_aggregation(q)?;
    println!("{}", serde_json::to_string_pretty(&out)?);
    Ok(())
}

fn watch_collection(db: &FireLite, collection: &str) -> Result<()> {
    println!("Watching [{collection}] ... Ctrl+C to exit");
    let rx = db.watch_collection(collection);
    while let Ok(event) = rx.recv() {
        println!("{} {}", format!("{:?}", event.kind).to_uppercase(), event.path);
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
                ("flags".into(), Value::Array(vec![Value::Bool(valid), Value::Bool(i % 5 == 0)])),
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
                    db, parts[0], filters, &[], &[], None, None, None, None, None, None, None, None, None, None, false, false, None
                ),
                2 => get_doc(db, path, None, false, None),
                _ => bail!("unsupported path depth for GET"),
            }
        }
        "POST" | "PUT" => set_doc(
            db,
            path,
            data.ok_or_else(|| anyhow!("--data required for {method}"))?,
            false,
            false,
        ),
        "PATCH" => set_doc(
            db,
            path,
            data.ok_or_else(|| anyhow!("--data required for PATCH"))?,
            true,
            false
        ),
        "DELETE" => delete_doc(db, path, false, Some(" ")),
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
    if field.is_empty() {
        bail!("filter field cannot be empty");
    }
    let op = parse_operator(parts[1])?;
    let value = parse_literal_for_operator(&op, strip_wrapping_quotes(parts[2]).trim())?;
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
    for token in input.split(',').map(str::trim).filter(|v| !v.is_empty()) {
        out.push(parse_literal(strip_wrapping_quotes(token))?);
    }
    if out.is_empty() {
        bail!("cursor values cannot be empty");
    }
    Ok(out)
}

fn parse_literal_for_operator(op: &Operator, input: &str) -> Result<Value> {
    match op {
        Operator::Contains | Operator::StartsWith | Operator::Match => {
            Ok(Value::String(input.to_string()))
        }
        _ => parse_literal(input),
    }
}

fn parse_literal(input: &str) -> Result<Value> {
    let lower = input.to_ascii_lowercase();
    if lower == "null" {
        return Ok(Value::Null);
    }
    if lower == "true" {
        return Ok(Value::Bool(true));
    }
    if lower == "false" {
        return Ok(Value::Bool(false));
    }
    if input == "__SERVER_TIMESTAMP__" {
        return Ok(Value::ServerTimestamp);
    }
    if let Ok(i) = i64::from_str(input) {
        return Ok(Value::Int(i));
    }
    if let Ok(f) = f64::from_str(input) {
        return Ok(Value::Float(f));
    }
    if let Ok(json) = serde_json::from_str::<JsonValue>(input) {
        return json_to_fire(json);
    }
    Ok(Value::String(input.to_string()))
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
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    format!("{:x}", now)
}

// 1. Move the match logic into a reusable function
fn execute_command(
    db: &FireLite, 
    command: Commands, 
    _db_path: &str, 
    _durability: DurabilityArg,
    net: Option<&NetSyncer>
) -> Result<()> {
    match command {
        Commands::Collections => list_collections(db)?,
        Commands::Get { path, output, set, fromfile, data } => {
            let payload = if set {
                Some(read_payload_input(data.as_deref(), fromfile.as_deref())?)
            } else {
                None
            };
            get_doc(db, &path, output.as_deref(), set, payload.as_deref())?
        },
        Commands::Set { path, data, fromfile, batch } => {
            let payload = read_payload_input(data.as_deref(), fromfile.as_deref())?;
            set_doc(db, &path, &payload, false, batch.unwrap_or(false))?
        }
        Commands::Update { path, data, fromfile, batch } => {
            let payload = read_payload_input(data.as_deref(), fromfile.as_deref())?;
            set_doc(db, &path, &payload, true, batch.unwrap_or(false))?
        }
        Commands::Delete { path, batch, data } => delete_doc(db, &path, batch.unwrap_or(false), data.as_deref())?,
        Commands::Query { collection, filters, and_filters, or_filters, fts, order, limit, offset, start_at, start_after, end_at, end_before, select, output, delete, set, data, fromfile } => {
            
            let payload = if set {
                Some(read_payload_input(data.as_deref(), fromfile.as_deref())?)
            } else {
                None
            };
            run_query(db, &collection, &filters, &and_filters, &or_filters, fts.as_deref(), order.as_deref(), limit, offset, start_at.as_deref(), start_after.as_deref(), end_at.as_deref(), end_before.as_deref(), select.as_deref(), output.as_deref(), delete, set, payload.as_deref())?
        }
        Commands::Aggregate { collection, kind, field, filters } => run_aggregate(db, &collection, kind, field.as_deref(), &filters)?,
        Commands::Watch { collection } => watch_collection(db, &collection)?,
        Commands::Seed { collection, docsize } => seed_collection(db, &collection, docsize)?,
        Commands::Index { command } => match command {
            IndexCommands::Create { collection, field } => {
                db.create_index(&collection, &field)?;
                println!("OK: created index");
            }
            IndexCommands::CreateComposite { collection, fields } => {
                let parts = parse_composite_fields(&fields)?;
                db.create_composite_index(&collection, parts);
            }
            IndexCommands::CreateFts { collection, field } => db.create_fts_index(&collection, &field)?,
            IndexCommands::List { collection } => {
                let out = db.list_indexes(collection.as_deref());
                println!("{}", serde_json::to_string_pretty(&out)?);
            }
        },
        Commands::TxSet { path, data, fromfile } => {
            let payload = read_payload_input(data.as_deref(), fromfile.as_deref())?;
            run_tx_set(db, &path, &payload)?
        }
        Commands::Stats => println!("{}", serde_json::to_string_pretty(&db.get_stats())?),
        Commands::Compact => db.compact()?,
        Commands::Rest { method, path, data, filters } => run_rest(db, &method, &path, data.as_deref(), &filters)?,
        // For Serve, we handle it separately to avoid infinite recursion
        Commands::Peers => {
            let status = net.ok_or_else(|| anyhow!("Networking not active."))?.status();
            println!("\n--- Mesh Network ---");
            println!("Status: {}", format_sync_status(status.status)); // <--- Used here too
            println!("Peers:  {}", status.peer_count);
            println!("\n{:<20} | {:<10}", "PEER ID", "NETWORK");
            println!("{}", "-".repeat(35));
            for id in status.known_peers {
                println!("{:<20} | ONLINE", id);
            }
            println!();
        }
        Commands::Exit | Commands::Quit => {
            // Handled by the loop break
        }
        Commands::Serve { .. } => bail!("Server already running"),
        // _ => bail!("Command not supported in this mode"),
    }
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
    db_path: String,
    durability: DurabilityArg,
    port: u16,
    node_id: String,
    key: String,
) -> Result<()> {
    let rt = tokio::runtime::Runtime::new()?;

    rt.block_on(async move {
        let db = Arc::new(open_db(&db_path, durability)?);
        
        // NEW: Init Mesh Syncer
        let net = NetSyncer::new(
            db.clone(),
            &node_id,
            &key,
            vec!["app_state".to_string()],
        );

        net.start(port).await.map_err(|e| anyhow!(e.to_string()))?;

        println!("🔥 FireLite v0.6.19 Mesh Shell Active");
        println!("🌐 Node: {} | Room Hash Verified", node_id);
        
        let mut rl = DefaultEditor::new().map_err(|e| anyhow!("Readline error: {}", e))?;
        
        loop {
            let status = net.status(); 
            let prompt = format!(
                "firelite({}:{} | {}) > ", 
                node_id, 
                status.peer_count, 
                format_sync_status(status.status) // <--- Now SyncStatus is used!
            );
            
            match rl.readline(&prompt) {
                Ok(line) => {
                    let line = line.trim();
                    if line.is_empty() { continue; }
                    if line == "exit" || line == "quit" { break; } 
                    
                    let _ = rl.add_history_entry(line);
                    let cmd_str = format!("firelite {}", line);
                    let args = shlex::split(&cmd_str).unwrap_or_default();

                    match Cli::try_parse_from(args) {
                        Ok(repl_cli) => {
                            if let Err(e) = execute_command(&db, repl_cli.command, &db_path, durability, Some(&net)) {
                                println!("❌ Error: {}", e);
                            }
                        }
                        Err(e) => println!("{}", e),
                    }
                }
                Err(ReadlineError::Interrupted) | Err(ReadlineError::Eof) => break,
                Err(err) => { println!("Readline Error: {:?}", err); break; }
            }
        }
        net.stop();
        Ok(())
    })
}