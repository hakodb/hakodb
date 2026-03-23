use firelite::FireLite;
use firelite::config::{FireLiteConfig, DurabilityMode};
use firelite::document::firelite_doc::FireLiteDoc;
use firelite::document::value::Value;
use firelite::query::query::Query;
use serde_json::{json, Map, Value as JsonValue};
use std::env;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = FireLiteConfig::default();
    config.durability_mode = DurabilityMode::OnCommit;
    
    let db = FireLite::open("./rust_persistence.db", config)?;
    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        print_usage();
        return Ok(());
    }

    let command = args[1].to_lowercase();

    match command.as_str() {
        "list" => handle_list(&db, args.get(2))?,
        "add" | "set" => handle_upsert(&db, args.get(2), args.get(3..), false)?,
        "update" => handle_upsert(&db, args.get(2), args.get(3..), true)?,
        "remove" => handle_delete(&db, args.get(2))?,
        "find" => handle_query(&db, args.get(2), args.get(3..))?,
        "watch" => handle_watch(&db, args.get(2))?,
        _ => {
            println!("Unknown command: {}", command);
            print_usage();
        }
    }

    Ok(())
}

fn print_usage() {
    println!("FireLite CLI Usage:");
    println!("  list                          - List all collections");
    println!("  list <col>                    - List all docs in collection");
    println!("  list <col>/<id>               - Get specific document");
    println!("  add  <col>[/id] <json>        - Add or overwrite a document");
    println!("  update <col>/<id> <json>      - Update fields in a document");
    println!("  remove <col>/<id>             - Delete a document");
    println!("  find <col> <key>=<val>        - Simple query");
    println!("  watch <col>                   - Real-time stream of changes");
}

fn handle_list(db: &FireLite, path: Option<&String>) -> Result<(), Box<dyn std::error::Error>> {
    match path {
        // FIX: Handle None and "/" separately to avoid "p not bound" error
        None => list_collections(db)?,
        Some(p) if p == "/" => list_collections(db)?,
        Some(p) => {
            let parts: Vec<&str> = p.split('/').filter(|s| !s.is_empty()).collect();
            if parts.len() == 1 {
                let docs = db.query(Query::new(parts[0]))?;
                let list: Vec<JsonValue> = docs.iter().map(|(id, doc)| doc_to_json(id, doc)).collect();
                println!("{}", serde_json::to_string_pretty(&list)?);
            } else {
                match db.get(parts[0], parts[1])? {
                    Some(doc) => println!("{}", serde_json::to_string_pretty(&doc_to_json(parts[1], &doc))?),
                    None => println!("404 Not Found"),
                }
            }
        }
    }
    Ok(())
}

fn list_collections(db: &FireLite) -> Result<(), Box<dyn std::error::Error>> {
    println!("Collections:");
    
    // Now calls the reference-counted O(1) method in the engine
    let collections = db.list_collections()?; 
    
    if collections.is_empty() {
        println!("  (No collections found)");
    } else {
        for col in collections {
            println!("  - {}", col);
        }
    }
    Ok(())
}

fn handle_upsert(db: &FireLite, path: Option<&String>, json_args: Option<&[String]>, is_update: bool) -> Result<(), Box<dyn std::error::Error>> {
    let p = path.ok_or("Path required (collection/id)")?;
    let raw_json = json_args.map(|s| s.join(" ")).ok_or("JSON data required")?;
    let json_val: JsonValue = serde_json::from_str(&raw_json)?;
    
    let parts: Vec<&str> = p.split('/').filter(|s| !s.is_empty()).collect();
    let col = parts[0];
    let id = if parts.len() > 1 { parts[1].to_string() } else { format!("{:x}", rand::random::<u32>()) };

    let mut doc = if is_update {
        db.get(col, &id)?.unwrap_or_default()
    } else {
        FireLiteDoc::default()
    };

    if let JsonValue::Object(map) = json_val {
        for (k, v) in map {
            doc.insert(k, json_value_to_fire_value(v)?);
        }
    }

    db.put(col, &id, &doc)?;
    println!("OK: Saved {}/{}", col, id);
    Ok(())
}

fn handle_query(db: &FireLite, col: Option<&String>, query_args: Option<&[String]>) -> Result<(), Box<dyn std::error::Error>> {
    let collection = col.ok_or("Collection name required")?;
    let query_str = query_args.and_then(|a| a.first()).ok_or("Query required (e.g. status=online)")?;
    
    if let Some((field, val)) = query_str.split_once('=') {
        let mut q = Query::new(collection);
        q = q.where_eq(field, Value::String(val.to_string()));
        
        let results = db.query(q)?;
        let json_list: Vec<JsonValue> = results.iter().map(|(id, doc)| doc_to_json(id, doc)).collect();
        println!("{}", serde_json::to_string_pretty(&json_list)?);
    }
    Ok(())
}

fn handle_watch(db: &FireLite, col: Option<&String>) -> Result<(), Box<dyn std::error::Error>> {
    let collection = col.ok_or("Collection name required")?;
    println!("Watching collection [{}]... (Ctrl+C to stop)", collection);
    
    let rx = db.watch_collection(collection);

    while let Ok(event) = rx.recv() {
        let kind_str = format!("{:?}", event.kind).to_uppercase();
        println!("[{}] {} changed", kind_str, event.path);
        
        if let Some((_, id)) = event.path.split_once(':') {
             if let Some(doc) = db.get(collection, id)? {
                 println!("  Data: {}", serde_json::to_string(&doc_to_json(id, &doc))?);
             }
        }
    }
    Ok(())
}

fn handle_delete(db: &FireLite, path: Option<&String>) -> Result<(), Box<dyn std::error::Error>> {
    let p = path.ok_or("Path required (collection/id)")?;
    let parts: Vec<&str> = p.split('/').filter(|s| !s.is_empty()).collect();
    if parts.len() < 2 { return Err("Doc ID required".into()); }
    db.delete(parts[0], parts[1])?;
    println!("Deleted.");
    Ok(())
}

fn fire_value_to_json(v: &Value) -> JsonValue {
    match v {
        Value::Null => JsonValue::Null,
        Value::Bool(b) => json!(b),
        Value::Int(i) => json!(i),
        Value::Float(f) => json!(f),
        Value::String(s) => json!(s),
        Value::Binary(b) => json!(b),
        Value::Timestamp(t) => json!(t),
        Value::ServerTimestamp => JsonValue::Null,
        Value::Map(fields) => {
            let mut map = Map::new();
            for (k, v) in fields { 
                // FIX: Use k.clone() and ensure it matches String
                map.insert(k.clone(), fire_value_to_json(v)); 
            }
            JsonValue::Object(map)
        }
    }
}

fn doc_to_json(id: &str, doc: &FireLiteDoc) -> JsonValue {
    let mut map = Map::new();
    map.insert("_id".to_string(), json!(id));
    for (k, v) in &doc.fields { 
        // FIX: Ensure k.clone() is used
        map.insert(k.clone(), fire_value_to_json(v)); 
    }
    JsonValue::Object(map)
}

fn json_value_to_fire_value(v: JsonValue) -> Result<Value, String> {
    match v {
        JsonValue::Null => Ok(Value::Null),
        JsonValue::Bool(b) => Ok(Value::Bool(b)),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() { Ok(Value::Int(i)) }
            else { Ok(Value::Float(n.as_f64().unwrap_or(0.0))) }
        },
        JsonValue::String(s) => {
            if s == "__SERVER_TIMESTAMP__" { Ok(Value::ServerTimestamp) }
            else { Ok(Value::String(s)) }
        },
        JsonValue::Object(map) => {
            let mut fields = Vec::new();
            for (mk, mv) in map { fields.push((mk, json_value_to_fire_value(mv)?)); }
            Ok(Value::Map(fields))
        },
        JsonValue::Array(a) => {
            let bytes: Vec<u8> = a.iter().filter_map(|x| x.as_u64().map(|b| b as u8)).collect();
            Ok(Value::Binary(bytes))
        }
    }
}