use firelite::config::FireLiteConfig;
use firelite::document::firelite_doc::FireLiteDoc;
use firelite::document::value::Value;
use firelite::engine::FireLite;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = FireLite::open(".firelite-example", FireLiteConfig::default())?;
    let mut doc = FireLiteDoc::default();
    doc.insert("name", Value::String("alice".to_string()));
    doc.insert("age", Value::Int(30));
    db.put("users", "1", &doc)?;
    let loaded = db.get("users", "1")?;
    println!("{:?}", loaded);
    Ok(())
}
