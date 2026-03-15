use firelite::config::FireLiteConfig;
use firelite::document::firelite_doc::FireLiteDoc;
use firelite::document::value::Value;
use firelite::engine::FireLite;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = FireLite::open(".firelite-bench", FireLiteConfig::default())?;
    for i in 0..1_000 {
        let mut doc = FireLiteDoc::default();
        doc.insert("id", Value::Int(i));
        db.put("bench", &i.to_string(), &doc)?;
    }
    Ok(())
}
