---
title: Rust (Native Crate)
---

## Install

```bash
cargo add firelite
```

## Hello World

```rust
use firelite::config::FireLiteConfig;
use firelite::document::firelite_doc::FireLiteDoc;
use firelite::document::value::Value;
use firelite::engine::FireLite;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut cfg = FireLiteConfig::default();
    cfg.encryption_key = Some("change-me-secret".to_string());

    let db = FireLite::open(".firelite-example", cfg)?;

    let mut doc = FireLiteDoc::default();
    doc.insert("name", Value::String("alice".to_string()));
    doc.insert("age", Value::Int(30));

    db.put("users", "u1", &doc)?;
    let loaded = db.get("users", "u1")?;
    println!("loaded? {}", loaded.is_some());

    db.flush()?;
    Ok(())
}
```
