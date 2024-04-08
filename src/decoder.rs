pub mod avro;
pub mod example;
pub mod testavro;

use std::sync::Arc;

use anyhow::{anyhow, Result};

use clickhouse_rs::types::Value;

pub type Row = Vec<(String, Value)>;
pub trait Decoder {
    fn get_name(&self) -> String;
    fn decode(&self, message: &[u8]) -> Result<Row, anyhow::Error>;
}

pub fn get_decoder(name: &str) -> Result<Arc<dyn Decoder + Send + Sync>> {
    let schema = String::from(
        r#"
    {
        "type": "record",
        "name": "test",
        "fields": [
            {"name": "a", "type": "long", "default": 42},
            {"name": "b", "type": "string"},
            {"name": "c", "type": {"type": "array", "items": "int"}}
        ]
    }
"#,
    );
    match name {
        "example" => Ok(Arc::new(example::Decoder {})),
        "avro" => Ok(Arc::new(avro::from_schema(schema)?)),
        "test-avro" => Ok(Arc::new(testavro::new()?)),
        _ => Err(anyhow!("unknown decoder {}", name)),
    }
}
