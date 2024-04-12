use clickhouse_rs::types::Value;
use serde::Deserialize;
use uuid::Uuid;

use super::Row;

pub struct Decoder;

#[derive(Deserialize)]
struct Entry {
    pub key: Uuid,
    pub value: i64,
}

impl super::Decoder for Decoder {
    fn get_name(&self) -> String {
        String::from("example")
    }
    fn decode(&self, message: &[u8]) -> Result<Row, anyhow::Error> {
        let r: Entry = serde_json::from_slice(message)?;
        Ok(vec![
            (String::from("id"), Value::from(r.key)),
            (String::from("v"), Value::from(r.value)),
        ])
    }
}
