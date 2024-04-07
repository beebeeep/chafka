pub mod avro;
pub mod example;

use std::sync::Arc;

use anyhow::{anyhow, Result};

use clickhouse_rs::types::Value;

pub type Row = Vec<(String, Value)>;
pub trait Decoder {
    fn get_name(&self) -> String;
    fn decode(&self, message: &[u8]) -> Result<Row, anyhow::Error>;
}

pub fn get_decoder(name: &str) -> Result<Arc<dyn Decoder + Send + Sync>> {
    match name {
        "example" => Ok(Arc::new(example::Decoder {})),
        _ => Err(anyhow!("unknown decoder {}", name)),
    }
}
