//! Manages decoders
pub mod avro;
pub mod example;
pub mod static_avro_example;

use std::sync::Arc;

use anyhow::{anyhow, Result};

use clickhouse_rs::types::Value;

/// Confluent [header](https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format) length
pub const CONFLUENT_HEADER_LEN: usize = 5;

/// ClickHouse row - vector of columns, each column is tuple of its name and value
pub type Row = Vec<(String, Value)>;

/// Decoder converts binary message from Kafka into ClickHouse row
pub trait Decoder {
    fn get_name(&self) -> String;
    fn decode(&self, message: &[u8]) -> Result<Row, anyhow::Error>;
}

/// Creates decoder of specified name.
/// If you add your own decoders, register them here
pub async fn get_decoder(
    name: &str,
    decoder_settings: Option<toml::Value>,
    topic: &str,
) -> Result<Arc<dyn Decoder + Send + Sync>, anyhow::Error> {
    match name {
        "example" => Ok(Arc::new(example::Decoder {})),
        "avro" => match decoder_settings {
            Some(s) => Ok(Arc::new(avro::new(topic, s.try_into()?).await?)),
            None => Err(anyhow!("avro config missing")),
        },
        "test-avro" => Ok(Arc::new(static_avro_example::new()?)),
        _ => Err(anyhow!("unknown decoder {}", name)),
    }
}
