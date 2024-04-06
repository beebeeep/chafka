pub mod avro;
pub mod example;
use clickhouse_rs::types::Value;

pub type Row = Vec<(String, Value)>;

pub trait Decoder {
    fn get_name(&self) -> String;
    fn decode(&self, message: &[u8]) -> Result<Row, anyhow::Error>;
}
