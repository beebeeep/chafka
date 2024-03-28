use clickhouse_rs::types::Value;

pub mod ingester;

pub trait Ingester {
    fn get_name() -> String;
    fn decode(message: Vec<u8>) -> Vec<Value>;
}
