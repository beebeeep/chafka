//! example implementation of statically-typed avro decoder
use std::io::BufReader;

use apache_avro::{from_avro_datum, from_value, Schema};
use clickhouse_rs::types::Value;
use serde::Deserialize;

use super::{Row, CONFLUENT_HEADER_LEN};

pub struct Decoder {
    schema: Schema,
}

#[derive(Deserialize)]
struct Entry {
    a: i64,
    b: String,
    c: Vec<i32>,
}

pub fn new() -> Result<Decoder, anyhow::Error> {
    Ok(Decoder {
        schema: Schema::parse_str(
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
        )?,
    })
}

impl super::Decoder for Decoder {
    fn get_name(&self) -> String {
        String::from("static-avro-example")
    }
    fn decode(&self, message: &[u8]) -> Result<Row, anyhow::Error> {
        let mut datum = BufReader::new(&message[CONFLUENT_HEADER_LEN..]);
        let v = from_avro_datum(&self.schema, &mut datum, None)?;
        let r: Entry = from_value::<Entry>(&v)?;
        Ok(vec![
            (String::from("a"), Value::from(r.a)),
            (String::from("b"), Value::from(r.b)),
            (String::from("c"), Value::from(r.c)),
        ])
    }
}
