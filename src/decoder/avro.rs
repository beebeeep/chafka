use std::{collections::HashMap, io::BufReader, sync::Arc, time::Duration};

use anyhow::anyhow;
use apache_avro::{from_avro_datum, types::Value, Schema};
use chrono::DateTime;
use clickhouse_rs::types::Value as CHValue;

use super::Row;

pub struct Decoder {
    schema: Schema,
}

pub fn from_schema(schema: String) -> Result<Decoder, anyhow::Error> {
    Ok(Decoder {
        schema: Schema::parse_str(&schema)?,
    })
}

impl super::Decoder for Decoder {
    fn get_name(&self) -> String {
        String::from("avro")
    }

    fn decode(&self, message: &[u8]) -> Result<Row, anyhow::Error> {
        let mut datum = BufReader::new(&message[5..]);
        let record;
        let mut row = Row::new();
        match from_avro_datum(&self.schema, &mut datum, None)? {
            Value::Record(x) => record = x,
            _ => return Err(anyhow!("avro message must be a record")),
        };
        for (column, value) in record {
            let v = avro2ch(value)?;
            row.push((column, v));
        }
        Ok(row)
    }
}

fn avro2ch(v: Value) -> Result<CHValue, anyhow::Error> {
    match v {
        Value::Null => Err(anyhow!("unexpected null")),
        Value::Record(_) => Err(anyhow!("unsupported nested record")),
        Value::Boolean(x) => Ok(CHValue::from(x)),
        Value::Int(x) => Ok(CHValue::from(x)),
        Value::Long(x) => Ok(CHValue::from(x)),
        Value::Float(x) => Ok(CHValue::from(x)),
        Value::Double(x) => Ok(CHValue::from(x)),
        Value::Bytes(x) => Ok(CHValue::from(x)),
        Value::String(x) => Ok(CHValue::from(x)),
        Value::Fixed(_, x) => Ok(CHValue::String(Arc::new(x))),
        Value::Enum(_, y) => Ok(CHValue::from(y)),
        Value::Union(_, _) => todo!("nullables not implemented"),
        Value::Array(x) => {
            let mut arr: Vec<CHValue> = Vec::new();
            for elem in x {
                arr.push(avro2ch(elem)?)
            }
            Ok(CHValue::Array(
                &clickhouse_rs::types::SqlType::Int32,
                Arc::new(arr),
            ))
        }
        Value::Map(x) => {
            let mut m: HashMap<String, i32> = HashMap::new();
            for (k, v) in x {
                if let Value::Int(elem) = v {
                    m.insert(k, elem);
                }
            }
            Ok(CHValue::from(m))
        }
        Value::Date(x) => Ok(CHValue::Date(x as u16)),
        Value::Decimal(_) => Err(anyhow!("unsupported decimal type")),
        Value::TimeMillis(x) => Ok(CHValue::from(x)),
        Value::TimeMicros(x) => Ok(CHValue::from(x)),
        Value::TimestampMillis(x) => Ok(CHValue::from(DateTime::from_timestamp_millis(x).unwrap())),
        Value::TimestampMicros(x) => Ok(CHValue::from(DateTime::from_timestamp_micros(x).unwrap())),
        Value::LocalTimestampMillis(x) => {
            Ok(CHValue::from(DateTime::from_timestamp_millis(x).unwrap()))
        }
        Value::LocalTimestampMicros(x) => {
            Ok(CHValue::from(DateTime::from_timestamp_micros(x).unwrap()))
        }
        Value::Duration(x) => {
            // don't ask, programmers and time ¯\_(ツ)_/¯
            let duration = Duration::from_millis(u32::from(x.millis()) as u64)
                + Duration::from_secs(86400 * u32::from(x.days()) as u64)
                + Duration::from_secs(30 * 86400 * u32::from(x.months()) as u64);
            Ok(CHValue::from(duration.as_millis() as u64))
        }
        Value::Uuid(x) => Ok(CHValue::from(x)),
    }
}
