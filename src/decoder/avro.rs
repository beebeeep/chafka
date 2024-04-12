use std::{collections::HashMap, io::BufReader, sync::Arc, time::Duration};

use anyhow::anyhow;
use apache_avro::{from_avro_datum, schema::RecordSchema, types::Value, Schema};
use chrono::DateTime;
use chrono_tz;

use clickhouse_rs::types::{DateTimeType, SqlType, Value as CHValue};

use super::{Row, CONFLUENT_HEADER_LEN};

pub struct Decoder {
    schema: Schema,
    array_types: TypeMapping,
    map_types: TypeMapping,
}
fn get_pod_sqltype(s: &Schema) -> Result<&'static SqlType, anyhow::Error> {
    match s {
        Schema::Boolean => Ok(&SqlType::Bool),
        Schema::Int => Ok(&SqlType::Int32),
        Schema::Long => Ok(&SqlType::Int64),
        Schema::Float => Ok(&SqlType::Float32),
        Schema::Double => Ok(&SqlType::Float64),
        Schema::Bytes => Ok(&SqlType::String),
        Schema::String => Ok(&SqlType::String),
        Schema::Uuid => Ok(&SqlType::Uuid),
        //Schema::Fixed(s) => Ok(&SqlType::FixedString(s.size)),
        Schema::Date => Ok(&SqlType::Date),
        Schema::TimeMillis => Ok(&SqlType::Int32),
        Schema::TimeMicros => Ok(&SqlType::Int64),
        Schema::TimestampMillis => Ok(&SqlType::DateTime(DateTimeType::DateTime64(
            3,
            chrono_tz::UTC,
        ))),
        Schema::TimestampMicros => Ok(&SqlType::DateTime(DateTimeType::DateTime64(
            6,
            chrono_tz::UTC,
        ))),
        Schema::LocalTimestampMillis => Ok(&SqlType::DateTime(DateTimeType::DateTime64(
            3,
            chrono_tz::UTC,
        ))),
        Schema::LocalTimestampMicros => Ok(&SqlType::DateTime(DateTimeType::DateTime64(
            6,
            chrono_tz::UTC,
        ))),
        Schema::Duration => Ok(&SqlType::Int64),
        _ => Err(anyhow!("unsupported type")),
    }
}
struct TypeMapping(Vec<(String, &'static SqlType)>);

impl TypeMapping {
    fn new(r: &RecordSchema) -> Result<(Self, Self), anyhow::Error> {
        let mut map_types: Vec<(String, &SqlType)> = Vec::new();
        let mut arr_types: Vec<(String, &SqlType)> = Vec::new();
        for field in &r.fields {
            match &field.schema {
                Schema::Array(v) => arr_types.push((field.name.clone(), get_pod_sqltype(v)?)),
                Schema::Map(v) => map_types.push((field.name.clone(), get_pod_sqltype(v)?)),
                _ => (),
            };
        }
        Ok((TypeMapping(arr_types), TypeMapping(map_types)))
    }

    fn get_type(&self, column: &str) -> Option<&'static SqlType> {
        match self.0.iter().find(|x| x.0 == column) {
            None => None,
            Some((_, t)) => Some(t),
        }
    }
}

pub fn from_schema(schema_str: String) -> Result<Decoder, anyhow::Error> {
    let schema = Schema::parse_str(&schema_str)?;
    match schema {
        Schema::Record(record) => {
            let (array_types, map_types) = TypeMapping::new(&record)?;
            Ok(Decoder {
                array_types,
                map_types,
                schema: Schema::Record(record),
            })
        }
        _ => Err(anyhow!("avro schema root must be a record")),
    }
}
impl Decoder {
    fn avro2ch(&self, column_name: &str, v: Value) -> Result<CHValue, anyhow::Error> {
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
                    arr.push(self.avro2ch(column_name, elem)?)
                }
                let column_type = self.array_types.get_type(column_name).unwrap();
                Ok(CHValue::Array(column_type, Arc::new(arr)))
            }
            Value::Map(x) => {
                let mut m: HashMap<CHValue, CHValue> = HashMap::new();
                for (k, v) in x {
                    m.insert(CHValue::from(k), self.avro2ch(column_name, v)?);
                }
                let column_type = self.map_types.get_type(column_name).unwrap();
                Ok(CHValue::Map(&SqlType::String, &column_type, Arc::new(m)))
            }
            Value::Date(x) => Ok(CHValue::Date(x as u16)),
            Value::Decimal(_) => Err(anyhow!("unsupported decimal type")),
            Value::TimeMillis(x) => Ok(CHValue::from(x)),
            Value::TimeMicros(x) => Ok(CHValue::from(x)),
            Value::TimestampMillis(x) => {
                Ok(CHValue::from(DateTime::from_timestamp_millis(x).unwrap()))
            }
            Value::TimestampMicros(x) => {
                Ok(CHValue::from(DateTime::from_timestamp_micros(x).unwrap()))
            }
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
}

impl super::Decoder for Decoder {
    fn get_name(&self) -> String {
        String::from("avro")
    }

    fn decode(&self, message: &[u8]) -> Result<Row, anyhow::Error> {
        let mut datum = BufReader::new(&message[CONFLUENT_HEADER_LEN..]);
        let record;
        let mut row = Row::new();
        match from_avro_datum(&self.schema, &mut datum, None)? {
            Value::Record(x) => record = x,
            _ => return Err(anyhow!("avro message must be a record")),
        };
        for (column, value) in record {
            let v = self.avro2ch(&column, value)?;
            row.push((column, v));
        }
        Ok(row)
    }
}
