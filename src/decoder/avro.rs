use std::{collections::HashMap, fs, io::BufReader, sync::Arc, time::Duration};

use anyhow::{anyhow, Result};

use apache_avro::{from_avro_datum, schema::RecordSchema, types::Value, Schema};
use chrono::DateTime;
use chrono_tz;

use clickhouse_rs::types::{DateTimeType, SqlType, Value as CHValue};
use reqwest::Url;
use schema_registry_api::{SchemaRegistry, SchemaVersion, SubjectName};
use serde::Deserialize;

use super::{Row, CONFLUENT_HEADER_LEN};

#[derive(Deserialize)]
pub struct Settings {
    pub field_names: Option<HashMap<String, String>>,
    pub include_fields: Option<Vec<String>>,
    pub exclude_fields: Option<Vec<String>>,
    pub schema_file: Option<String>,
    pub registry_url: Option<String>,
}

pub struct Decoder {
    schema: Schema,
    array_types: TypeMapping,
    map_types: TypeMapping,
    settings: Settings,
    name_overrides: Vec<(String, String)>,
}

struct TypeMapping(Vec<(String, &'static SqlType)>);

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

    fn decode(&self, message: &[u8]) -> Result<Row> {
        let mut datum = BufReader::new(&message[CONFLUENT_HEADER_LEN..]);
        let record;
        let mut row = Row::new();
        match from_avro_datum(&self.schema, &mut datum, None)? {
            Value::Record(x) => record = x,
            _ => return Err(anyhow!("avro message must be a record")),
        };
        for (column, value) in record {
            if let Some(excl) = &self.settings.exclude_fields {
                if excl.iter().any(|c| c == column.as_str()) {
                    continue;
                }
            }
            if let Some(incl) = &self.settings.include_fields {
                if !incl.iter().any(|c| c == column.as_str()) {
                    continue;
                }
            }
            let v = self.avro2ch(&column, value)?;
            let column_name = match self.name_overrides.iter().find(|m| m.0 == column) {
                None => column,
                Some((_, n)) => n.to_owned(),
            };
            row.push((column_name, v));
        }
        Ok(row)
    }
}

impl TypeMapping {
    fn new(r: &RecordSchema) -> Result<(Self, Self)> {
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

pub async fn new(topic: &str, settings: Settings) -> Result<Decoder> {
    let schema = get_schema(&topic, &settings).await?;
    let mut name_overrides: Vec<(String, String)> = Vec::new();
    if let Some(names) = &settings.field_names {
        names
            .iter()
            .for_each(|(k, v)| name_overrides.push((k.to_owned(), v.to_owned())));
    }
    match schema {
        Schema::Record(record) => {
            let (array_types, map_types) = TypeMapping::new(&record)?;
            Ok(Decoder {
                array_types,
                map_types,
                schema: Schema::Record(record),
                settings: settings,
                name_overrides,
            })
        }
        _ => Err(anyhow!("avro schema root must be a record")),
    }
}

pub async fn get_schema(topic: &str, settings: &Settings) -> Result<Schema> {
    match &settings.schema_file {
        Some(f) => Ok(Schema::parse_str(&fs::read_to_string(f)?)?),
        None => match &settings.registry_url {
            None => Err(anyhow!("registry_url or schema_file must be specified")),
            Some(registry_url) => {
                let sr_client = SchemaRegistry::build_default(Url::parse(&registry_url)?)?;
                let subject_name = format!("{topic}-value").parse::<SubjectName>()?;
                let subject = sr_client
                    .subject()
                    .version(&subject_name, SchemaVersion::Latest)
                    .await?;
                match subject {
                    None => Err(anyhow!("subject {} not found", subject_name)),
                    Some(s) => Ok(Schema::parse_str(&s.schema)?),
                }
            }
        },
    }
}

fn get_pod_sqltype(s: &Schema) -> Result<&'static SqlType> {
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
