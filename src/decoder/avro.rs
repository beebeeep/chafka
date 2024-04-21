use std::{collections::HashMap, fs, io::BufReader, sync::Arc, time::Duration};

use anyhow::{anyhow, Result};

use apache_avro::{from_avro_datum, schema::RecordSchema, types::Value, Schema};
use chrono::DateTime;
use chrono_tz::{self};

use clickhouse_rs::types::{DateTimeType, SqlType, Value as CHValue};
use reqwest::Url;
use schema_registry_api::{SchemaRegistry, SchemaVersion, SubjectName};
use serde::Deserialize;
use uuid::Uuid;

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
    name_overrides: Vec<(String, String)>,
    include_fields: Vec<String>,
    exclude_fields: Vec<String>,
    null_values: NullValues,
}

struct TypeMapping(Vec<(String, &'static SqlType)>);
struct NullValues(Vec<(String, CHValue)>);

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
            Value::Union(_, v) => match *v {
                Value::Null => match self.null_values.0.iter().find(|(n, _)| column_name.eq(n)) {
                    None => Err(anyhow!("cannot find nullable field {}", column_name)),
                    Some(x) => Ok(x.1.clone()),
                },
                v => self.avro2ch(column_name, v),
            },
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
            if self.exclude_fields.iter().any(|c| c == column.as_str()) {
                continue;
            }
            if !self.include_fields.is_empty()
                && !self.include_fields.iter().any(|c| c == column.as_str())
            {
                continue;
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
                Schema::Array(v) => arr_types.push((field.name.clone(), get_schema_type(v)?.0)),
                Schema::Map(v) => map_types.push((field.name.clone(), get_schema_type(v)?.0)),
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
    let mut include_fields: Vec<String> = Vec::new();
    let mut exclude_fields: Vec<String> = Vec::new();
    if let Some(names) = settings.field_names {
        names
            .iter()
            .for_each(|(k, v)| name_overrides.push((k.to_owned(), v.to_owned())));
    }
    if let Some(flds) = settings.include_fields {
        flds.iter().for_each(|e| include_fields.push(e.to_owned()));
    }
    if let Some(flds) = settings.exclude_fields {
        flds.iter().for_each(|e| exclude_fields.push(e.to_owned()));
    }
    match schema {
        Schema::Record(record) => {
            let (array_types, map_types, null_values) = analyze_schema(&record)?;
            Ok(Decoder {
                array_types,
                map_types,
                schema: Schema::Record(record),
                name_overrides,
                include_fields,
                exclude_fields,
                null_values,
            })
        }
        _ => Err(anyhow!("avro schema root must be a record")),
    }
}

async fn get_schema(topic: &str, settings: &Settings) -> Result<Schema> {
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

/// checks schema for compatibility and returns type mappings for arrays and maps,
/// as well as mapping of nullable fields to its zero values
fn analyze_schema(s: &RecordSchema) -> Result<(TypeMapping, TypeMapping, NullValues)> {
    let (array_types, map_types) = TypeMapping::new(s)?;
    let mut null_values: Vec<(String, CHValue)> = Vec::new();
    for fld in &s.fields {
        match &fld.schema {
            Schema::Record(_) => {
                return Err(anyhow!(
                    "field {}: nested records are not supported",
                    fld.name
                ))
            }
            Schema::Union(union) => {
                let schemas = union.variants();
                if schemas.len() != 2 {
                    return Err(anyhow!(
                        "field {}: only supported union type is [null, <type>]",
                        fld.name
                    ));
                }
                match schemas[0] {
                    Schema::Null => (),
                    _ => {
                        return Err(anyhow!(
                            "field {}: only supported union type is [null, <type>]",
                            fld.name
                        ))
                    }
                }
                null_values.push((fld.name.clone(), get_schema_type(&schemas[1])?.1))
            }
            _ => (),
        }
    }
    Ok((array_types, map_types, NullValues(null_values)))
}

/// translates avro type into clickhouse type
/// and returns relevant SqlType and its zero value
fn get_schema_type(s: &Schema) -> Result<(&'static SqlType, CHValue)> {
    match s {
        Schema::Boolean => Ok((&SqlType::Bool, CHValue::from(false))),
        Schema::Int => Ok((&SqlType::Int32, CHValue::Int32(0))),
        Schema::Long => Ok((&SqlType::Int64, CHValue::Int64(0))),
        Schema::Float => Ok((&SqlType::Float32, CHValue::Float32(0.0))),
        Schema::Double => Ok((&SqlType::Float64, CHValue::Float64(0.0))),
        Schema::Bytes => Ok((&SqlType::String, CHValue::from(Vec::<u8>::new()))),
        Schema::String => Ok((&SqlType::String, CHValue::from(String::new()))),
        Schema::Uuid => Ok((&SqlType::Uuid, CHValue::from(Uuid::nil()))),
        //Schema::Fixed(s) => Ok((&SqlType::FixedString(s.size))),
        Schema::Date => Ok((&SqlType::Date, CHValue::Date(0u16))),
        Schema::TimeMillis => Ok((&SqlType::Int32, CHValue::DateTime(0, chrono_tz::UTC))),
        Schema::TimeMicros => Ok((&SqlType::Int32, CHValue::DateTime(0, chrono_tz::UTC))),
        Schema::TimestampMillis => Ok((
            &SqlType::DateTime(DateTimeType::DateTime64(3, chrono_tz::UTC)),
            CHValue::DateTime64(0, (3, chrono_tz::UTC)),
        )),
        Schema::TimestampMicros => Ok((
            &SqlType::DateTime(DateTimeType::DateTime64(6, chrono_tz::UTC)),
            CHValue::DateTime64(0, (6, chrono_tz::UTC)),
        )),
        Schema::LocalTimestampMillis => Ok((
            &SqlType::DateTime(DateTimeType::DateTime64(3, chrono_tz::UTC)),
            CHValue::DateTime64(0, (3, chrono_tz::UTC)),
        )),
        Schema::LocalTimestampMicros => Ok((
            &SqlType::DateTime(DateTimeType::DateTime64(6, chrono_tz::UTC)),
            CHValue::DateTime64(0, (6, chrono_tz::UTC)),
        )),
        Schema::Duration => Ok((&SqlType::Int64, CHValue::UInt64(0))),
        _ => Err(anyhow!("unsupported type")),
    }
}
