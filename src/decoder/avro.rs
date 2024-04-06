use apache_avro::{types::Value, Reader, Schema};

use super::Row;

pub struct Ingester {
    schema: Schema,
}

pub fn from_schema(schema: String) -> Result<Ingester, anyhow::Error> {
    Ok(Ingester {
        schema: Schema::parse_str(&schema)?,
    })
}

impl super::Decoder for Ingester {
    fn get_name(&self) -> String {
        String::from("avro")
    }

    fn decode(&self, message: &[u8]) -> Result<Row, anyhow::Error> {
        let reader = Reader::with_schema(&self.schema, message)?;
        for record in reader {
            match record? {
                Value::Null => println!("value null"),
                Value::Boolean(x) => println!("value Boolean: {x}"),
                Value::Int(x) => println!("value Int: {x}"),
                Value::Long(x) => println!("value Long: {x}"),
                Value::Float(x) => println!("value Float: {x}"),
                Value::Double(x) => println!("value Double: {x}"),
                Value::Bytes(x) => println!("value Bytes: {:?}", x),
                Value::String(x) => println!("value String: {x}"),
                Value::Fixed(x, y) => println!("value Fixed: {x} {:?}", y),
                Value::Enum(x, y) => println!("value Enum: {x} {y}"),
                Value::Union(x, y) => println!("value Union: {x} {:?}", y),
                Value::Array(x) => println!("value Array: {:?}", x),
                Value::Map(x) => println!("value Map: {:?}", x),
                Value::Record(x) => println!("value Record: {:?}", x),
                Value::Date(x) => println!("value Date: {x}"),
                Value::Decimal(x) => println!("value Decimal: {:?}", x),
                Value::TimeMillis(x) => println!("value TimeMillis: {x}"),
                Value::TimeMicros(x) => println!("value TimeMicros: {x}"),
                Value::TimestampMillis(x) => println!("value TimestampMillis: {x}"),
                Value::TimestampMicros(x) => println!("value TimestampMicros: {x}"),
                Value::LocalTimestampMillis(x) => println!("value LocalTimestampMillis: {x}"),
                Value::LocalTimestampMicros(x) => println!("value LocalTimestampMicros: {x}"),
                Value::Duration(x) => println!("value Duration: {:?}", x),
                Value::Uuid(x) => println!("value Uuid: {x}"),
            }
        }
        todo!();
    }
}
