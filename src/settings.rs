use std::collections::HashMap;

use config::{Config, ConfigError, File};
use serde::Deserialize;

#[derive(Deserialize)]
pub struct Ingester {
    pub decoder: String,
    pub kafka_broker: String,
    pub topic: String,
    pub consumer_group: Option<String>,
    pub batch_size: Option<usize>,
    pub batch_timeout_seconds: Option<u64>,
    pub clickhouse_url: String,
    pub clickhouse_table: String,
    pub custom: Option<toml::Value>,
}

#[derive(Deserialize)]
pub struct Settings {
    pub ingesters: HashMap<String, Ingester>,
}

impl Settings {
    pub fn new(cfgfile: &str) -> Result<Self, ConfigError> {
        let cfg = Config::builder()
            .add_source(File::with_name("config/default").required(false))
            .add_source(File::with_name(cfgfile).required(true))
            .build()?;
        let mut settings: Settings = cfg.try_deserialize()?;
        for (name, cfg) in &mut settings.ingesters {
            cfg.batch_size = match cfg.batch_size {
                None => Some(1000),
                Some(x) => Some(x),
            };
            cfg.batch_timeout_seconds = match cfg.batch_timeout_seconds {
                None => Some(10),
                Some(x) => Some(x),
            };
            cfg.consumer_group = match &cfg.consumer_group {
                None => Some(name.to_owned()),
                Some(x) => Some(x.to_owned()),
            }
        }
        Ok(settings)
    }
}
