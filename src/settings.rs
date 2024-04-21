//! Application config
use std::collections::HashMap;

use config::{Config, ConfigError, File};
use serde::Deserialize;

/// configuration of single topic ingester
#[derive(Deserialize)]
pub struct Ingester {
    /// name of decoder to use
    pub decoder: String,
    /// address of bootstrap kafka broker
    pub kafka_broker: String,
    /// topic to ingest
    pub topic: String,
    /// consumer group to use (default: use ingester's name)
    pub consumer_group: Option<String>,
    /// max ClickHouse insert batch size (default: 1000)
    pub batch_size: Option<usize>,
    /// batching timeout (default: 10s)
    pub batch_timeout_seconds: Option<u64>,
    /// URL of ClickHouse
    pub clickhouse_url: String,
    /// ClickHouse table to ingest into
    pub clickhouse_table: String,
    /// Decoder-specific configuration
    pub custom: Option<toml::Value>,
}

#[derive(Deserialize)]
pub struct Settings {
    /// Map of ingester names and settings
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
