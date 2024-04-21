//! ## chafka
//! Extensible service for real-time data ingestion from Kafka to ClickHouse.
//!
//! ## Installation
//! Just use cargo.
//!
//! ## Configuration
//! Example config:
//! ```
//! [ingesters.example]
//! decoder = "avro"                        # using generic avro decoder
//! kafka_broker = "localhost:9091"
//! topic = "test-topic"
//! batch_size = 100000
//! batch_timeout_seconds = 10
//! clickhouse_url = "tcp://localhost:9000"
//! clickhouse_table = "test_chafka_avro"
//! custom.schema_file = "./example.avsc"   # take schema from local file
//! custom.field_names = { c = "c_arr" }    # field "c" is ingested into column "c_arr"
//! ```
//!
//! ## Extending
//! While this service contains generic decoder [avro],
//! that can be used for ingesting relatively simple avro messages (without nested records),
//! this service was meant to be extended to support different serialization formats and CH tables
//! by writing own implementations of [Decoder] trait.
//! Ultimately, you may have own decoder for each topic you are ingesting.
//!
//! Refer to [example] decoder as a reference.
//!
//! [Decoder]: decoder::Decoder
//! [avro]: decoder::avro
//! [example]: decoder::example
//!
//! ## Kafka and ClickHouse
//! Chafka uses Kafka's consumer groups and performs safe offset management ---
//! it will only commit offsets of messages that have been successfully inserted into CH.
//!
//! Chafka also automatically batches inserts to CH for optimal performance.
//! Batching is controlled by batch size and batch timeout, allowing user to tune
//! ingestion process either for throughput or for latency.
use clap::Parser;
use ingester::Ingester;
use settings::Settings;
use tokio::task::JoinSet;

pub mod decoder;
pub mod ingester;
pub mod settings;

#[doc(hidden)]
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    config: String,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let settings = Settings::new(&args.config).expect("cannot load config");
    let mut ingesters = JoinSet::new();
    for (name, cfg) in settings.ingesters {
        ingesters.spawn(async move {
            let mut ingester = Ingester::new(cfg)
                .await
                .unwrap_or_else(|e| panic!("failed to create ingester {name}: {:#}", e));
            ingester.start().await;
        });
    }
    while let Some(_) = ingesters.join_next().await {}
}
