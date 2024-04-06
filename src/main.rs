use clickhouse_rs::Pool;
use decoder::example;
use ingester::Ingester;
use rdkafka::{
    consumer::{Consumer, StreamConsumer},
    ClientConfig,
};
use std::time::Duration;

pub mod decoder;
pub mod ingester;

#[tokio::main]
async fn main() {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9091")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set("group.id", "chafka")
        .create()
        .expect("Consumer creation failed");
    consumer.subscribe(&["test-topic"]).unwrap();
    let pool = Pool::new("tcp://localhost:9000");
    let mut ingester = Ingester::new(
        10,
        Duration::from_secs(10),
        pool,
        consumer,
        example::Decoder {},
    );
    ingester.start().await;
}
