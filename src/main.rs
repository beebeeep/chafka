use clickhouse_rs::{row, types::Block, Pool};
use rdkafka::{
    consumer::{Consumer, StreamConsumer},
    ClientConfig, Message, Offset, TopicPartitionList,
};
use serde::Deserialize;
use std::{collections::HashMap, time::Duration};
use uuid::Uuid;

async fn insert_batch(pool: &Pool, batch: &Vec<Vec<u8>>) -> Result<(), anyhow::Error> {
    let mut ch = pool.get_handle().await?;
    let mut block = Block::with_capacity(batch.len());
    for msg in batch {
        let row: MyRow = serde_json::from_slice(msg)?;
        block.push(row! {id: row.key, v: row.value})?;
    }
    ch.insert("test_chafka", block).await?;
    Ok(())
}

async fn get_batch(consumer: &StreamConsumer) -> (TopicPartitionList, Vec<Vec<u8>>) {
    let mut batch: Vec<Vec<u8>> = Vec::new();
    let mut topic_map: HashMap<(String, i32), Offset> = HashMap::new();
    while batch.len() < 10 {
        match tokio::time::timeout(Duration::from_secs(3), consumer.recv()).await {
            Err(e) => {
                eprintln!("got timeout: {e}");
                break;
            }
            Ok(Err(e)) => {
                eprintln!("error receiving message: {e}");
                break;
            }
            Ok(Ok(msg)) => {
                let k = (msg.topic().to_string(), msg.partition());
                let next_offset = msg.offset() + 1; //commiting _next_ message offset as per https://docs.rs/rdkafka/latest/rdkafka/consumer/trait.Consumer.html#tymethod.commit
                match topic_map.get(&k) {
                    None => topic_map.insert(k, Offset::from_raw(next_offset)),
                    Some(offset) if offset.to_raw().unwrap() < next_offset => {
                        topic_map.insert(k, Offset::from_raw(next_offset))
                    }
                    _ => None,
                };

                batch.push(msg.payload().unwrap().to_owned());
            }
        }
    }
    return (
        TopicPartitionList::from_topic_map(&topic_map).unwrap(),
        batch,
    );
}

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

    loop {
        let (tpl, batch) = get_batch(&consumer).await;
        for msg in &batch {
            println!("got message: {}", String::from_utf8_lossy(msg));
        }
        if batch.is_empty() {
            continue;
        }
        match insert_batch(&pool, &batch).await {
            Ok(()) => consumer
                .commit(&tpl, rdkafka::consumer::CommitMode::Sync)
                .unwrap_or_else(|e| eprintln!("failed to commit offsets: {e}")),
            Err(e) => eprintln!("inserting batch: {}", e),
        }
    }
}
