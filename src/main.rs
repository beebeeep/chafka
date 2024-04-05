use clickhouse_rs::{types::Block, Pool};
use ingester::{example, Ingester, Row};
use rdkafka::{
    consumer::{Consumer, StreamConsumer},
    ClientConfig, Message, Offset, TopicPartitionList,
};
use std::{collections::HashMap, time::Duration};

mod ingester;

async fn insert_batch(pool: &Pool, batch: Vec<Row>) -> Result<(), anyhow::Error> {
    if batch.len() == 0 {
        return Ok(());
    }

    let mut ch = pool.get_handle().await?;
    let mut block = Block::with_capacity(batch.len());
    for msg in batch {
        block.push(msg)?;
    }
    ch.insert("test_chafka", block).await?;
    Ok(())
}

async fn get_batch(
    consumer: &StreamConsumer,
    ingester: &impl Ingester,
) -> (TopicPartitionList, Vec<Row>) {
    let mut batch: Vec<Row> = Vec::new();
    let mut topic_map: HashMap<(String, i32), Offset> = HashMap::new();
    while batch.len() < 10 {
        match tokio::time::timeout(Duration::from_secs(3), consumer.recv()).await {
            Err(_) => {
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
                match ingester.decode(msg.payload().unwrap()) {
                    Ok(row) => batch.push(row),
                    Err(err) => {
                        eprintln!("failed to decode message: {}", err)
                    }
                };
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
    let example_ingester = example::Ingester {};

    loop {
        let (tpl, batch) = get_batch(&consumer, &example_ingester).await;
        if tpl.count() == 0 {
            continue;
        }
        match insert_batch(&pool, batch).await {
            Ok(()) => {
                eprintln!("committing offsets: {:?}", tpl);
                consumer
                    .commit(&tpl, rdkafka::consumer::CommitMode::Sync)
                    .unwrap_or_else(|e| eprintln!("failed to commit offsets: {e}"))
            }
            Err(e) => eprintln!("inserting batch: {}", e),
        }
    }
}
