use std::{collections::HashMap, time::Duration};

use anyhow::anyhow;
use clickhouse_rs::{Block, Pool};
use rdkafka::{
    consumer::{Consumer, StreamConsumer},
    Message, Offset, TopicPartitionList,
};
use tokio::time::sleep;

use crate::decoder::{Decoder, Row};

const CH_BACKOFF: std::time::Duration = Duration::from_secs(1);

pub struct Ingester<T: Decoder> {
    batch: Vec<Row>,
    batch_size: usize,
    batch_timeout: Duration,
    pool: Pool,
    consumer: StreamConsumer,
    decoder: T,
}

impl<T: Decoder> Ingester<T> {
    pub fn new(
        batch_size: usize,
        batch_timeout: Duration,
        pool: Pool,
        consumer: StreamConsumer,
        decoder: T,
    ) -> Ingester<T> {
        Ingester {
            batch: Vec::new(),
            batch_size,
            batch_timeout,
            pool,
            consumer,
            decoder,
        }
    }

    pub async fn start(&mut self) {
        loop {
            let tpl = self.get_batch().await;
            if tpl.count() + self.batch.len() == 0 {
                continue;
            }
            self.try_insert(tpl).await;
        }
    }

    async fn try_insert(&mut self, tpl: TopicPartitionList) {
        // keep trying insert to CH until we succeed and only after that commit offsets
        loop {
            match self.insert_batch().await {
                Ok(()) => {
                    self.consumer
                        .commit(&tpl, rdkafka::consumer::CommitMode::Sync)
                        .unwrap_or_else(|e| eprintln!("failed to commit offsets: {e}"));
                    return;
                }
                Err(e) => {
                    eprintln!("inserting batch: {}, pending rows: {}", e, self.batch.len());
                    sleep(CH_BACKOFF).await;
                }
            }
        }
    }

    async fn insert_batch(&mut self) -> Result<(), anyhow::Error> {
        if self.batch.is_empty() {
            return Ok(());
        }

        let mut ch = self.pool.get_handle().await?;
        let mut block = Block::with_capacity(self.batch.len());
        for msg in &self.batch {
            block.push(msg.to_owned())?;
        }
        match ch.insert("test_chafka", block).await {
            Ok(_) => {
                self.batch.truncate(0);
                Ok(())
            }
            Err(e) => Err(anyhow!("inserting batch to CH: {}", e)),
        }
    }

    async fn get_batch(&mut self) -> TopicPartitionList {
        let mut topic_map: HashMap<(String, i32), Offset> = HashMap::new();
        while self.batch.len() < self.batch_size {
            match tokio::time::timeout(self.batch_timeout, self.consumer.recv()).await {
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
                    match self.decoder.decode(msg.payload().unwrap()) {
                        Ok(row) => self.batch.push(row),
                        Err(err) => {
                            eprintln!("failed to decode message: {}", err)
                        }
                    };
                }
            }
        }
        TopicPartitionList::from_topic_map(&topic_map).unwrap()
    }
}
