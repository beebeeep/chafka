Chafka
======
Extensible service for real-time data ingestion from Kafka to ClickHouse.

Motivation
==========
ClickHouse can natively consume data from Kafka using [Kafka table engine](https://clickhouse.com/docs/en/integrations/kafka#kafka-table-engine), or [Kafka Connect sink](https://clickhouse.com/docs/en/integrations/kafka/clickhouse-kafka-connect-sink).
However, these solutions have own issues - in particular, lack of flexibility and observability.  This project is a standalone service that allows you to ingest your data in safe and controllable manner while
providing full control over schema and deserialization process. 

Installation
============
Use cargo.

Configuration
=============
Example of the config can be found in example.toml.

Architecture
============
Conceptually, service consists of two main components - the ingestion core, responsible for consuming messages from Kafka and writing data to ClickHouse; and one or several "decoders" - packages implementing a
simple trait to unmarshal message from Kafka into set of ClickHouse columns. Out of the box there is a universal configurable Avro decoder, and you also may add your own. 

Kafka and ClickHouse
====================
Chafka uses Kafka's consumer groups and performs safe offset management -
it will only commit offsets of messages that have been successfully inserted into CH.

Chafka also automatically batches inserts to CH for optimal performance.
Batching is controlled by batch size and batch timeout, allowing user to tune
ingestion process either for throughput or for latency.

Delivery and consistency guarantees
===================================
The baseline is "at least once" semantics - message offset will not be committed unless CH confirmed the successful INSERT. However, keep in mind that reality is a bit more complex: written data still may be lost even after confirmation in case of disk problems or catastrophic failure of server with CH itself. There are few ways to improve durability:
* Use [replicated tables](https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/replication) (ReplicatedMergeTree etc)
* Enable more meticulous fsync in MergeTree settings:
    ```xml
    <merge_tree>
        <fsync_after_insert>1</fsync_after_insert>
        <fsync_part_directory>1</fsync_part_directory>
        <min_rows_to_fsync_after_merge>1</min_rows_to_fsync_after_merge>
        <min_compressed_bytes_to_fsync_after_merge>1</min_compressed_bytes_to_fsync_after_merge>
    </merge_tree>
    ```
