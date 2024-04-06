Chafka
======
Extensible service for real-time data ingestion from Kafka to ClickHouse.

Motivation
==========
ClickHouse can natively consume data from Kafka using [Kafka table engine](https://clickhouse.com/docs/en/integrations/kafka#kafka-table-engine), or [Kafka Connect sink](https://clickhouse.com/docs/en/integrations/kafka/clickhouse-kafka-connect-sink).
However, these solutions have own issues - in particular, lack of flexibility and observability.  This project is a standalone service that allows you to ingest your data in safe and controllable manner while
providing full control over schema and deserialization process. 

Architecture
============
Conceptually, service consists of two main components - the ingestion core, responsible for consuming messages from Kafka and writing data to ClickHouse; and one or several "decoders" - packages implementing a
simple interface to unmarshal message from Kafka into set of ClickHouse columns. Out of the box there is a universal configurable Avro ingester, and you also may add your own. 

Status
======
As you may have noticed already, the project currently is merely a MVP, not suitable for any actual work. But at least you got the idea, right? 
