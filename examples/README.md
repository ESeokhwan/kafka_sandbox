# Kafka client examples

This module contains some Kafka client examples.

1. Start a Kafka 2.5+ local cluster with a plain listener configured on port 9092.
2. Run `examples/bin/java-producer-consumer-demo.sh 10000` to asynchronously send 10k records to topic1 and consume them.
3. Run `examples/bin/java-producer-consumer-demo.sh 10000 sync` to synchronous send 10k records to topic1 and consume them.
4. Run `examples/bin/exactly-once-demo.sh 6 3 10000` to create input-topic and output-topic with 6 partitions each,
   start 3 transactional application instances and process 10k records.

## Global sequence consumer

`GlobalSequenceConsumerExample` fetches a half-open global offset range from a topic configured with
`global.sequence.enabled=true`. Unlike `KafkaConsumer`, the global sequence consumer does not join a consumer group,
track a position, or commit offsets. It returns one size-bounded page at a time and the example continues from each
page's `nextGlobalOffset`. This requires the brokers and clients from this branch.

Build the examples and broker dependencies, then run:

```sh
./gradlew :examples:jar :core:copyDependantLibs
bin/kafka-run-class.sh kafka.examples.GlobalSequenceConsumerExample \
  localhost:9092 globally-sequenced-topic 0 100
```

The arguments are `<bootstrap> <topic-name> <start> <end-exclusive> [client.properties]`. The optional properties
file accepts common SSL/SASL settings plus the global consumer settings. For example:

```properties
isolation.level=read_committed
global.sequence.fetch.max.batches=2
fetch.max.bytes=1048576
```

The example uses byte-array deserializers and prints keys and values as Base64. Every record line includes its global
offset and physical partition/offset. Every page line includes the topic UUID, next cursor, committed end, pending
state, and partial error. The first page fixes the finite read end to the smaller of the requested end and its
committed end. Later pages verify that the topic still has the first page's UUID. The example stops at that end or a
pending transaction; a partial error is reported after its valid prefix and cursor have been printed. An empty page
with an advancing cursor is continued because it can represent an aborted range.

Applications must store `(topic UUID, next global offset)` with their completed processing. Console output and cursor
storage are not atomic in this example.

## Raw global sequence protocol diagnostics

`GlobalSequenceReadDemo` accepts a topic UUID and exposes raw Lookup/Fetch mappings for protocol troubleshooting:

```sh
bin/kafka-run-class.sh kafka.examples.globalsequence.GlobalSequenceReadDemo \
  localhost:9092 TOPIC_UUID 0 100 read_committed
```

Its modes are `lookup`, `read_uncommitted`, and `read_committed`. An optional final `client.properties` argument
configures SASL/SSL. It uses two-batch pages, prints mappings and records with Base64 values, and stops at the first
page's committed end, a pending transaction, or an error. Use `GlobalSequenceConsumerExample` for the public
name-based consumer path. See the [global sequence operating guide](../docs/design/global-sequence-operations.md) for
topic creation, offset semantics, retries, and limitations.
