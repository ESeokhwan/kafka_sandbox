# Kafka client examples

This module contains some Kafka client examples.

1. Start a Kafka 2.5+ local cluster with a plain listener configured on port 9092.
2. Run `examples/bin/java-producer-consumer-demo.sh 10000` to asynchronously send 10k records to topic1 and consume them.
3. Run `examples/bin/java-producer-consumer-demo.sh 10000 sync` to synchronous send 10k records to topic1 and consume them.
4. Run `examples/bin/exactly-once-demo.sh 6 3 10000` to create input-topic and output-topic with 6 partitions each,
   start 3 transactional application instances and process 10k records.

The global sequence example requires this branch's brokers and clients, rather than an unmodified Kafka cluster.
Build with `./gradlew :examples:jar :core:copyDependantLibs`, then run:

```sh
bin/kafka-run-class.sh kafka.examples.globalsequence.GlobalSequenceReadDemo \
  localhost:9092 TOPIC_UUID 0 100 read_committed
```

Modes are `lookup`, `read_uncommitted`, and `read_committed`. An optional final `client.properties` argument
configures SASL/SSL. The example uses small two-batch pages and prints the selected records plus the next
global cursor; values are Base64 encoded. It stops at the first page's committed end, pending transaction,
or error. See the [global sequence operating guide](../docs/design/global-sequence-operations.md) for topic
creation, offset semantics, retries, and limitations.
