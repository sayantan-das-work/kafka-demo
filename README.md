# kafka-demo
A demo project to demonstrate the implementation using Kafka

## Steps for linux systems

### Start Kafka with KRaft

1. bin/kafka-storage.sh random-uuid (e.g uM9sQTEkRqCURFCyL54hMQ)

2. bin/kafka-storage.sh format -t <uuid_from above step 1> -c config/kraft/server.properties

3. bin/kafka-server-start.sh config/kraft/server.properties

## Create kafka topic with partition
bin/kafka-topics.sh --create --topic "topic-name" --bootstrap-server localhost:9092 --partitions <#>

** For console consumer
bin/kafka-console-consumer.sh --topic "topic-name-from-where-to-consume" --from-beginning --bootstrap-server localhost:9092

#### Additional link : https://learn.conduktor.io/kafka/how-to-install-apache-kafka-on-linux-without-zookeeper-kraft-mode/
