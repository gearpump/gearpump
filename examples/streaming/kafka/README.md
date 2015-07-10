This README explains how to quick-start a KafkaWordCount example on a single node (localhost) stey by step. Basically, the KafkaWordCount example consumes some data from Kafka, executes the classic wordcount computation and then produces back to Kafka. First of all, we need to download and install Kafka. 

## Step 1. Download and install Kafka

Please download the latest kafka from [the official site](http://kafka.apache.org/downloads.html). Extract the downloaded package to any directory you feel comfortable.

## Step 2. Configure and Start Zookeeper

Kafka depends on Zookeeper so next step is to boot up Zookeeper. We'll use the single-node Zookeeper instance packaged with Kafka here for quick start. Suppose you have installed Kafka at `$KAFKA_PATH`, configure Zookeepr client port in  `$KAFKA_PATH/config/zookeeper.properties`.

   ```bash
   # the port at which the clients will connect
   clientPort=2181
   ```

Now start Zookeeper

   ```bash
   $KAFKA_PATH/bin/zookeeper-server-start.sh $KAFKA_PATH/config/zookeeper.properties
   ```
  
## Step 3. Configure and start Kafka

We are good to setup and launch Kafka now. The default Kafka configurations in `$KAFKA_PATH/config/server.properties` should be fine.

   ```bash
   # The id of the broker. This must be set to a unique integer for each broker.
   broker.id=0

   # The port the socket server listens on
   port=9092

   # The number of threads doing disk I/O
   num.io.threads=8

   # The number of logical partitions per topic per server. More partitions allow greater parallelism
   # for consumption, but also mean more files.
   num.partitions=2

   # Zookeeper connection string (see zookeeper docs for details).
   # This is a comma separated host:port pairs, each corresponding to a zk
   # server. e.g. "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002".
   # You can also append an optional chroot string to the urls to specify the
   # root directory for all kafka znodes.
   zookeeper.connect=localhost:2181
   ```

Let's start a Kafka broker

   ```bash
   $KAFKA_PATH/bin/kafka-server-start.sh $KAFKA_PATH/config/kafka.properties
   ```

## Step 4. Create topics and prepare data

Kafka requires you to create topics before writing to it. Also, we have to prepare some data on Kafka to consume from.

We'll create two here for consume (topic1) and produce (topic2) respectively.

   ```bash
   $KAFKA_PATH/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic topic1
   $KAFKA_PATH/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic topic2
   ```
   
Note that `--replication-factor` should not be larger than the number of brokers. 
   
We'll leverage the producer performance test for data preparation.   
   
   ```bash
   $KAFKA_PATH/bin/kafka-producer-perf-test.sh --broker-list localhost:9092 --topic topic1 --messages 500000000
   ```

## Step 5. Build and run KafkaWordCount examples

Change directory into gearpump root, build gearpump with `sbt pack` and launch a local gearpump cluster.

   ```bash
   ./target/pack/bin/local
   ```
   
Finally, let's run the KafkaWordCount example.

   ```bash
   ./target/pack/bin/gear app -jar ./examples/target/$SCALA_VERSION_MAJOR/gearpump-examples-assembly-$VERSION.jar org.apache.gearpump.streaming.examples.kafka.wordcount.KafkaWordCount
   ```

One more step is to verify that we've succeeded in producing data to Kafka.

```bash
   $KAFKA_PATH/bin/kafka-console-consumer.sh --zookeeper localhost:2181 --from-beginning --topic topic2
```

