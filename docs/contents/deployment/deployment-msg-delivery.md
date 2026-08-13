## How to deploy for At Least Once Message Delivery?

!!! warning "Historical connector example"
    Current `master` defines the `TimeReplayableSource` and `CheckpointStore`
    contracts but does not ship the `KafkaSource`, `KafkaStorageFactory`, or
    `HadoopCheckpointStore` classes used below. Treat this page as a historical
    architecture example. A current deployment must provide equivalent durable
    source-offset and state-checkpoint implementations. See [Streaming Runtime
    Guarantees](../internals/runtime-guarantees.md) before relying on a delivery
    level.

The historical deployment described below paired a Kafka-backed replayable
source with a Kafka-backed offset store. A current implementation needs
equivalent durable timestamp-to-offset checkpointing; see
[What is At Least Once Message Delivery](../introduction/message-delivery.md#what-is-at-least-once-message-delivery)
for the delivery model.

Here's an example to deploy a local Kafka cluster. 

1. download the latest Kafka from the official website and extract to a local directory (`$KAFKA_HOME`)

2. Boot up the single-node Zookeeper instance packaged with Kafka. 

    	:::bash
    	$KAFKA_HOME/bin/zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties

 
3. Start a Kafka broker

	    :::bash
	    $KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/kafka.properties
	      

4. When creating a offset store for `KafkaSource`, set the zookeeper connect string to `localhost:2181` and broker list to `localhost:9092` in `KafkaStorageFactory`.

	    :::scala
	    val offsetStorageFactory = new KafkaStorageFactory("localhost:2181", "localhost:9092")
	    val source = new KafkaSource("topic1", "localhost:2181", offsetStorageFactory)
	    

## How to deploy for Exactly Once Message Delivery?

The historical exactly-once design additionally stored task checkpoints in
HDFS. A current deployment needs both durable source-offset storage and a
durable shared `CheckpointStore` implementation; the HDFS-specific classes in
this example are not shipped on `master`.

Here's an example to deploy a local HDFS cluster.

1. download Hadoop 2.6 from the official website and extracts it to a local directory `HADOOP_HOME`

2. add following configuration to `$HADOOP_HOME/etc/core-site.xml`

	    :::xml
	    <configuration>
	      <property>
	        <name>fs.defaultFS</name>
	        <value>hdfs://localhost:9000</value>
	      </property>
	    </configuration>
	    

3. start HDFS

	    :::bash
	    $HADOOP_HOME/sbin/start-dfs.sh
	    
   
4. When creating a `HadoopCheckpointStore`, set the hadoop configuration as in the `core-site.xml`

		:::scala   
    	val hadoopConfig = new Configuration
    	hadoopConfig.set("fs.defaultFS", "hdfs://localhost:9000")
    	val checkpointStoreFactory = new HadoopCheckpointStoreFactory("MessageCount", hadoopConfig, new FileSizeRotation(1000))
