## How to deploy for At Least Once Message Delivery?

As introduced in the [What is At Least Once Message Delivery](../introduction/message-delivery#what-is-at-least-once-message-delivery), Gearpump has a built in KafkaSource. To get at least once message delivery, users should deploy a Kafka cluster as the offset store along with the Gearpump cluster. 

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

Exactly Once Message Delivery requires both an offset store and a checkpoint store. For the offset store, a Kafka cluster should be deployed as in the previous section. As for the checkpoint store, Gearpump has built-in support for Hadoop file systems, like HDFS. Hence, users should deploy a HDFS cluster alongside the Gearpump cluster. 

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

    