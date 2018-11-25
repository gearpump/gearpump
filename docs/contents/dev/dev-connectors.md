## Basic Concepts
`DataSource` and `DataSink` are the two main concepts Gearpump use to connect with the outside world.

### DataSource
`DataSource` is the start point of a streaming processing flow. 


### DataSink
`DataSink` is the end point of a streaming processing flow.

## Implemented Connectors

### `DataSource` implemented
Currently, we have following `DataSource` supported.

Name | Description
-----| ----------
`CollectionDataSource` | Convert a collection to a recursive data source. E.g. `seq(1, 2, 3)` will output `1,2,3,1,2,3...`.
`KafkaSource` | Read from Kafka.

### `DataSink` implemented
Currently, we have following `DataSink` supported.

Name | Description
-----| ----------
`HBaseSink` | Write the message to HBase. The message to write must be HBase `Put` or a tuple of `(rowKey, family, column, value)`.
`KafkaSink` | Write to Kafka.

## Use of Connectors

### Use of Kafka connectors

To use Kafka connectors in your application, you first need to add the `gearpump-external-kafka` library dependency in your application:

#### SBT

	:::sbt
	"io.gearpump" %% "gearpump-external-kafka" % {{GEARPUMP_VERSION}}

#### XML

	:::xml
	<dependency>
	  <groupId>io.gearpump</groupId>
	  <artifactId>gearpump-external-kafka</artifactId>
	  <version>{{GEARPUMP_VERSION}}</version>
	</dependency>


This is a simple example to read from Kafka and write it back using `KafkaSource` and `KafkaSink`. Users can optionally set a `CheckpointStoreFactory` such that Kafka offsets are checkpointed and at-least-once message delivery is guaranteed. 

#### Low level API 

	:::scala
	val appConfig = UserConfig.empty
	val props = new Properties
	props.put(KafkaConfig.ZOOKEEPER_CONNECT_CONFIG, zookeeperConnect)
	props.put(KafkaConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
	props.put(KafkaConfig.CHECKPOINT_STORE_NAME_PREFIX_CONFIG, appName)
	val source = new KafkaSource(sourceTopic, props)
	val checkpointStoreFactory = new KafkaStoreFactory(props)
	source.setCheckpointStore(checkpointStoreFactory)
	val sourceProcessor = DataSourceProcessor(source, sourceNum)
	val sink = new KafkaSink(sinkTopic, props)
	val sinkProcessor = DataSinkProcessor(sink, sinkNum)
	val partitioner = new ShufflePartitioner
	val computation = sourceProcessor ~ partitioner ~> sinkProcessor
	val app = StreamApplication(appName, Graph(computation), appConfig)

#### High level API

	:::scala
	val props = new Properties
	val appName = "KafkaDSL"
	props.put(KafkaConfig.ZOOKEEPER_CONNECT_CONFIG, zookeeperConnect)
	props.put(KafkaConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
	props.put(KafkaConfig.CHECKPOINT_STORE_NAME_PREFIX_CONFIG, appName)
		
	val app = StreamApp(appName, context)
		
	if (atLeastOnce) {
	  val checkpointStoreFactory = new KafkaStoreFactory(props)
	  KafkaDSL.createAtLeastOnceStream(app, sourceTopic, checkpointStoreFactory, props, sourceNum)
	      .writeToKafka(sinkTopic, props, sinkNum)
	} else {
	  KafkaDSL.createAtMostOnceStream(app, sourceTopic, props, sourceNum)
	      .writeToKafka(sinkTopic, props, sinkNum)
	}
		

In the above example, configurations are set through Java properties and shared by `KafkaSource`, `KafkaSink` and `KafkaCheckpointStoreFactory`.
Their configurations can be defined differently as below. 

#### `KafkaSource` configurations

Name | Descriptions | Type | Default 
---- | ------------ | ---- | -------
`KafkaConfig.ZOOKEEPER_CONNECT_CONFIG` | Zookeeper connect string for Kafka topics management | String 
`KafkaConfig.CLIENT_ID_CONFIG` | An id string to pass to the server when making requests | String | ""  
`KafkaConfig.GROUP_ID_CONFIG` | A string that uniquely identifies a set of consumers within the same consumer group | "" 
`KafkaConfig.FETCH_SLEEP_MS_CONFIG` | The amount of time(ms) to sleep when hitting fetch.threshold | Int | 100 
`KafkaConfig.FETCH_THRESHOLD_CONFIG` | Size of internal queue to keep Kafka messages. Stop fetching and go to sleep when hitting the threshold | Int | 10000 
`KafkaConfig.PARTITION_GROUPER_CLASS_CONFIG` | Partition grouper class to group partitions among source tasks |  Class | DefaultPartitionGrouper 
`KafkaConfig.MESSAGE_DECODER_CLASS_CONFIG` | Message decoder class to decode raw bytes from Kafka | Class | DefaultMessageDecoder 
`KafkaConfig.TIMESTAMP_FILTER_CLASS_CONFIG` | Timestamp filter class to filter out late messages | Class | DefaultTimeStampFilter 


#### `KafkaSink` configurations

Name | Descriptions | Type | Default 
---- | ------------ | ---- | ------- 
`KafkaConfig.BOOTSTRAP_SERVERS_CONFIG` | A list of host/port pairs to use for establishing the initial connection to the Kafka cluster | String |  
`KafkaConfig.CLIENT_ID_CONFIG` | An id string to pass to the server when making requests | String | ""  

#### `KafkaCheckpointStoreFactory` configurations

Name | Descriptions | Type | Default 
---- | ------------ | ---- | ------- 
`KafkaConfig.ZOOKEEPER_CONNECT_CONFIG` | Zookeeper connect string for Kafka topics management | String | 
`KafkaConfig.BOOTSTRAP_SERVERS_CONFIG` | A list of host/port pairs to use for establishing the initial connection to the Kafka cluster | String | 
`KafkaConfig.CHECKPOINT_STORE_NAME_PREFIX` | Name prefix for checkpoint store | String | "" 
`KafkaConfig.REPLICATION_FACTOR` | Replication factor for checkpoint store topic | Int | 1 

### Use of `HBaseSink`

To use `HBaseSink` in your application, you first need to add the `gearpump-external-hbase` library dependency in your application:

#### SBT

	:::sbt
	"io.gearpump" %% "gearpump-external-hbase" % {{GEARPUMP_VERSION}}

#### XML
	:::xml
	<dependency>
	  <groupId>io.gearpump</groupId>
	  <artifactId>gearpump-external-hbase</artifactId>
	  <version>{{GEARPUMP_VERSION}}</version>
	</dependency>


To connect to HBase, you need to provide following info:
  
  * the HBase configuration to tell which HBase service to connect
  * the table name (you must create the table yourself, see the [HBase documentation](https://hbase.apache.org/book.html))

Then, you can use `HBaseSink` in your application:

	:::scala
	//create the HBase data sink
	val sink = HBaseSink(UserConfig.empty, tableName, HBaseConfiguration.create())
	
	//create Gearpump Processor
	val sinkProcessor = DataSinkProcessor(sink, parallelism)


	:::scala
	//assume stream is a normal `Stream` in DSL
	stream.writeToHbase(UserConfig.empty, tableName, parallelism, "write to HBase")


You can tune the connection to HBase via the HBase configuration passed in. If not passed, Gearpump will try to check local classpath to find a valid HBase configuration (`hbase-site.xml`).

Attention, due to the issue discussed [here](http://stackoverflow.com/questions/24456484/hbase-managed-zookeeper-suddenly-trying-to-connect-to-localhost-instead-of-zooke) you may need to create additional configuration for your HBase sink:

	:::scala
	def hadoopConfig = {
	 val conf = new Configuration()
	 conf.set("hbase.zookeeper.quorum", "zookeeperHost")
	 conf.set("hbase.zookeeper.property.clientPort", "2181")
	 conf
	}
	val sink = HBaseSink(UserConfig.empty, tableName, hadoopConfig)


## How to implement your own `DataSource`

To implement your own `DataSource`, you need to implement two things:

1. The data source itself
2. a helper class to easy the usage in a DSL

### Implement your own `DataSource`
You need to implement a class derived from `io.gearpump.streaming.transaction.api.TimeReplayableSource`.

### Implement DSL helper (Optional)
If you would like to have a DSL at hand you may start with this customized stream; it is better if you can implement your own DSL helper.
You can refer `KafkaDSLUtil` as an example in Gearpump source.

Below is some code snippet from `KafkaDSLUtil`:

	:::scala
	object KafkaDSLUtil {
	
	  def createStream[T](
	      app: StreamApp,
	      topics: String,
	      parallelism: Int,
	      description: String,
	      properties: Properties): dsl.Stream[T] = {
	    app.source[T](new KafkaSource(topics, properties), parallelism, description)
	  }
	}


## How to implement your own `DataSink`
To implement your own `DataSink`, you need to implement two things:

1. The data sink itself
2. a helper class to make it easy use in DSL

### Implement your own `DataSink`
You need to implement a class derived from `io.gearpump.streaming.sink.DataSink`.

### Implement DSL helper (Optional)
If you would like to have a DSL at hand you may start with this customized stream; it is better if you can implement your own DSL helper.
You can refer `HBaseDSLSink` as an example in Gearpump source.

Below is some code snippet from `HBaseDSLSink`:

	:::scala
	class HBaseDSLSink[T](stream: Stream[T]) {
	  def writeToHbase(userConfig: UserConfig, table: String, parallism: Int, description: String): Stream[T] = {
	    stream.sink(HBaseSink[T](userConfig, table), parallism, userConfig, description)
	  }
	  
	  def writeToHbase(userConfig: UserConfig, configuration: Configuration, table: String, parallism: Int, description: String): Stream[T] = {
	    stream.sink(HBaseSink[T](userConfig, table, configuration), parallism, userConfig, description)
	  }  
	}
	
	object HBaseDSLSink {
	  implicit def streamToHBaseDSLSink[T](stream: Stream[T]): HBaseDSLSink[T] = {
	    new HBaseDSLSink[T](stream)
	  }
	}

