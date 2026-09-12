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
`IcebergSink` | Continuously append messages to partitioned or unpartitioned Iceberg v3 tables.
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

### Use of the Iceberg v3 sink connector

Add the `gearpump-external-iceberg` dependency to the application:

#### SBT

	:::sbt
	"io.github.gearpump" %% "gearpump-external-iceberg" % {{GEARPUMP_VERSION}}

#### XML

	:::xml
	<dependency>
	  <groupId>io.github.gearpump</groupId>
	  <artifactId>gearpump-external-iceberg_2.13</artifactId>
	  <version>{{GEARPUMP_VERSION}}</version>
	</dependency>

The connector uses Iceberg Java 1.11.0 and requires table format version 3. Tables can be addressed
directly by Hadoop location or through any Iceberg catalog implementation available on the
application classpath.

The sink supports partition fanout, target-sized file rolling, field-name or custom record mapping,
table metadata refresh between checkpoints, and commit metrics. Like Iceberg's Flink Sink V2,
reaching the target file size only rolls a data file; it does not trigger a table commit. At a
Gearpump checkpoint, the writer completes only records older than that checkpoint and produces a
serialized committable. `DataSinkTask` persists the committable through Gearpump's
`CheckpointStore`, appends its files to the table, and reports the checkpoint clock only after the
Iceberg commit succeeds. Records on or after the boundary remain in the next writer.

Iceberg appends carry a stable identifier derived from the commit namespace, task, and checkpoint
timestamp. This makes recovery of persisted committables and replay after an uncertain commit
idempotent while the identifying snapshot remains in table metadata. The default namespace is the
Gearpump application name and ID; configure a unique `commitNamespace` when independent Gearpump
clusters can use the same application identity against one table. The connector no longer writes a
table-local WAL and does not require `FileIO` prefix operations.

The sink processor must enable Gearpump checkpointing and provide a durable, shared
`CheckpointStoreFactory`. Gearpump's `InMemoryCheckpointStoreFactory` is test-only and is not
sufficient for recovery. The source must also be time-replayable from Gearpump's recovered clock.
The runtime's documented checkpoint limitations still apply, and this protocol does not provide
row-level deduplication for records intentionally sent through different checkpoint identities.
Operators must retain identifying snapshots for the required recovery horizon and separately
compact small files, expire older snapshots, and remove orphan files.

	:::scala
	import io.gearpump.external.iceberg._
	import io.gearpump.cluster.UserConfig
	import io.gearpump.streaming.sink.DataSinkProcessor
	import io.gearpump.streaming.state.impl.PersistentStateConfig
	import io.gearpump.streaming.transaction.api.CheckpointStoreFactory
	import org.apache.iceberg.Schema
	import org.apache.iceberg.types.Types

	val schema = new Schema(
	  Types.NestedField.required(1, "id", Types.LongType.get()),
	  Types.NestedField.required(2, "data", Types.StringType.get()),
	  Types.NestedField.optional(3, "event_millis", Types.LongType.get())
	)

	val table = IcebergTableConfig.forNewV3Table("/tmp/gearpump-iceberg", schema)
	val sink = new IcebergSink(
	  table,
	  options = IcebergSinkOptions(
	    targetFileSizeBytes = Some(128 * 1024 * 1024),
	    commitNamespace = Some("production-events"),
	    recordMapper = IcebergRecordMapper.recordOnly
	  )
	)

	val checkpointStoreFactory: CheckpointStoreFactory = durableCheckpointStoreFactory
	val checkpointConfig = UserConfig.empty
	  .withBoolean(PersistentStateConfig.STATE_CHECKPOINT_ENABLE, value = true)
	  .withLong(PersistentStateConfig.STATE_CHECKPOINT_INTERVAL_MS, 60 * 1000L)
	  .withValue(
	    PersistentStateConfig.STATE_CHECKPOINT_STORE_FACTORY,
	    checkpointStoreFactory)

	val sinkProcessor = DataSinkProcessor(sink, parallelism = 2,
	  description = "IcebergSink", taskConf = checkpointConfig)

For a catalog-backed table, supply standard Iceberg catalog properties. Catalog implementations
not included by `iceberg-core`, such as Hive, must be added to the application dependencies.

	:::scala
	val catalogTable = IcebergTableConfig.forCatalogV3Table(
	  catalogName = "production",
	  tableIdentifier = "analytics.events",
	  catalogProperties = Map(
	    "type" -> "rest",
	    "uri" -> "https://catalog.example.com"
	  )
	)


## How to implement your own `DataSource`

To implement your own `DataSource`, you need to implement two things:

1. The data source itself
2. an optional helper that wraps it in `DataSourceProcessor`

### Implement your own `DataSource`
You need to implement a class derived from `io.gearpump.streaming.transaction.api.TimeReplayableSource`.

### Implement a helper (Optional)
If you want a small convenience layer, add a helper that returns a `Processor` backed by your source:

	:::scala
	object SourceUtil {
	
	  def processor(
	      source: DataSource,
	      parallelism: Int,
	      description: String): Processor[DataSourceTask[Any, Any]] = {
	    DataSourceProcessor(source, parallelism, description)
	  }
	}


## How to implement your own `DataSink`
To implement your own `DataSink`, you need to implement two things:

1. The data sink itself
2. an optional helper that wraps it in `DataSinkProcessor`

### Implement your own `DataSink`
You need to implement a class derived from `io.gearpump.streaming.sink.DataSink`.

### Implement a helper (Optional)
If you want a small convenience layer, add a helper that returns a `Processor` backed by your sink:

	:::scala
	object SinkUtil {
	  def processor(
	      sink: DataSink,
	      parallelism: Int,
	      userConfig: UserConfig = UserConfig.empty): Processor[DataSinkTask] = {
	    DataSinkProcessor(sink, parallelism, userConfig)
	  }
	}
