---
layout: global
title: Gearpump Connectors
---

## Basic Concepts
`DataSource` and `DataSink` are the two main concepts Gearpump use to connect with the outside world.

### DataSource
`DataSource` is the concept in Gearpump that without input and will output messages. So, basically, `DataSource` is the start point of a streaming processing flow.

As Gearpump depends on `DataSource` to be replayable to ensure at-least-once message delivery and exactly-once message delivery, for some data sources, we will need a `io.gearpump.streaming.transaction.api.OffsetStorageFactory` to store the offset (progress) of current `DataSource`. So that, when a replay is needed, Gearpump can guide `DataSource` to replay from certain offset.

Currently Gearpump `DataSource` only support infinite stream. Finite stream support will be added in a near future release.

### DataSink
`DataSink` is the concept that without output but will consume messages. So, `Sink` is the end point of a streaming processing flow.

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

### Use of `KafkaSource`
To use `kafkaSource` in your application, you first need to add the `gearpump-external-Kafka` library dependency in your application:

```
"com.github.intel-hadoop" %% "gearpump-external-kafka" % {{ site.GEARPUMP_VERSION }}
```

```xml
<dependency>
  <groupId>com.github.intel-hadoop</groupId>
  <artifactId>gearpump-external-kafka</artifactId>
  <version>{{ site.GEARPUMP_VERSION }}</version>
</dependency>
```

To connect to Kafka, you need to provide following info:
 - the Zookeeper address
 - the Kafka topic

Then, you can use `KafkaSource` in your application:

```scala

   //Specify the offset storage.
   //Here we use the same zookeeper as the offset storage.
   //A set of corresponding topics will be created to store the offsets.
   //You are free to specify your own offset storage
   val offsetStorageFactory = new KafkaStorageFactory(zookeepers, brokers)

   //create the kafka data source
   val source = new KafkaSource(topic, zookeepers, offsetStorageFactory)

   //create Gearpump Processor
   val reader = DataSourceProcessor(source, parallelism)
```

```scala

  //specify the offset storage
  //here we use the same zookeeper as the offset storage (a set of corresponding topics will be created to store the offsets)
  //you are free to specify your own offset storage
  val offsetStorageFactory = new KafkaStorageFactory(zookeepers, brokers)

  val source = KafkaDSLUtil.createStream(app, parallelism, "Kafka Source", topics, zookeepers, offsetStorageFactory)
  ...
```

### Use of `HBaseSink`

To use `HBaseSink` in your application, you first need to add the `gearpump-external-hbase` library dependency in your application:


```
"com.github.intel-hadoop" %% "gearpump-external-hbase" % {{ site.GEARPUMP_VERSION }}
```

```xml
<dependency>
  <groupId>com.github.intel-hadoop</groupId>
  <artifactId>gearpump-external-hbase</artifactId>
  <version>{{ site.GEARPUMP_VERSION }}</version>
</dependency>
```


To connect to HBase, you need to provide following info:
 - the HBase configuration to tell which HBase service to connect
 - the table name (you must create the table yourself, see the [HBase documentation](https://hbase.apache.org/book.html))

Then, you can use `HBaseSink` in your application:

```scala
   //create the HBase data sink
   val sink = HBaseSink(UserConfig.empty, tableName, HBaseConfiguration.create())

   //create Gearpump Processor
   val sinkProcessor = DataSinkProcessor(sink, parallelism)
```

```scala
  //assume stream is a normal `Stream` in DSL
  stream.writeToHbase(UserConfig.empty, tableName, parallelism, "write to HBase")
```

You can tune the connection to HBase via the HBase configuration passed in. If not passed, Gearpump will try to check local classpath to find a valid HBase configuration (`hbase-site.xml`).

Attention, due to the issue discussed [here](http://stackoverflow.com/questions/24456484/hbase-managed-zookeeper-suddenly-trying-to-connect-to-localhost-instead-of-zooke) you may need to create additional configuration for your HBase sink:

```scala
   def hadoopConfig = {
     val conf = new Configuration()
     conf.set("hbase.zookeeper.quorum", "zookeeperHost")
     conf.set("hbase.zookeeper.property.clientPort", "2181")
     conf
   }
```

```scala
   val sink = HBaseSink(UserConfig.empty, tableName, hadoopConfig)
```

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

```scala
object KafkaDSLUtil {
  //T is the message type
  def createStream[T: ClassTag](
      app: StreamApp,
      parallelism: Int,
      description: String,
      topics: String,
      zkConnect: String,
      offsetStorageFactory: OffsetStorageFactory): dsl.Stream[T] = {
    app.source[T](new KafkaSource(topics, zkConnect, offsetStorageFactory)
        with TypedDataSource[T], parallelism, description)
  }
}
```

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

```scala
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
```
