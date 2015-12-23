#Gearpump HBase

Gearpump integration for [Apache HBase](https://hbase.apache.org)

## Usage

The message type that HBaseSink is able to handle including:

 1. Tuple4[String, String, String, String] which means (rowKey, columnGroup, columnName, value)
 2. Tuple4[Array[Byte], Array[Byte], Array[Byte], Array[Byte]] which means (rowKey, columnGroup, columnName, value)
 3. Sequence of type 1 and 2
  
Suppose there is a DataSource Task will output above-mentitioned messages, you can write a simple application then:

```scala
val sink = new HBaseSink(UserConfig.empty, "$tableName")
val sinkProcessor = DataSinkProcessor(sink, "$sinkNum")
val split = Processor[DataSource]("$splitNum")
val computation = split ~> sinkProcessor
val application = StreamApplication("HBase", Graph(computation), UserConfig.empty)
```

## Launch the application

The HBase cluster should run on where Gearpump is deployed.
Suppose HBase is installed at ```/usr/lib/hbase``` on every node and you already have your application built into a jar file. 
Then before submitting the application, you need to add HBase lib folder and conf folder into ```gearpump.executor.extraClasspath``` in ```conf/gear.conf```, for example ```/usr/lib/hbase/lib/*:/usr/lib/hbase/conf```. 
Please note only client side's configuration change is needed. After that, you are able to submmit the application.
 
## Working with Secured HBASE

When the remote HBase is security enabled, a kerberos keytab and the corresponding principal name need to be
provided for the gearpump-hbase connector. Specifically, the UserConfig object passed into the HBaseSink should contain
{("gearpump.keytab.file", "\\$keytab"), ("gearpump.kerberos.principal", "\\$principal")}, example code of writing an application
to write to secured HBase:

```scala
val principal = "gearpump/fully.qualified.domain.name@YOUR-REALM.COM"
val keytabContent = Files.toByteArray(new File("path_to_keytab_file))
val appConfig = UserConfig.empty
      .withString("gearpump.kerberos.principal", principal)
      .withBytes("gearpump.keytab.file", keytabContent)
val sink = new HBaseSink(appConfig, "$tableName")
val sinkProcessor = DataSinkProcessor(sink, "$sinkNum")
val split = Processor[Split]("$splitNum")
val computation = split ~> sinkProcessor
val application = StreamApplication("HBase", Graph(computation), UserConfig.empty)
```

Note here the keytab file set into config should be a byte array.


