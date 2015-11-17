# Gearpump HDFS

Gearpump components for interacting with HDFS file systems.

## Usage

1. File Rotation interface
```scala
trait Rotation extends Serializable {
  def mark(timestamp: TimeStamp, offset: Long): Unit
  def shouldRotate: Boolean
  def rotate: Unit
}
```
This interface is used by a Hdfs Sink to decide when to rotate the output file, the default one is a FileSizeRotation, which will rotate to a new output file if the file size is larger than 128MB.

2. OutputFormatter interface
```scala
trait SequenceFormat extends Serializable{
  def getKeyClass: Class[_ <: Writable]
  def getValueClass: Class[_ <: Writable]
  def getKey(message: Message): Writable
  def getValue(message: Message): Writable
}
```
This interface is used by SequenceFileSink to determinate the output format, there is a DefaultSequenceFormatter which write Message's ```timestamp``` as key and Message's ```msg``` as string value.

## Launch the application

The HDFS cluster should run on where Gearpump is deployed.
Suppose HDFS is installed at ```/usr/lib/hadoop2.6``` on every node and you already have your application built into a jar file. 
Then before submitting the application, you need to add Hdfs lib folder and conf folder into ```gearpump.executor.extraClasspath``` in ```conf/gear.conf```, for example ```/usr/lib/hadoop2.6/share/hadoop/common/*:/usr/lib/hadoop2.6/share/hadoop/common/lib/*:/usr/lib/hadoop2.6/share/hadoop/hdfs/*:/usr/lib/hadoop2.6/share/hadoop/hdfs/lib/*:/usr/lib/hadoop2.6/etc/conf```. 
Please note only client side's configuration change is needed. After that, you are able to submmit the application.

## Working with Secured HDFS

When the remote Hdfs is security enabled, a kerberos keytab and the corresponding principal name need to be
provided for the gearpump-hdfs connector. Specifically, the UserConfig object passed into the SequenceFileSink should contain
{("gearpump.keytab.file", "\\$keytab"), ("gearpump.kerberos.principal", "\\$principal")}, example code of writing an application
to write to secured Hdfs:

```scala
val principal = "gearpump/fully.qualified.domain.name@YOUR-REALM.COM"
val keytabContent = Files.toByteArray(new File("path_to_keytab_file))
val appConfig = UserConfig.empty
      .withString("gearpump.kerberos.principal", principal)
      .withBytes("gearpump.keytab.file", keytabContent)
val sink = new SequenceFileSink(appConfig, "hdfs://host:9000/user/")
val sinkProcessor = DataSinkProcessor(sink, "$sinkNum")
val split = Processor[Split]("$splitNum")
val computation = split ~> sinkProcessor
val application = StreamApplication("Hdfs", Graph(computation), UserConfig.empty)
```

Note here the keytab file set into config should be a byte array.
 