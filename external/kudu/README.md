#Gearpump Kudu

Gearpump integration for [Apache Kudu](https://kudu.apache.org)

## Usage

The message type that KuduSink is able to handle including:

 1. Map[String, String] which means (columnName, columnValue)
  
Suppose there is a DataSource Task will output above-mentioned messages, you can write a simple application then:

```scala
val sink = new KuduSink(UserConfig.empty, "$tableName")
val sinkProcessor = DataSinkProcessor(sink, "$sinkNum")
val split = Processor[DataSource]("$splitNum")
val computation = split ~> sinkProcessor
val application = StreamApplication("Kudu", Graph(computation), UserConfig.empty)
```

## Launch the application

The Kudu cluster should run on where Gearpump is deployed.
Suppose Kdudu is installed at ```/usr/lib/kudu``` on every node and you already have your application built into a jar file. 
Please note only client side's configuration change is needed. After that, you are able to submit the application.


## If you need to supply the Kudu cluster details for the connection

```scala

    val map = Map[String, String]("KUDUSINK" -> "kudusink", "kudu.masters"->"kuduserver",
      "KUDU_USER" -> "kudu.user", "GEARPUMP_KERBEROS_PRINCIPAL" -> "gearpump.kerberos.principal",
      "GEARPUMP_KEYTAB_FILE" -> "gearpump.keytab.file", "TABLE_NAME" -> "kudu.table.name"
    )
```

## Working with Kerberized Kudu

Before running job make sure you run kinit, with just a kinit you should be able to run the job and insert records into Kudu table