#GearPump HBase

GearPump integration for [Apache HBase](https://hbase.apache.org)

## Working with Secured HBASE

When the remote HBase is security enabled, a kerberos keytab and the corresponding principal name need to be
provided for the gearpump-hbase connector. Specifically, the UserConfig object passed into the HBaseSink should contain
{("gearpump.keytab.file", "$keytab"), ("gearpump.kerberos.principal", "$principal")}, example code:

```
val appConfig = UserConfig.empty
      .withString("gearpump.kerberos.principal", "$principal")
      .withBytes("gearpump.keytab.file", "$keytabContent")
val sink = new HBaseSink(appConfig, "$tableName")
val sinkProcessor = DataSinkProcessor(sink, "$sinkNum")
```

Note here the keytab file set into config should be a serialized file.


