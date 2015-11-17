---
layout: global
displayTitle: Gearpump Security Guide
title: Gearpump Security Guide
description: Gearpump Security Guide
---

Until now Gearpump support being launched in a secured Yarn cluster and writing to secured HBase, here secured means Kerberos enabled. 
Further security related feature is in progress.

## How to launch GearPump in a secured Yarn cluster
Suppose user ```gear``` will luanch the Gearpump, corresponding principal should be created in KDC server.

1. Create HDFS folder /user/gear/, make sure all read-write rights are granted for user ```gear```
2. Upload the gearpump-{{ site.GEARPUMP_VERSION }}.tar.gz jars to HDFS folder: /user/gear/, you can refer to [How to get gearpump distribution](get-gearpump-distribution.html) to get the Gearpump binary.
3. Modify the config file ```conf/yarn.conf.template``` or create your own config file
4. You must do ```kinit``` before accessing the Yarn cluster, then run 
  ``` bash
  bin/yarnclient -version gearpump-{{ site.GEARPUMP_VERSION }} -config conf/yarn.conf
  ```
  
## How to write to secured HBase
When the remote HBase is security enabled, a kerberos keytab and the corresponding principal name need to be
provided for the gearpump-hbase connector. Specifically, the UserConfig object passed into the HBaseSink should contain
{("gearpump.keytab.file", "\\$keytab"), ("gearpump.kerberos.principal", "\\$principal")}, example code:

```scala
val appConfig = UserConfig.empty
      .withString("gearpump.kerberos.principal", "$principal")
      .withBytes("gearpump.keytab.file", "$keytabContent")
val sink = new HBaseSink(appConfig, "$tableName")
val sinkProcessor = DataSinkProcessor(sink, "$sinkNum")
```

Note here the keytab file set into config should be a serialized file.

## Future Plan

### More external components support
1. HDFS
2. Kafka

### Authentication(Kerberos)
Since Gearpump’s Master-Worker structure is similar to HDFS’s NameNode-DataNode and Yarn’s ResourceManager-NodeManager, we may follow the way they use.

1. User create kerberos principal and keytab for Gearpump.
2. Deploy the keytab files to all the cluster nodes.
3. Configure Gearpump’s conf file, specify kerberos principal and local keytab file localtion.
4. Start Master and Worker.

Every application have a submitter user. We will separate the application from different user, like different log folder for different applications. 
Only authenticated user can submit the application to Gearpump's Master.

### Authorization
Hopefully more on this soon
