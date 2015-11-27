---
layout: global
displayTitle: Gearpump Security Guide
title: Gearpump Security Guide
description: Gearpump Security Guide
---

Until now Gearpump support being launched in a secured Yarn cluster and writing to secured HBase, here secured means Kerberos enabled. 
Further security related feature is in progress.

## How to launch GearPump in a secured Yarn cluster
Suppose user ```gearpump``` will launch gearpump on YARN, then the corresponding principal `gearpump` should be created in KDC server.

1. Create Kerberos principal for user ```gearpump```, on the KDC machine run
 
   ``` 
   sudo kadmin.local
   ```
   
   In the kadmin.local or kadmin shell, create the principal
   
   ```
   kadmin:  addprinc gearpump/fully.qualified.domain.name@YOUR-REALM.COM
   ```
   
   Remember that user ```gearpump``` must exist on every node of Yarn. 
   
2. Create HDFS folder /user/gearpump/, make sure all read-write rights are granted for user ```gearpump```

   ```
   drwxr-xr-x   - gearpump  gearpump           0 2015-11-27 14:03 /user/gearpump
   ```
   
3. Upload the gearpump-pack-2.11.5-{{ site.GEARPUMP_VERSION }}.tar.gz jars to HDFS folder: /user/gearpump/, you can refer to [How to get gearpump distribution](get-gearpump-distribution.html) to get the Gearpump binary.
4. Modify the config file ```conf/yarn.conf``` or create your own config file, the ```gearpump.yarn.client.hdfsRoot``` should point to the hdfs folder just created.
5. Get Kerberos credentials to sumbit the job:

   ```
   kinit gearpump/fully.qualified.domain.name@YOUR-REALM.COM
   ```
   
   Here you can login with keytab or password, please refer Kerberos's document for details.
    
   ``` bash
   bin/yarnclient -version gearpump-pack-2.11.5-{{ site.GEARPUMP_VERSION }} -config conf/yarn.conf
   ```
  
## How to write to secured HBase
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
