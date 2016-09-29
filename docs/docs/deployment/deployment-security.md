Until now Gearpump supports deployment in a secured Yarn cluster and writing to secured HBase, where "secured" means Kerberos enabled. 
Further security related feature is in progress.

## How to launch Gearpump in a secured Yarn cluster
Suppose user `gear` will launch gearpump on YARN, then the corresponding principal `gear` should be created in KDC server.

1. Create Kerberos principal for user `gear`, on the KDC machine
 
		:::bash 
   		sudo kadmin.local
   
	In the kadmin.local or kadmin shell, create the principal
   
   		:::bash
   		kadmin:  addprinc gear/fully.qualified.domain.name@YOUR-REALM.COM
   
	Remember that user `gear` must exist on every node of Yarn. 

2. Upload the gearpump-{{SCALA_BINARY_VERSION}}-{{GEARPUMP_VERSION}}.zip to remote HDFS Folder, suggest to put it under `/usr/lib/gearpump/gearpump-{{SCALA_BINARY_VERSION}}-{{GEARPUMP_VERSION}}.zip`

3. Create HDFS folder /user/gear/, make sure all read-write rights are granted for user `gear`

   		:::bash
   		drwxr-xr-x - gear gear 0 2015-11-27 14:03 /user/gear
   
   
4. Put the YARN configurations under classpath.
  Before calling `yarnclient launch`, make sure you have put all yarn configuration files under classpath. Typically, you can just copy all files under `$HADOOP_HOME/etc/hadoop` from one of the YARN cluster machine to `conf/yarnconf` of gearpump. `$HADOOP_HOME` points to the Hadoop installation directory. 
  
5. Get Kerberos credentials to submit the job:

   		:::bash
   		kinit gearpump/fully.qualified.domain.name@YOUR-REALM.COM
   
   
	Here you can login with keytab or password. Please refer Kerberos's document for details.
    
		:::bash
		yarnclient launch -package /usr/lib/gearpump/gearpump-{{SCALA_BINARY_VERSION}}-{{GEARPUMP_VERSION}}.zip
   
  
## How to write to secured HBase
When the remote HBase is security enabled, a kerberos keytab and the corresponding principal name need to be
provided for the gearpump-hbase connector. Specifically, the `UserConfig` object passed into the HBaseSink should contain
`{("gearpump.keytab.file", "\\$keytab"), ("gearpump.kerberos.principal", "\\$principal")}`. example code of writing to secured HBase:

	:::scala
	val principal = "gearpump/fully.qualified.domain.name@YOUR-REALM.COM"
	val keytabContent = Files.toByteArray(new File("path_to_keytab_file"))
	val appConfig = UserConfig.empty
	      .withString("gearpump.kerberos.principal", principal)
	      .withBytes("gearpump.keytab.file", keytabContent)
	val sink = new HBaseSink(appConfig, "$tableName")
	val sinkProcessor = DataSinkProcessor(sink, "$sinkNum")
	val split = Processor[Split]("$splitNum")
	val computation = split ~> sinkProcessor
	val application = StreamApplication("HBase", Graph(computation), UserConfig.empty)


Note here the keytab file set into config should be a byte array.

## Future Plan

### More external components support
1. HDFS
2. Kafka

### Authentication(Kerberos)
Since Gearpump’s Master-Worker structure is similar to HDFS’s NameNode-DataNode and Yarn’s ResourceManager-NodeManager, we may follow the way they use.

1. User creates kerberos principal and keytab for Gearpump.
2. Deploy the keytab files to all the cluster nodes.
3. Configure Gearpump’s conf file, specify kerberos principal and local keytab file location.
4. Start Master and Worker.

Every application has a submitter/user. We will separate the application from different users, like different log folders for different applications. 
Only authenticated users can submit the application to Gearpump's Master.

### Authorization
Hopefully more on this soon
