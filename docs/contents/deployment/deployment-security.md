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

## Authenticated artifact storage

JAR transfers require mutual TLS with endpoint hostname verification. Configure
`gearpump.security.tls.key-store`, `trust-store`, `store-type` and `password` using
service-owned protected configuration. Only trusted submission clients and cluster
runtimes should receive certificates. There is no HTTP fallback. Each certificate
must identify the advertised hostname in its subject alternative names.

Set `gearpump.jarstore.access-token` to a cryptographically random base64url secret
of at least 43 characters and distribute it only to authorized submission clients
and runtimes. Requests require both a trusted client certificate and this bearer
credential. Credentials must not be placed in dashboard diagnostics or public files.
The token represents the artifact-service principal, not separate tenant identities.

Uploads reserve their maximum permitted size before writing. Defaults limit the
persistent root to 1 GiB, 1024 artifacts and 64 MiB per artifact. Existing files are
counted at startup; exceeding the quota denies new uploads. Failed/empty uploads
are deleted. Operators can delete unused artifacts with authenticated DELETE
`/artifact?file=<name>` (or `FileServer.Client.delete`). Do not delete artifacts
needed by a running/recoverable application. There is no automatic age-based
removal that could disrupt recovery. Custom storage providers must implement
inventory and deletion; serving fails closed if these are unavailable.

FilePath metadata now includes the uploader's immutable SHA-256 digest. Downloads
require it, check HTTP and IO results, verify bytes before atomically publishing
the local JAR, and delete partial files on failure. Older artifacts without digest
metadata must be reuploaded. Restart the whole cluster for this protocol change.
