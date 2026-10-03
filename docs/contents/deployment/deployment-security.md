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

## Cluster and application control capabilities

The actor control plane now uses mutual TLS (`pekko.ssl.tcp`); plain TCP actor
addresses are unsupported. Its trust store must contain only operator-approved
cluster/submission/runtime peers. Set `gearpump.security.control-secret` to an
independent random base64url value of at least 43 characters on the master,
workers and authorized administrative clients/dashboard service. These clients
are cluster administrators. Their immutable audit identity comes from the
master's `control-user` setting, not a submitted username. This does not implement
separate per-human owner roles for administrative clients.

MasterProxy attaches ControlRequest capabilities automatically. Application
configurations receive a distinct random application capability and app ID,
with administrative/worker secrets explicitly masked. Applications can access
only their own state/resource/lifecycle messages. Registration/status/storage
messages without authority are denied at both master and application-manager
entry points. Recovery rotates application capabilities, invalidating old epochs.
Application data uses a separate namespace from master/recovery metadata. Legacy
checkpoint namespaces are not migrated; drain jobs before upgrading and restart
from durable external state where required.

`GetApplicationControl` is available only to administrative clients, allowing
application management tools to obtain scoped authority. Never log or expose
these credentials. Configuration queries strip credential sections.

Applications still execute under the configured service OS account. Capabilities
protect network/application message boundaries; they do not isolate malicious
code that can read that account's private files or another process. Use separate
OS identities/containers for untrusted tenant code and restrict certificate access.

## Worker launch grants

Resource requests pass through an authorized allocation broker. It installs a
random single-use grant on the allocated worker before returning resources to
the launcher. A grant expires two minutes after worker receipt and binds the
application, positive slot count and application capability. The worker rejects
unallocated, reused, expired, cross-application, zero/negative, duplicate-ID and
non-bootstrap launches before creating an executor. Pending grants are bounded
and `gearpump.worker.max-executors` defaults to 256.

Worker shutdown/resource-release requests also require the owning application
capability; resources can only decrease from their live allocation. Administrative
control credentials are masked in generated executor configurations. Runtime
capabilities and grants must not be logged. This changes allocation/launch message
formats and requires upgrading every cluster component together.

Embedded executors may share the JVM, but each now has its own ActorSystem and
application configuration. They do not inherit the worker's administrative key or
share an Express application key across unrelated applications. Master discovery
canonicalizes configured and discovered addresses before checking endpoint identity.

### Dynamic application changes

`ReplaceProcessor` and `ShellCommand` require a `ControlRequest` with the target
application ID and its live capability. The AppMaster and direct DAG/shell handlers
check the envelope before mutating state or starting a process. `ClientContext`
and HTTP services obtain this authority through the authenticated administrative
master proxy; application principals cannot retrieve another application's key.
Artifact downloads additionally require SHA-256 metadata from the authenticated
artifact service. Recovered applications rotate their capability.
