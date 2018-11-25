## Master and Worker configuration

Master and Worker daemons will only read configuration from `conf/gear.conf`.

Master reads configuration from section master and gearpump:

	:::bash
	master {
	}
	gearpump{
	}


Worker reads configuration from section worker and gearpump:

	:::bash
	worker {
	}
	gearpump{
	}
	

## Configuration for user submitted application job

For user application job, it will read configuration file `gear.conf` and `application.conf` from classpath, while `application.conf` has higher priority.
The default classpath contains:

1. `conf/`
2. current working directory.

For example, you can put a `application.conf` on your working directory, and then it will be effective when you submit a new job application.

## Logging

To change the log level, you need to change both `gear.conf`, and `log4j.properties`.

### To change the log level for master and worker daemon

Please change `log4j.rootLevel` in `log4j.properties`, `gearpump-master.akka.loglevel` and `gearpump-worker.akka.loglevel` in `gear.conf`.

### To change the log level for application job

Please change `log4j.rootLevel` in `log4j.properties`, and `akka.loglevel` in `gear.conf` or `application.conf`.

## Gearpump Default Configuration

This is the default configuration for `gear.conf`.

| config item    | default value  | description      |
| -------------- | -------------- | ---------------- |
| gearpump.hostname | "127.0.0.1" | hostname of current machine. If you are using local mode, then set this to 127.0.0.1. If you are using cluster mode, make sure this hostname can be accessed by other machines. |
| gearpump.cluster.masters | ["127.0.0.1:3000"] | Config to set the master nodes of the cluster. If there are multiple master in the list, then the master nodes runs in HA mode. For example, you may start three master, on node1: `bin/master -ip node1 -port 3000`, on node2: `bin/master -ip node2 -port 3000`, on node3: `bin/master -ip node3 -port 3000`, then you need to set  `gearpump.cluster.masters = ["node1:3000","node2:3000","node3:3000"]` |
| gearpump.task-dispatcher | "gearpump.shared-thread-pool-dispatcher" | default dispatcher for task actor |
| gearpump.metrics.enabled | true | flag to enable the metrics system |
| gearpump.metrics.sample-rate | 1 | We will take one sample every `gearpump.metrics.sample-rate` data points. Note it may have impact that the statistics on UI portal is not accurate. Change it to 1 if you want accurate metrics in UI |
| gearpump.metrics.report-interval-ms | 15000 | we will report once every 15 seconds |
| gearpump.metrics.reporter  | "akka" | available value: "graphite", "akka", "logfile" which write the metrics data to different places. |
| gearpump.retainHistoryData.hours | 72 | max hours of history data to retain, Note: Due to implementation limitation(we store all history in memory), please don't set this to too big which may exhaust memory. |
| gearpump.retainHistoryData.intervalMs | 3600000 |  time interval between two data points for history data (unit: ms). Usually this is set to a big value so that we only store coarse-grain data |
| gearpump.retainRecentData.seconds | 300 | max seconds of recent data to retain. This is for the fine-grain data |
| gearpump.retainRecentData.intervalMs | 15000 | time interval between two data points for recent data (unit: ms) |
| gearpump.log.daemon.dir | "logs" | The log directory for daemon processes(relative to current working directory) |
| gearpump.log.application.dir | "logs" | The log directory for applications(relative to current working directory) |
| gearpump.serializers | a map | custom serializer for streaming application, e.g. `"scala.Array" = ""` |
| gearpump.worker.slots | 1000 | How many slots each worker contains |
| gearpump.appmaster.vmargs | "-server  -Xss1M -XX:+HeapDumpOnOutOfMemoryError -XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=80 -XX:+UseParNewGC -XX:NewRatio=3 -Djava.rmi.server.hostname=localhost" | JVM arguments for AppMaster |
| gearpump.appmaster.extraClasspath | "" | JVM default class path for AppMaster |
| gearpump.executor.vmargs | "-server -Xss1M -XX:+HeapDumpOnOutOfMemoryError -XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=80 -XX:+UseParNewGC -XX:NewRatio=3  -Djava.rmi.server.hostname=localhost" | JVM arguments for executor |
| gearpump.executor.extraClasspath | "" | JVM default class path for executor |
| gearpump.jarstore.rootpath | "jarstore/" |   Define where the submitted jar file will be stored. This path follows the hadoop path schema. For HDFS, use `hdfs://host:port/path/`, and HDFS HA, `hdfs://namespace/path/`; if you want to store on master nodes, then use local directory. `jarstore.rootpath = "jarstore/"` will point to relative directory where master is started. `jarstore.rootpath = "/jarstore/"` will point to absolute directory on master server |
| gearpump.scheduling.scheduler-class |"io.gearpump.cluster.scheduler.PriorityScheduler" | Class to schedule the applications. |
| gearpump.services.host | "127.0.0.1" | dashboard UI host address |
| gearpump.services.port | 8090 | dashboard UI host port |
| gearpump.netty.buffer-size | 5242880 | netty connection buffer size |
| gearpump.netty.max-retries | 30 | maximum number of retries for a netty client to connect to remote host |
| gearpump.netty.base-sleep-ms | 100 | base sleep time for a netty client to retry a connection. Actual sleep time is a multiple of this value |
| gearpump.netty.max-sleep-ms | 1000 | maximum sleep time for a netty client to retry a connection |
| gearpump.netty.message-batch-size | 262144 | netty max batch size |
| gearpump.netty.flush-check-interval | 10 | max flush interval for the netty layer, in milliseconds |
| gearpump.netty.dispatcher | "gearpump.shared-thread-pool-dispatcher" | default dispatcher for netty client and server |
| gearpump.shared-thread-pool-dispatcher | default Dispatcher with "fork-join-executor" | default shared thread pool dispatcher |
| gearpump.single-thread-dispatcher | PinnedDispatcher | default single thread dispatcher |
| gearpump.serialization-framework | "io.gearpump.serializer.FastKryoSerializationFramework" | Gearpump has built-in serialization framework using Kryo. Users are allowed to use a different serialization framework, like Protobuf. See `io.gearpump.serializer.FastKryoSerializationFramework` to find how a custom serialization framework can be defined |
| worker.executor-share-same-jvm-as-worker | false | whether the executor actor is started in the same jvm(process) from which running the worker actor, the intention of this setting is for the convenience of single machine debugging, however, the app jar need to be added to the worker's classpath when you set it true and have a 'real' worker in the cluster |