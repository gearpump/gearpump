---
layout: global
displayTitle: Gearpump Configuration
title: Gearpump Configuration
description: Gearpump Configuration
---

## Gearpump Configuration

The configuration can be changed at conf/gear.conf.
If you change the configuration, you need to restart the daemon process(master, worker) to make the change effective.

config item | default value | description
---------------|--------|---------------
gearpump.hostname | "127.0.0.1" | hostname of current machine. If you are using local mode, then set this to 127.0.0.1, if you are using cluster mode, make sure this hostname can be accessed by other machines.
gearpump.cluster.masters | ["127.0.0.1:3000"] | Config to set the master nodes of the cluster. If there are multiple master in the list, then the master nodes runs in HA mode.  ### For example, you may start three master, on node1: bin/master -ip node1 -port 3000, on node2: bin/master -ip node2 -port 3000, on node3: bin/master -ip node3 -port 3000, then you need to set the cluster.masters = ["node1:3000","node2:3000","node3:3000"]
gearpump.task-dispatcher | "akka.actor.pined-dispatcher" | default dispatcher for task actor
gearpump.metrics.enabled | false | flag to enable the metrics system
gearpump.metrics.sample-rate | 10 | We will take one metric out of ${sample.rate}
gearpump.metrics.report-interval-ms | 15000 | we will report once every 15 seconds
gearpump.metrics.reporter  | "logfile" | available value: "graphite", "akka", "logfile" which write the metrics data to different places.
gearpump.metrics.graphite.host | "127.0.0.1" | when set the reporter = "graphite", the target graphite host.
gearpump.metrics.graphite.port | 2003 | when set the reporter = "graphite", the target graphite port
gearpump.retainHistoryData.hours | 72 | max hours of history data to retain, Note: Due to implementation limitation(we store all history in memory), please don't set this to too big which may exhaust memory.
gearpump.retainHistoryData.intervalMs | 3600000 |  # time interval between two data points for history data (unit: ms) Usually this value is set to a big value so that we only store coarse-grain data
gearpump.retainRecentData.seconds | 300 | max seconds of recent data to retain, tHis is for the fine-grain data
gearpump.retainRecentData.intervalMs | 15000 | time interval between two data points for recent data (unit: ms)
gearpump.log.daemon.dir | "logs" | The log directory for daemon processes(relative to current working directory)
gearpump.log.application.dir | "logs" | The log directory for applications(relative to current working directory)
gearpump.serializers | a map | custom serializer for streaming application
gearpump.worker.slots | 100 | How many slots each worker contains
gearpump.appmaster.vmargs | "" | JVM arguments for AppMaster
gearpump.appmaster.extraClasspath | "" | JVM default class path for AppMaster
gearpump.executor.vmargs | "" | JVM arguments for executor
gearpump.executor.extraClasspath | "" | JVM default class path for executor
gearpump.jarstore.rootpath | "jarstore/" |   Define where the submitted jar file will be stored at. This path follows the hadoop path schema, For HDFS, use hdfs://host:port/path/; For FTP, use ftp://host:port/path; If you want to store on master nodes, then use local directory. jarstore.rootpath = "jarstore/" will points to relative directory where master is started. jarstore.rootpath = "/jarstore/" will points to absolute directory on master server
gearpump.scheduling.scheduler-class | | Default value is "io.gearpump.cluster.scheduler.PriorityScheduler". Class to schedule the applications.
gearpump.services.host | 127.0.0.1 | dashboard UI host address
gearpump.services.port | 8090 | dashboard UI host port
gearpump.services.ws | 8091 | web socket service port for long connection.
gearpump.netty.buffer-size | 5242880
gearpump.netty.max-retries | 30
gearpump.netty.base-sleep-ms | 100
gearpump.netty.max-sleep-ms | 1000
gearpump.netty.message-batch-size | 262144
gearpump.netty.fulsh-check-interval | 10
