---
layout: global
title: Gearpump RESTful API reference
---

## Query version

### GET version

Example:

```bash
curl http://127.0.0.1:8090/version
```

Sample Response:

```
0.7.1-SNAPSHOT
```

## Master Service

### GET api/v1.0/master
Get information of masters

Example:

```bash
curl http://127.0.0.1:8090/api/v1.0/master
```

Sample Response:

```
{
  "masterDescription": {
    "leader": [
      "master@127.0.0.1",
      3000
    ],
    "cluster": [
      [
        "127.0.0.1",
        3000
      ]
    ],
    "aliveFor": "642941",
    "logFile": "/Users/foobar/gearpump/logs",
    "jarStore": "jarstore/",
    "masterStatus": "synced",
    "homeDirectory": "/Users/foobar/gearpump"
  }
}
```

### GET api/v1.0/master/applist
Query information of all applications

Example:

```bash
curl http://127.0.0.1:8090/api/v1.0/master/applist
```

Sample Response:

```
{
  "appMasters": [
    {
      "status": "active",
      "appId": 1,
      "appName": "wordCount",
      "appMasterPath": "akka.tcp://app1-executor-1@127.0.0.1:52212/user/daemon/appdaemon1/$c",
      "workerPath": "akka.tcp://master@127.0.0.1:3000/user/Worker0",
      "submissionTime": "1450758114766",
      "startTime": "1450758117294",
      "user": "lisa"
    }
  ]
}
```

### GET api/v1.0/master/workerlist
Query information of all workers

Example:

```bash
curl http://127.0.0.1:8090/api/v1.0/master/workerlist
```

Sample Response:

```
[
  {
    "workerId": 1,
    "state": "active",
    "actorPath": "akka.tcp://master@127.0.0.1:3000/user/Worker0",
    "aliveFor": "431565",
    "logFile": "logs/",
    "executors": [
      {
        "appId": 1,
        "executorId": -1,
        "slots": 1
      },
      {
        "appId": 1,
        "executorId": 0,
        "slots": 1
      }
    ],
    "totalSlots": 1000,
    "availableSlots": 998,
    "homeDirectory": "/usr/lisa/gearpump/",
    "jvmName": "11788@lisa"
  },
  {
    "workerId": 0,
    "state": "active",
    "actorPath": "akka.tcp://master@127.0.0.1:3000/user/Worker1",
    "aliveFor": "431546",
    "logFile": "logs/",
    "executors": [
      {
        "appId": 1,
        "executorId": 1,
        "slots": 1
      }
    ],
    "totalSlots": 1000,
    "availableSlots": 999,
    "homeDirectory": "/usr/lisa/gearpump/",
    "jvmName": "11788@lisa"
  }
]
```

### GET api/v1.0/master/config
Get the configuration of all masters

Example:

```bash
curl http://127.0.0.1:8090/api/v1.0/master/config
```

Sample Response:

```
{
  "extensions": [
    "akka.contrib.datareplication.DataReplication$"
  ]
  "akka": {
    "loglevel": "INFO"
    "log-dead-letters": "off"
    "log-dead-letters-during-shutdown": "off"
    "actor": {
      ## Master forms a akka cluster
      "provider": "akka.cluster.ClusterActorRefProvider"
    }
    "cluster": {
      "roles": ["master"]
      "auto-down-unreachable-after": "15s"
    }
    "remote": {
      "log-remote-lifecycle-events": "off"
    }
  }
}
```

### GET api/v1.0/master/metrics/&lt;query_path&gt;?readLatest=&lt;true|false&gt;
Get the master node metrics.

Example:

```bash
curl http://127.0.0.1:8090/api/v1.0/master/metrics/master?readLatest=true
```

Sample Response:

```
{
    "path"
:
    "master", "metrics"
:
    [{
        "time": "1450758725070",
        "value": {"$type": "io.gearpump.metrics.Metrics.Gauge", "name": "master:memory.heap.used", "value": "59764272"}
    }, {
        "time": "1450758725070",
        "value": {"$type": "io.gearpump.metrics.Metrics.Gauge", "name": "master:thread.daemon.count", "value": "18"}
    }, {
        "time": "1450758725070",
        "value": {
            "$type": "io.gearpump.metrics.Metrics.Gauge",
            "name": "master:memory.total.committed",
            "value": "210239488"
        }
    }, {
        "time": "1450758725070",
        "value": {"$type": "io.gearpump.metrics.Metrics.Gauge", "name": "master:memory.heap.max", "value": "880017408"}
    }, {
        "time": "1450758725070",
        "value": {"$type": "io.gearpump.metrics.Metrics.Gauge", "name": "master:memory.total.max", "value": "997457920"}
    }, {
        "time": "1450758725070",
        "value": {
            "$type": "io.gearpump.metrics.Metrics.Gauge",
            "name": "master:memory.heap.committed",
            "value": "179830784"
        }
    }, {
        "time": "1450758725070",
        "value": {"$type": "io.gearpump.metrics.Metrics.Gauge", "name": "master:memory.total.used", "value": "89117352"}
    }, {
        "time": "1450758725070",
        "value": {"$type": "io.gearpump.metrics.Metrics.Gauge", "name": "master:thread.count", "value": "28"}
    }]
}
```

## Worker service

### GET api/v1.0/worker/&lt;workerId&gt;
Query worker information.

Example:

```bash
curl http://127.0.0.1:8090/api/v1.0/worker/0
```

Sample Response:

```
{
  "workerId": 0,
  "state": "active",
  "actorPath": "akka.tcp://master@127.0.0.1:3000/user/Worker1",
  "aliveFor": "831069",
  "logFile": "logs/",
  "executors": [
    {
      "appId": 1,
      "executorId": 1,
      "slots": 1
    }
  ],
  "totalSlots": 1000,
  "availableSlots": 999,
  "homeDirectory": "/usr/lisa/gearpump/",
  "jvmName": "11788@lisa"
}
```

### GET api/v1.0/worker/&lt;workerId&gt;/config
Query worker config

Example:

```bash
curl http://127.0.0.1:8090/api/v1.0/worker/0/config
```

Sample Response:

```
{
  "extensions": [
    "akka.contrib.datareplication.DataReplication$"
  ]
  "akka": {
    "loglevel": "INFO"
    "log-dead-letters": "off"
    "log-dead-letters-during-shutdown": "off"
    "actor": {
      ## Master forms a akka cluster
      "provider": "akka.cluster.ClusterActorRefProvider"
    }
    "cluster": {
      "roles": ["master"]
      "auto-down-unreachable-after": "15s"
    }
    "remote": {
      "log-remote-lifecycle-events": "off"
    }
  }
}
```

### GET api/v1.0/worker/&lt;workerId&gt;/metrics/&lt;query_path&gt;?readLatest=&lt;true|false&gt;
Get the worker node metrics.

Example:

```bash
curl http://127.0.0.1:8090/api/v1.0/worker/0/metrics/worker?readLatest=true
```

Sample Response:

```
{
    "path"
:
    "worker", "metrics"
:
    [{
        "time": "1450759137860",
        "value": {
            "$type": "io.gearpump.metrics.Metrics.Gauge",
            "name": "worker1:memory.total.used",
            "value": "152931440"
        }
    }, {
        "time": "1450759137860",
        "value": {"$type": "io.gearpump.metrics.Metrics.Gauge", "name": "worker1:thread.daemon.count", "value": "18"}
    }, {
        "time": "1450759137860",
        "value": {
            "$type": "io.gearpump.metrics.Metrics.Gauge",
            "name": "worker0:memory.heap.used",
            "value": "123139640"
        }
    }, {
        "time": "1450759137860",
        "value": {
            "$type": "io.gearpump.metrics.Metrics.Gauge",
            "name": "worker0:memory.total.max",
            "value": "997457920"
        }
    }, {
        "time": "1450759137860",
        "value": {
            "$type": "io.gearpump.metrics.Metrics.Gauge",
            "name": "worker0:memory.heap.committed",
            "value": "179830784"
        }
    }, {
        "time": "1450759137860",
        "value": {"$type": "io.gearpump.metrics.Metrics.Gauge", "name": "worker0:thread.count", "value": "28"}
    }, {
        "time": "1450759137860",
        "value": {"$type": "io.gearpump.metrics.Metrics.Gauge", "name": "worker0:memory.heap.max", "value": "880017408"}
    }, {
        "time": "1450759137860",
        "value": {"$type": "io.gearpump.metrics.Metrics.Gauge", "name": "worker1:memory.heap.max", "value": "880017408"}
    }, {
        "time": "1450759137860",
        "value": {
            "$type": "io.gearpump.metrics.Metrics.Gauge",
            "name": "worker0:memory.total.committed",
            "value": "210239488"
        }
    }, {
        "time": "1450759137860",
        "value": {
            "$type": "io.gearpump.metrics.Metrics.Gauge",
            "name": "worker0:memory.total.used",
            "value": "152931440"
        }
    }, {
        "time": "1450759137860",
        "value": {"$type": "io.gearpump.metrics.Metrics.Gauge", "name": "worker1:thread.count", "value": "28"}
    }, {
        "time": "1450759137860",
        "value": {
            "$type": "io.gearpump.metrics.Metrics.Gauge",
            "name": "worker1:memory.total.max",
            "value": "997457920"
        }
    }, {
        "time": "1450759137860",
        "value": {
            "$type": "io.gearpump.metrics.Metrics.Gauge",
            "name": "worker1:memory.heap.committed",
            "value": "179830784"
        }
    }, {
        "time": "1450759137860",
        "value": {
            "$type": "io.gearpump.metrics.Metrics.Gauge",
            "name": "worker1:memory.total.committed",
            "value": "210239488"
        }
    }, {
        "time": "1450759137860",
        "value": {"$type": "io.gearpump.metrics.Metrics.Gauge", "name": "worker0:thread.daemon.count", "value": "18"}
    }, {
        "time": "1450759137860",
        "value": {
            "$type": "io.gearpump.metrics.Metrics.Gauge",
            "name": "worker1:memory.heap.used",
            "value": "123139640"
        }
    }]
}
```

## Application service


### GET api/v1.0/appmaster/&lt;appId&gt;?detail=&lt;true|false&gt;
Query information of an specific application of Id appId

Example:

```bash
curl http://127.0.0.1:8090/api/v1.0/appmaster/1?detail=true
```

Sample Response:

```
{
  "appId": 1,
  "appName": "wordCount",
  "processors": [
    [
      0,
      {
        "id": 0,
        "taskClass": "io.gearpump.streaming.examples.wordcount.Split",
        "parallelism": 1,
        "description": "",
        "taskConf": {
          "_config": {}
        },
        "life": {
          "birth": "0",
          "death": "9223372036854775807"
        },
        "executors": [
          1
        ],
        "taskCount": [
          [
            1,
            {
              "count": 1
            }
          ]
        ]
      }
    ],
    [
      1,
      {
        "id": 1,
        "taskClass": "io.gearpump.streaming.examples.wordcount.Sum",
        "parallelism": 1,
        "description": "",
        "taskConf": {
          "_config": {}
        },
        "life": {
          "birth": "0",
          "death": "9223372036854775807"
        },
        "executors": [
          0
        ],
        "taskCount": [
          [
            0,
            {
              "count": 1
            }
          ]
        ]
      }
    ]
  ],
  "processorLevels": [
    [
      0,
      0
    ],
    [
      1,
      1
    ]
  ],
  "dag": {
    "vertexList": [
      0,
      1
    ],
    "edgeList": [
      [
        0,
        "io.gearpump.partitioner.HashPartitioner",
        1
      ]
    ]
  },
  "actorPath": "akka.tcp://app1-executor-1@127.0.0.1:52212/user/daemon/appdaemon1/$c/appmaster",
  "clock": "1450759382430",
  "executors": [
    {
      "executorId": 0,
      "executor": "akka.tcp://app1system0@127.0.0.1:52240/remote/akka.tcp/app1-executor-1@127.0.0.1:52212/user/daemon/appdaemon1/$c/appmaster/executors/0#-1554950276",
      "workerId": 1,
      "status": "active"
    },
    {
      "executorId": 1,
      "executor": "akka.tcp://app1system1@127.0.0.1:52241/remote/akka.tcp/app1-executor-1@127.0.0.1:52212/user/daemon/appdaemon1/$c/appmaster/executors/1#928082134",
      "workerId": 0,
      "status": "active"
    },
    {
      "executorId": -1,
      "executor": "akka://app1-executor-1/user/daemon/appdaemon1/$c/appmaster",
      "workerId": 1,
      "status": "active"
    }
  ],
  "startTime": "1450758117306",
  "uptime": "1268472",
  "user": "lisa",
  "homeDirectory": "/usr/lisa/gearpump/",
  "logFile": "logs/",
  "historyMetricsConfig": {
    "retainHistoryDataHours": 72,
    "retainHistoryDataIntervalMs": 3600000,
    "retainRecentDataSeconds": 300,
    "retainRecentDataIntervalMs": 15000
  }
}
```

### DELETE api/v1.0/appmaster/&lt;appId&gt;
shutdown application appId

### GET api/v1.0/appmaster/&lt;appId&gt;/stallingtasks
Query list of unhealthy tasks of an specific application of Id appId

Example:

```bash
curl http://127.0.0.1:8090/api/v1.0/appmaster/2/stallingtasks
```

Sample Response:

```
{
  "tasks": [
    {
      "processorId": 0,
      "index": 0
    }
  ]
}
```


### GET api/v1.0/appmaster/&lt;appId&gt;/config
Query the configuration of specific application appId

Example:

```bash
curl http://127.0.0.1:8090/api/v1.0/appmaster/1/config
```

Sample Response:

```
{
    "gearpump" : {
        "appmaster" : {
            "extraClasspath" : "",
            "vmargs" : "-server -Xms512M -Xmx1024M -Xss1M -XX:+HeapDumpOnOutOfMemoryError -XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=80 -XX:+UseParNewGC -XX:NewRatio=3"
        },
        "cluster" : {
            "masters" : [
                "127.0.0.1:3000"
            ]
        },
        "executor" : {
            "extraClasspath" : "",
            "vmargs" : "-server -Xms512M -Xmx1024M -Xss1M -XX:+HeapDumpOnOutOfMemoryError -XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=80 -XX:+UseParNewGC -XX:NewRatio=3"
        },
        "jarstore" : {
            "rootpath" : "jarstore/"
        },
        "log" : {
            "application" : {
                "dir" : "logs"
            },
            "daemon" : {
                "dir" : "logs"
            }
        },
        "metrics" : {
            "enabled" : true,
            "graphite" : {
                "host" : "127.0.0.1",
                "port" : 2003
            },
            "logfile" : {},
            "report-interval-ms" : 15000,
            "reporter" : "akka",
            "retainHistoryData" : {
                "hours" : 72,
                "intervalMs" : 3600000
            },
            "retainRecentData" : {
                "intervalMs" : 15000,
                "seconds" : 300
            },
            "sample-rate" : 10
        },
        "netty" : {
            "base-sleep-ms" : 100,
            "buffer-size" : 5242880,
            "flush-check-interval" : 10,
            "max-retries" : 30,
            "max-sleep-ms" : 1000,
            "message-batch-size" : 262144
        },
        "netty-dispatcher" : "akka.actor.default-dispatcher",
        "scheduling" : {
            "scheduler-class" : "io.gearpump.cluster.scheduler.PriorityScheduler"
        },
        "serializers" : {
            "[B" : "",
            "[C" : "",
            "[D" : "",
            "[F" : "",
            "[I" : "",
            "[J" : "",
            "[Ljava.lang.String;" : "",
            "[S" : "",
            "[Z" : "",
            "io.gearpump.Message" : "io.gearpump.streaming.MessageSerializer",
            "io.gearpump.streaming.task.Ack" : "io.gearpump.streaming.AckSerializer",
            "io.gearpump.streaming.task.AckRequest" : "io.gearpump.streaming.AckRequestSerializer",
            "io.gearpump.streaming.task.LatencyProbe" : "io.gearpump.streaming.LatencyProbeSerializer",
            "io.gearpump.streaming.task.TaskId" : "io.gearpump.streaming.TaskIdSerializer",
            "scala.Tuple1" : "",
            "scala.Tuple2" : "",
            "scala.Tuple3" : "",
            "scala.Tuple4" : "",
            "scala.Tuple5" : "",
            "scala.Tuple6" : "",
            "scala.collection.immutable.$colon$colon" : "",
            "scala.collection.immutable.List" : ""
        },
        "services" : {
            # gear.conf: 112
            "host" : "127.0.0.1",
            # gear.conf: 113
            "http" : 8090,
            # gear.conf: 114
            "ws" : 8091
        },
        "task-dispatcher" : "akka.actor.pined-dispatcher",
        "worker" : {
            # reference.conf: 100
            # # How many slots each worker contains
            "slots" : 100
        }
    }
}

```

### GET api/v1.0/appmaster/&lt;appId&gt;/metrics/&lt;query_path&gt;?readLatest=&lt;true|false&gt;&aggregator=&lt;aggregator_class&gt;
Query metrics information of a specific application appId
Filter metrics with path metrics path

aggregator points to a aggregator class, which will aggregate on the current metrics, and return a smaller set.

Example:

```bash
curl http://127.0.0.1:8090/api/v1.0/appmaster/1/metrics/app1?readLatest=true&aggregator=io.gearpump.streaming.metrics.ProcessorAggregator
```

Sample Response:

```
{
    "path"
:
    "worker", "metrics"
:
    [{
        "time": "1450759137860",
        "value": {
            "$type": "io.gearpump.metrics.Metrics.Gauge",
            "name": "worker1:memory.total.used",
            "value": "152931440"
        }
    }, {
        "time": "1450759137860",
        "value": {"$type": "io.gearpump.metrics.Metrics.Gauge", "name": "worker1:thread.daemon.count", "value": "18"}
    }, {
        "time": "1450759137860",
        "value": {
            "$type": "io.gearpump.metrics.Metrics.Gauge",
            "name": "worker0:memory.heap.used",
            "value": "123139640"
        }
    }, {
        "time": "1450759137860",
        "value": {
            "$type": "io.gearpump.metrics.Metrics.Gauge",
            "name": "worker0:memory.total.max",
            "value": "997457920"
        }
    }, {
        "time": "1450759137860",
        "value": {
            "$type": "io.gearpump.metrics.Metrics.Gauge",
            "name": "worker0:memory.heap.committed",
            "value": "179830784"
        }
    }, {
        "time": "1450759137860",
        "value": {"$type": "io.gearpump.metrics.Metrics.Gauge", "name": "worker0:thread.count", "value": "28"}
    }, {
        "time": "1450759137860",
        "value": {"$type": "io.gearpump.metrics.Metrics.Gauge", "name": "worker0:memory.heap.max", "value": "880017408"}
    }, {
        "time": "1450759137860",
        "value": {"$type": "io.gearpump.metrics.Metrics.Gauge", "name": "worker1:memory.heap.max", "value": "880017408"}
    }, {
        "time": "1450759137860",
        "value": {
            "$type": "io.gearpump.metrics.Metrics.Gauge",
            "name": "worker0:memory.total.committed",
            "value": "210239488"
        }
    }, {
        "time": "1450759137860",
        "value": {
            "$type": "io.gearpump.metrics.Metrics.Gauge",
            "name": "worker0:memory.total.used",
            "value": "152931440"
        }
    }, {
        "time": "1450759137860",
        "value": {"$type": "io.gearpump.metrics.Metrics.Gauge", "name": "worker1:thread.count", "value": "28"}
    }, {
        "time": "1450759137860",
        "value": {
            "$type": "io.gearpump.metrics.Metrics.Gauge",
            "name": "worker1:memory.total.max",
            "value": "997457920"
        }
    }, {
        "time": "1450759137860",
        "value": {
            "$type": "io.gearpump.metrics.Metrics.Gauge",
            "name": "worker1:memory.heap.committed",
            "value": "179830784"
        }
    }, {
        "time": "1450759137860",
        "value": {
            "$type": "io.gearpump.metrics.Metrics.Gauge",
            "name": "worker1:memory.total.committed",
            "value": "210239488"
        }
    }, {
        "time": "1450759137860",
        "value": {"$type": "io.gearpump.metrics.Metrics.Gauge", "name": "worker0:thread.daemon.count", "value": "18"}
    }, {
        "time": "1450759137860",
        "value": {
            "$type": "io.gearpump.metrics.Metrics.Gauge",
            "name": "worker1:memory.heap.used",
            "value": "123139640"
        }
    }]
}
```

### GET api/v1.0/appmaster/&lt;appId&gt;/errors
Get task error messages

Example:

```bash
curl http://127.0.0.1:8090/api/v1.0/appmaster/1/errors
```

Sample Response:

```
{"time":"0","error":null}
```

### POST api/v1.0/appmaster/&lt;appId&gt;/restart
Restart the application

## Executor Service

### GET api/v1.0/appmaster/&lt;appId&gt;/executor/&lt;executorid&gt;/config
Get executor config

Example:

```bash
curl http://127.0.0.1:8090/api/v1.0/appmaster/1/executor/1/config
```

Sample Response:

```
{
  "extensions": [
    "akka.contrib.datareplication.DataReplication$"
  ]
  "akka": {
    "loglevel": "INFO"
    "log-dead-letters": "off"
    "log-dead-letters-during-shutdown": "off"
    "actor": {
      ## Master forms a akka cluster
      "provider": "akka.cluster.ClusterActorRefProvider"
    }
    "cluster": {
      "roles": ["master"]
      "auto-down-unreachable-after": "15s"
    }
    "remote": {
      "log-remote-lifecycle-events": "off"
    }
  }
}
```

### GET api/v1.0/appmaster/&lt;appId&gt;/executor/&lt;executorid&gt;
Get executor information.

Example:

```bash
curl http://127.0.0.1:8090/api/v1.0/appmaster/1/executor/1
```

Sample Response:

```
{
  "id": 1,
  "workerId": 0,
  "actorPath": "akka.tcp://app1system1@127.0.0.1:52241/remote/akka.tcp/app1-executor-1@127.0.0.1:52212/user/daemon/appdaemon1/$c/appmaster/executors/1",
  "logFile": "logs/",
  "status": "active",
  "taskCount": 1,
  "tasks": [
    [
      0,
      [
        {
          "processorId": 0,
          "index": 0
        }
      ]
    ]
  ],
  "jvmName": "21304@lisa"
}
```
