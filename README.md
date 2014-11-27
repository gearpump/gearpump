GearPump
========

[![Build Status](https://travis-ci.org/intel-hadoop/gearpump.svg?branch=master)](https://travis-ci.org/intel-hadoop/gearpump?branch=master)
[![codecov.io](https://codecov.io/github/intel-hadoop/gearpump/coverage.svg?branch=master)](https://codecov.io/github/intel-hadoop/gearpump?branch=master)

![](https://raw.githubusercontent.com/clockfly/gearpump/master/doc/logo/logo.png)

#####This is an on-going effort that's not mature yet. Presently there is no load balancing yet, though we want to build support for those in weeks. We will update the wiki about the detailed status and design.

An Actor Driven streaming framework, inspired by MillWheel, Storm, Spark Streaming, and Samza.

Per initial benchmarks we are able to process 2 million messages/second (100 bytes per message) with a 30ms latency on a 4-node cluster.

###Why we name it Gearpump
The name Gearpump is a reference the engineering term “Gear Pump”, which is a super simple pump that consists of only two gears, but is very powerful at streaming water from left to right.

###Actor Hierarchy
![](https://raw.githubusercontent.com/intel-hadoop/gearpump/master/doc/actor_hierarchy.png)

###How to Build
  ```bash
  ## Build Gearpump
  sbt clean publishLocal pack
  ```
  This will generate scripts under core/target/pack/bin, examples/target/pack/bin and rest/target/pack/bin

###How to test
  ```bash
  ## Build Gearpump
  sbt clean jacoco:cover
  ```
  This will generate test coverage reports for each component under its target/jacoco/html/index.html.

##How to Package for distribution
  ```bash
  ## Package Gearpump
  sbt clean pack-archive
  ```
  This will produce target/gearpump${version}.tar.gz which contains the ./bin and ./lib files.

##How to Install to /usr/local
  ```bash
  ## Run Build step above
  cd core/target/pack
  sudo make install PREFIX="/usr/local"
  This will install scripts to run local, master or shell to /usr/local/bin and jars to /usr/local/lib.
  ```

###Local Mode

1. Start Local Cluster in same process
  ```bash
  ## By default, it will create 4 workers
  core/target/pack/bin/local -port 3000
  ```

2. Start an Example
  
  ```bash
  ## Run WordCount example
  target/pack/bin/gear app -jar ./examples/wordcount/target/pack/lib/gearpump-examples-wordcount-0.2-SNAPSHOT.jar org.apache.gearpump.streaming.examples.wordcount.WordCount -master 127.0.0.1:3000
  ```
  ```bash
  ## Run SOL example
  target/pack/bin/gear app -jar ./examples/sol/target/pack/lib/gearpump-examples-sol-0.2-SNAPSHOT.jar org.apache.gearpump.streaming.examples.sol.SOL -master 127.0.0.1:3000
  ```
  ```bash
  ## Run KafkaWordCount example
  target/pack/bin/gear app -jar ./examples/kafka/target/pack/lib/gearpump-examples-kafka-0.2-SNAPSHOT.jar org.apache.gearpump.streaming.examples.kafka.wordcount.KafkaWordCount -master 127.0.0.1:3000
  ```
  ```bash
  ## Run Fsio example
  target/pack/bin/gear app -jar ./examples/fsio/target/pack/lib/gearpump-examples-fsio-0.2-SNAPSHOT.jar org.apache.gearpump.streaming.examples.fsio.SequenceFileIO -master 127.0.0.1:3000 -input <input> -output <output>
  ```


###Cluster Mode

1. modify target/pack/conf/application.conf, set "gearpump.cluster.masters" to a list of master nodes. The target/pack/conf/application.conf must be synchronized on all nodes in the cluster.

  ```
  gearpump {
   ...
  cluster {
    masters = ["node1:3000"]
  }
  }
  ```
  
2. On node1, Start Master
  ```bash
  ## on node1
  target/pack/bin/master -ip node1 -port 3000  
  ```

3. On any machine, Start workers. You can start multiple workers on same on different machines. Worker will read the master location information "gearpump.cluster.masters" from target/pack/conf/application.conf

  ```bash
  target/pack/bin/worker
  ```
  
4. Start a Client Example 
  ```bash
  ## Run WordCount example
  target/pack/bin/gear app -jar ./examples/wordcount/target/pack/lib/gearmp-examples-wordcount-0.2-SNAPSHOT.jar org.apache.gearpump.streaming.examples.wordcount.WordCount -master 127.0.0.1:3000
  ```

###Master HA

We allow to start master on multiple nodes. For example, if we start master on 5 nodes, then we can at most tolerate 2 master nodes failure. 

1. modify target/pack/conf/application.conf, set "gearpump.cluster.masters" to a list of master nodes. The target/pack/conf/application.conf must be synchronized on all nodes in the cluster.

  ```
  gearpump {
   ...
  cluster {
    masters = ["node1:3000", "node2:3000", "node3:3000"]
  }
  }
  ```

2. On node1, node2, node3, Start Master
  ```bash
  ## on node1
  target/pack/bin/master -ip node1 -port 3000  
  
  ## on node2
  target/pack/bin/master -ip node2 -port 3000  
  
  ## on node3
  target/pack/bin/master -ip node3 -port 3000  
  ```  

3. You can kill any node, the master HA will take effect. It can take up to 15 seconds for master node to failover. You can change the failover timeout time by setting "master.akka.cluster.auto-down-unreachable-after"
  
###Metrics and Dashboard
Gearpump use Graphite for the metrics dashboard. By default, metrics is disabled. If you want to use metrics, you need to install a graphite to get the metrics.

After than, you need to configure the conf/application.conf

    ```
	gearpump.metrics.enabled = true  # Default is false, thus metrics is not enabled.
	gearpump.metrics.graphite.host = "your actual graphite host name or ip"  
	gearpump.metrics.graphite.port = 2003   ## Your graphite port
	gearpump.metrics.sample.rate = 10        ## this means we will sample 1 message for every 10 messages
	```
For guide about how to install and configure Graphite, please check the Graphite website http://graphite.wikidot.com/.	For guide about how to use Grafana, please check guide in [doc/dashboard/README.md](doc/dashboard/README.md)

Here is how it looks like for grafana dashboard:

![](https://raw.githubusercontent.com/clockfly/gearpump/master/doc/dashboard.png)

Serialization
========================

The configuration for serialization is:

```
gearpump {
  serializers {
    "org.apache.gearpump.Message" = "org.apache.gearpump.streaming.MessageSerializer"
    "org.apache.gearpump.streaming.task.AckRequest" = "org.apache.gearpump.streaming.AckRequestSerializer"
    "org.apache.gearpump.streaming.task.Ack" = "org.apache.gearpump.streaming.AckSerializer"

    ## Use default serializer for this type
    "scala.Tuple2" = ""
  }
}
```

We use library kryo and akka-kryo library https://github.com/romix/akka-kryo-serialization. The following list contians supported value types.

```

# gearpump types
Message
AckRequest
Ack

# akka types
akka.actor.ActorRef

# scala types
scala.Enumeration#Value
scala.collection.mutable.Map[_, _]
scala.collection.immutable.SortedMap[_, _]
scala.collection.immutable.Map[_, _]
scala.collection.immutable.SortedSet[_]
scala.collection.immutable.Set[_]
scala.collection.mutable.SortedSet[_]
scala.collection.mutable.Set[_]
scala.collection.generic.MapFactory[scala.collection.Map]
scala.collection.generic.SetFactory[scala.collection.Set]
scala.collection.Traversable[_]
Tuple2
Tuple3


# java complex types
byte[]
char[]
short[]
int[]
long[]
float[]
double[]
boolean[]
String[]
Object[]
BigInteger
BigDecimal
Class
Date
Enum
EnumSet
Currency
StringBuffer
StringBuilder
TreeSet
Collection
TreeMap
Map
TimeZone
Calendar
Locale

## Primitive types
int
String
float
boolean
byte
char
short
long
double
void
```

Acknowledge
========================
The netty transport code work is based on apache storm. Thanks to apache storm contributors.
