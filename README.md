#####This is an on-going effort, and not mature yet. There is no at least once or exactly once, load balance yet, though we want to build support for those in weeks. We will update the wiki about the detailed status, the design, and the plan as soon as possible.


GearPump
========

![](https://raw.githubusercontent.com/clockfly/gearpump/master/project/logo/logo.png)


A Actor Driven streaming framework.

A initial benchmarks shows that we can process 2million messages/second (100 bytes per message) with latency around 30ms on a cluster of 4 nodes.


###Actor Hierarchy
![](https://raw.githubusercontent.com/clockfly/gearpump/master/doc/actor_hierachy.png)

###How to Build
  ```bash
  ## Build Gearpump
  sbt clean pack
  ```
  This will generate scripts under target/pack/bin

##How to Package for distribution
  ```bash
  ## Package Gearpump
  sbt clean pack-archive
  ```
  This will produce target/gearpump${version}.tar.gz which contains the ./bin and ./lib files.

##How to Install to /usr/local
  ```bash
  ## Run Build step above
  cd target/pack
  sudo make install PREFIX="/usr/local"
  This will install scripts to /usr/local/bin and jars to /usr/local/lib.
  ```

###Local Mode

1. Start Local Cluster in same process
  ```bash
  ## By default, it will create 4 workers
  target/pack/bin/local -port 3000
  ```

2. Start WordCount Example
  
  ```bash
  ## Create Application
  target/pack/bin/wordcount -master 127.0.0.1:3000
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
  
4. Start Client Example Code
  ```bash
  ## Create Application
  target/pack/bin/wordcount -master 127.0.0.1:3000
  ```

###Master HA

We allow to start master on multiple nodes. For example, if we start master on 5 nodes, then we can at most tolerate 2 master nodes failure. 

1. modify target/pack/conf/application.conf, set "gearpump.cluster.masters" to a list of master nodes. The target/pack/conf/application.conf must be synchronized on all nodes in the cluster.

  ```
  gearpump {
   ...
  cluster {
    masters = ["node1:3000, node2:3000, node3:3000"]
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
For guide about how to install and configure Graphite, please check the Graphite website http://graphite.wikidot.com/.	For guide about how to use Grafana, please check guide in [dashboard/README.md](dashboard/README.md)

Here is how it looks like for grafana dashboard:

![](https://raw.githubusercontent.com/clockfly/gearpump/master/doc/dashboard.png)

Acknowledge
========================
The netty transport code work is based on apache storm. Thanks to apache storm contributors.
