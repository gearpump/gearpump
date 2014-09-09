#####This is an on-going effort, and not mature yet. There is no HA feature, fault-tolerance, load balance yet, though we want to build support for those in weeks. We will update the wiki about the detailed status, the design, and the plan as soon as possible.


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
  ```
  This will install scripts to /usr/local/bin and jars to /usr/local/lib

###Local Mode
1. Start Local Cluster in same process
  ```bash
  ## Create Cluster on port localhost:8092, create 4 worker, 4 worker exists in same process
  target/pack/bin/local -port 8092 -sameprocess -workernum 4
  ```

2. Start Local Cluster in different process
  ```bash
  ## Create Cluster on port localhost:8092, create 4 worker, 4 worker exists in seperate process
  target/pack/bin/local -port 8092 -workernum 4
  ```
3. Start Client Example Code
  
  ```bash
  ## Create Application
  target/pack/bin/wordcount <master ip> <master port> <split number> <sum number> <runseconds>
  ```


###Cluster Mode
1. modify target/pack/conf/application.conf, set gearpump.clusters to a list of master node.
  ```
  gearpump {
   ...
  cluster {
    masters = ["127.0.0.1:3000"]
  }
  }
  ```
  
2. On 1 node, Start Master
  ```bash
  ## Create Master on <master ip>:8092, 
  target/pack/bin/master -ip 127.0.0.1 -port 3000
  ```

3. On same or different machine, Start workers. If you want to start 3 worker, then you need to run this command 3 times.

  ```bash
  target/pack/bin/worker
  ```
4. Start Client Example Code

  ```bash
  ## Create Application
  target/pack/bin/wordcount -masters 127.0.0.1:3000 -split <split number> -sum <sum number> -runseconds <runseconds>
  ```

###Metrics and Dashboard
Gearpump use Graphite for the metrics dashboard. You need to install a graphite to get the metrics, if you don't need metrics dashboard, you don't need to install Graphite.

After than, you need to configure the conf/application.conf

    ```
	gearpump.metrics.enabled = true  # Default is false, thus metrics is not enabled.
	gearpump.metrics.graphite.host = "your actual graphite host name or ip"  
	gearpump.metrics.graphite.port = 2003   ## Your graphite port
	gearpump.metrics.sample.rate = 10        ## this means we will sample 1 message for every 10 messages
	```
For guide about how to install and configure Graphite, please check the Graphite website http://graphite.wikidot.com/.	For guide about how to use Grafana, please check guide in [dashboard/README.md](dashboard/README.md)


Acknowledge
========================
The netty transport code work is based on apache storm. Thanks to apache storm contributors.
