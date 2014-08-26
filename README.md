#####This is an on-going effort, and not mature yet. We will update the wiki about the status, the design, and the plan in weeks.


GearPump
========

![](https://raw.githubusercontent.com/clockfly/gearpump/master/project/logo/logo.png)


A Actor Driven event processing framework.


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
1. On 1 node, Start Master
  ```bash
  ## Create Master on <master ip>:8092, 
  target/pack/bin/master -port 8092
  ```

2. On same or different machine, Start workers. If you want to start 3 worker, then you need to run this command 3 times.

  ```bash
  target/pack/bin/worker -ip <master ip> -port <master port>
  ```
3. Start Client Example Code

  ```bash
  ## Create Application
  target/pack/bin/wordcount -ip <master ip> -port <master port> -split <split number> -sum <sum number> -runseconds <runseconds>
  ```

###Metrics and Dashboard
Gearpump use Graphite for the metrics dashboard. You need to install a graphite to get the metrics. 

After than, you need to configure the conf/application.conf

    ```
	gearpump.metrics.enabled = true
	gearpump.metrics.graphite.host = "your actual graphite host name or ip"  
	gearpump.metrics.graphite.port = 2003   ## Your graphite port
	gearpump.metrics.sample.rate = 10        ## this means we will sample 1 message for every 10 messages
	```
For guide about how to install and configure Graphite, please check the Graphite website http://graphite.wikidot.com/.	For guide about how to use Grafana, please check guide in [dashboard/README.md](dashboard/README.md)
