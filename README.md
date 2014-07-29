GearPump
========

![](http://i.istockimg.com/file_thumbview_approve/27804028/3/stock-illustration-27804028-gear-pump.jpg)


A Actor Driven event processing framework.
It contains two part, Gears and GearPump.

Gears is a general resource scheduler, it is design to support multiple Heterogeneous resource scheduling requirements.
GearPump is a data flow processing engine built on top of Gears.

###How to Build
  ```bash
  ## Build Gearpump
  mvn package
  ```
  You can find the gearpump-{version}.zip under gearpump-dist/binary/target/. The source zip can be found at: gearpump-dist/source/target/


###Local Mode
0. Unpack gearpump-{version}.zip to some place in your system

1. Start Local Cluster in same process
  ```bash
  ## Create Cluster on port localhost:80, create 4 worker, 4 worker exists in same process
  gearpump_local.sh -port 80 -sameprocess -workernum 4
  ```

2. Start Local Cluster in different process
  ```bash
  ## Create Cluster on port localhost:80, create 4 worker, 4 worker exists in seperate process
  gearpump_local -port 80 -workernum 4
  ```
3. Start Client Example Code
  
  ```bash
  ## Create Application
  wordcount.sh <master ip> <master port> <split number> <sum number> <runseconds>
  ```


###Cluster Mode
0. Unpack gearpump-{version}.zip to all machines in the cluster

1. On 1 node, Start Master
  ```bash
  ## Create Master on <master ip>:80, 
  gearpump_master.sh 80
  ```

2. On same or different machine, Start workers. If you want to start 3 worker, then you need to run this command 3 times.

  ```bash
  gearpump_worker.sh <master ip> <master port>
  ```
3. Start Client Example Code

  ```bash
  ## Create Application
  wordcount.sh <master ip> <master port>  <split number> <sum number> <runseconds>
  ```
