GearPump
========

![](http://i.istockimg.com/file_thumbview_approve/27804028/3/stock-illustration-27804028-gear-pump.jpg)


A Actor Driven event processing framework.
It contains two part, Gears and GearPump.

Gears is a general resource scheduler, it is design to support multiple Heterogeneous resource scheduling requirements.
GearPump is a data flow processing engine built on top of Gears.

###Local Mode
1. Start Local Cluster in same process
  ```bash
  ## Create Cluster on port localhost:80, create 4 worker, 4 worker exists in same process
  java -cp <classpath> org.apache.gears.cluster.Starter local -port 80 -sameprocess true -workernum 4
  ```

2. Start Local Cluster in different process
  ```bash
  ## Create Cluster on port localhost:80, create 4 worker, 4 worker exists in seperate process
  java -cp <classpath> org.apache.gears.cluster.Starter local -port 80 -sameprocess false -workernum 4
  ```
3. Start Client Example Code
  ```bash
  ## Create Application
  java -cp <classpath> org.apache.gearpump.app.examples.wordcount.WordCount <master ip> <master port>
  ```


###Cluster Mode
1. On 1 node, Start Master

  ```bash
  ## Create Master on <master ip>:80, 
  java -cp <classpath> master -port 80
  ```

2. On same or different machine, Start workers. If you want to start 3 worker, then you need to run this command 3 times.

  ```bash
  java -cp <classpath> worker -ip <master ip> -port <master port>
  ```
3. Start Client Example Code

  ```bash
  ## Create Application
  java -cp <classpath> org.apache.gearpump.app.examples.wordcount.WordCount <master ip> <master port>
  ```
