GearPump
========

![](http://i.istockimg.com/file_thumbview_approve/27804028/3/stock-illustration-27804028-gear-pump.jpg)


A Actor Driven event processing framework.
It contains two part, Gears and GearPump.

Gears is a general resource scheduler, it is design to support multiple Heterogeneous resource scheduling requirements.
GearPump is a data flow processing engine built on top of Gears.


How to run gear pump
========

###Local Mode
1. Start a KV Service, on local port 80
  ```bash
  cd C:\myData\gearpump\core
  mvn exec:java -Dexec.mainClass="org.apache.gearpump.service.SimpleKVService" -Dexec.args="80"
  ```

2. Start a Local Cluster, this will start the local cluster.
  ```bash
  mvn exec:java -Dexec.mainClass="org.apache.gears.cluster.LocalCluster" -Dexec.args="http://127.0.0.1:80/kv"
  ```

  To run in single process mode, add an extra system property
  ```bash
  mvn exec:java -Dexec.mainClass="org.apache.gears.cluster.LocalCluster"  -DLOCAL -Dexec.args="http://127.0.0.1:80/kv"
  ````
3. Start a Application,
  ```bash
  mvn exec:java -Dexec.mainClass="org.apache.gearpump.app.examples.wordcount.WordCount"  -Dexec.args="http://127.0.0.1:80/kv"
  ```

###Remoe Mode


