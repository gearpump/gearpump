## GearPump [![Build Status](https://travis-ci.org/intel-hadoop/gearpump.svg?branch=master)](https://travis-ci.org/intel-hadoop/gearpump?branch=master) [![codecov.io](https://codecov.io/github/intel-hadoop/gearpump/coverage.svg?branch=master)](https://codecov.io/github/intel-hadoop/gearpump?branch=master) [![Bitdeli Badge](https://d2weczhvl823v0.cloudfront.net/intel-hadoop/gearpump/trend.png)](https://bitdeli.com/free "Bitdeli Badge")
 
GearPump is a lightweight real-time big data streaming engine. It is inspired by recent advances in the [Akka](https://github.com/akka/akka) framework and a desire to improve on existing streaming frameworks.

The	name	GearPump	is	a	reference to	the	engineering term “gear	pump,”	which	is	a	super simple
pump	that	consists of	only	two	gears,	but	is	very	powerful at	streaming water.

![](https://raw.githubusercontent.com/clockfly/gearpump/master/doc/logo/logo.png)

We model streaming within the Akka actor hierarchy.

![](https://raw.githubusercontent.com/intel-hadoop/gearpump/master/doc/actor_hierarchy.png)

Per initial benchmarks we are able to process 11 million messages/second (100 bytes per message) with a 17ms latency on a 4-node cluster.

![](https://raw.githubusercontent.com/intel-hadoop/gearpump/master/doc/dashboard.png)

For steps to reproduce the performance test, please check [Performance benchmark](https://github.com/intel-hadoop/gearpump/wiki/How-we-do-benchmark)

## Technical papers
There is a 20 pages technical paper on typesafe blog, with technical highlights https://typesafe.com/blog/gearpump-real-time-streaming-engine-using-akka


## Getting Started: WordCount

1. Clone the GearPump repository

  ```bash
  git clone https://github.com/intel-hadoop/gearpump.git
  cd gearpump
  ```

2. Start master

  We support [Master HA](https://github.com/intel-hadoop/gearpump/wiki/Run-Examples#master-ha) and allow master to start on multiple nodes. 

  Build a package

  ```bash
  
  ## Please use scala 2.11
  ## The target package path: target/gearpump-$VERSION.tar.gz
  sbt clean assembly packArchive ## Or use: sbt clean assembly pack-archive
  ```
  
  Distribute the package to all nodes. Modify `conf/gear.conf` on all nodes. You MUST configure ```akka.remote.netty.tcp.hostname``` to make it point to your hostname(or ip), and `gearpump.cluster.masters` to represent a list of master nodes.

  ```
  ### Put Akka configuration here
  base {

    ##############################
    ### Required to change!!
    ### You need to set the ip address or hostname of this machine
    ###
    akka.remote.netty.tcp.hostname = "127.0.0.1"
  }

  #########################################
  ### This is the default configuration for gearpump
  ### To use the application, you at least need to change gearpump.cluster to point to right master
  #########################################
  gearpump {

    ##############################
    ### Required to change!!
    ### You need to set the master cluster address here
    ###
    ###
    ### For example, you may start three master
    ### on node1: bin/master -ip node1 -port 3000
    ### on node2: bin/master -ip node2 -port 3000
    ### on node3: bin/master -ip node3 -port 3000
    ###
    ### Then you need to set the cluster.masters = ["node1:3000","node2:3000","node3:3000"]
    cluster {
      masters = ["127.0.0.1:3000"]
    }
  }
  ```
 
  After this, start the master daemon on all nodes you have configured in `gearpump.cluster.masters`. If you have configured `gearpump.cluster.masters` to:
  
  ```
  gearpump{
     cluster {
      masters = ["node1:3000", "node2:3000"]
    }
  }
  ```
  
  Then start master daemon on ```node1``` and ```node2```.

  ```bash
  ## on node1
  cd gearpump-$VERSION
  bin/master -ip node1 -port 3000
  
  ## on node2
  cd gearpump-$VERSION
  bin/master -ip node1 -port 3000
  ```

3. Start worker

  Start multiple workers on one or more nodes. 
  
  Modify `conf/gear.conf` and make sure `gearpump.cluster.masters` points to the list of masters you started.  (e.g. node1).

  ```
  gearpump {
   ...
    cluster {
      masters = ["node1:3000"]
    }
  }
  ```

  ```bash
  bin/worker
  ```

4. Distribute application jar and run

  Distribute your application jar to one node of the cluster. For the built-in example jar, it is already located at `examples/gearpump-examples-assembly-$VERSION.jar`

  ```bash
  ## To run WordCount example
  bin/gear app -jar examples/gearpump-examples-assembly-$VERSION.jar org.apache.gearpump.streaming.examples.wordcount.WordCount -master node1:3000
  ```
  
  User can change the configuration by providing a "application.conf" in classpath. "application.conf" follows HCON format.

Check the wiki pages for more on [build](https://github.com/intel-hadoop/gearpump/wiki/Build) and [running examples in local modes](https://github.com/intel-hadoop/gearpump/wiki/Run-Examples).

## How to write a GearPump Application

This is what a [GearPump WordCount](https://github.com/intel-hadoop/gearpump/tree/master/examples/wordcount/src/main/scala/org/apache/gearpump/streaming/examples/wordcount) looks like.

  ```scala
  object WordCount extends App with ArgumentsParser {

    def application(config: ParseResult) : AppDescription = {
      val partitioner = new HashPartitioner()
      val split = TaskDescription(classOf[Split].getName, splitNum)
      val sum = TaskDescription(classOf[Sum].getName, sumNum)
      
      // Here we define the dag
      val dag = Graph(split ~ partitioner ~> sum)
      
      val app = AppDescription("wordCount", classOf[AppMaster].getName, appConfig, dag)
      app
    }
    
    val config = parse(args)
    val context = ClientContext(config.getString("master"))
    context.submit(application(config))
  }
  ```

For detailed description on writing a GearPump application, please check [Write GearPump Applications](https://github.com/intel-hadoop/gearpump/wiki/Write-GearPump-Applications) on the wiki.

## Further information

For more documentation and implementation details, please visit [GearPump Wiki](https://github.com/intel-hadoop/gearpump/wiki).

We'll have QnA and discussions at [GearPump User List](https://groups.google.com/forum/#!forum/gearpump-user).

Issues should be reported to [GearPump GitHub issue tracker](https://github.com/intel-hadoop/gearpump/issues) and contributions are welcomed via [pull requests](https://github.com/intel-hadoop/gearpump/pulls)

## Contributors (alphabetically)

* [Weihua Jiang](https://github.com/whjiang)
* [Kam Kasravi](https://github.com/kkasravi)
* [Suneel Marthi](https://github.com/smarthi)
* [Huafeng Wang](https://github.com/huafengw)
* [Manu Zhang](https://github.com/manuzhang)
* [Sean Zhong](https://github.com/clockfly)

## License

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0

## Acknowledgement

The netty transport code work is based on [Apache Storm](http://storm.apache.org). Thanks Apache Storm contributors.



