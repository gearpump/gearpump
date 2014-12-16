## GearPump [![Build Status](https://travis-ci.org/intel-hadoop/gearpump.svg?branch=master)](https://travis-ci.org/intel-hadoop/gearpump?branch=master) [![codecov.io](https://codecov.io/github/intel-hadoop/gearpump/coverage.svg?branch=master)](https://codecov.io/github/intel-hadoop/gearpump?branch=master)

GearPump is a lightweight real-time big data streaming engine. It is inspired by recent advances in the [Akka](https://github.com/akka/akka) framework and a desire to improve on existing streaming frameworks.

![](https://raw.githubusercontent.com/clockfly/gearpump/master/doc/logo/logo.png)

We model streaming within the Akka actor hierarchy.

![](https://raw.githubusercontent.com/intel-hadoop/gearpump/master/doc/actor_hierarchy.png)

Per initial benchmarks we are able to process 11 million messages/second (100 bytes per message) with a 17ms latency on a 4-node cluster.

![](https://raw.githubusercontent.com/intel-hadoop/gearpump/master/doc/dashboard.png)

## Technical papers
There is a 20 pages technical paper on typesafe blog, with technical highlights https://typesafe.com/blog/gearpump-real-time-streaming-engine-using-akka


## Getting Started: WordCount

1. Clone the GearPump repository

  ```bash
  git clone https://github.com/intel-hadoop/gearpump.git
  cd gearpump
  ```

2. Start master

  We support [Master HA](https://github.com/intel-hadoop/gearpump/wiki/Run-Examples#master-ha) and allow master to start on multiple nodes. Modify `core/src/main/resources/reference.conf` and set `gearpump.cluster.masters` to the list of nodes you plan to start master on (e.g. node1).

  ```
  gearpump {
   ...
    cluster {
      masters = ["node1:3000"]
    }
  }
  ```

  Build a package, distribute to all nodes, and extract it.

  ```bash
  sbt clean packArchive
  ```

  Start master on the nodes you set in the conf previously.

  ```bash
  ## on node1
  cd gearpump-$VERSION
  bin/master -ip node1 -port 3000
  ```

3. Start worker

  Start multiple workers on one or more nodes. Worker will read the master location information  `gearpump.cluster.masters` from `reference.conf`.

  ```bash
  bin/worker
  ```

4. Distribute application jar and run

  Distribute wordcount jar `examples/wordcount/target/gearpump-examples-wordcount-$VERSION.jar` to one of cluster nodes and run with

  ```bash
  ## Run WordCount example
  bin/gear app -jar gearpump-examples-wordcount-$VERSION.jar org.apache.gearpump.streaming.examples.wordcount.WordCount -master node1:3000
  ```

Check the wiki pages for more on [build](https://github.com/intel-hadoop/gearpump/wiki/Build) and [running examples in local modes](https://github.com/intel-hadoop/gearpump/wiki/Run-Examples).

## How to write a GearPump Application

This is what a [GearPump WordCount](https://github.com/intel-hadoop/gearpump/tree/master/examples/wordcount/src/main/scala/org/apache/gearpump/streaming/examples/wordcount) looks like.

  ```scala
  class WordCount extends Starter with ArgumentsParser {
    private val LOG: Logger = LoggerFactory.getLogger(classOf[WordCount])

    override val options: Array[(String, CLIOption[Any])] = Array(
      "master" -> CLIOption[String]("<host1:port1,host2:port2,host3:port3>", required = true),
      "split" -> CLIOption[Int]("<how many split tasks>", required = false, defaultValue = Some(4)),
      "sum" -> CLIOption[Int]("<how many sum tasks>", required = false, defaultValue = Some(4)),
      "runseconds"-> CLIOption[Int]("<how long to run this example>", required = false, defaultValue = Some(60))
    )

    override def application(config: ParseResult) : AppDescription = {
      val splitNum = config.getInt("split")
      val sumNum = config.getInt("sum")
      val appConfig = Configs(Configs.SYSTEM_DEFAULT_CONFIG)
      val partitioner = new HashPartitioner()
      val split = TaskDescription(classOf[Split].getCanonicalName, splitNum)
      val sum = TaskDescription(classOf[Sum].getCanonicalName, sumNum)
      val app = AppDescription("wordCount", classOf[AppMaster].getCanonicalName, appConfig, Graph(split ~ partitioner ~> sum))
      app
    }
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
