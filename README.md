## GearPump [![Build Status](https://travis-ci.org/intel-hadoop/gearpump.svg?branch=master)](https://travis-ci.org/intel-hadoop/gearpump?branch=master) [![codecov.io](https://codecov.io/github/intel-hadoop/gearpump/coverage.svg?branch=master)](https://codecov.io/github/intel-hadoop/gearpump?branch=master) 
 
[![download](https://raw.githubusercontent.com/clockfly/icons/master/gearpump-0.2-rc1.jpg)](https://github.com/intel-hadoop/gearpump/releases)
 
GearPump is a lightweight real-time big data streaming engine. It is inspired by recent advances in the [Akka](https://github.com/akka/akka) framework and a desire to improve on existing streaming frameworks.

![](https://raw.githubusercontent.com/clockfly/gearpump/master/doc/logo/logo.png)

The	name	GearPump	is	a	reference to	the	engineering term “gear	pump,”	which	is	a	super simple
pump	that	consists of	only	two	gears,	but	is	very	powerful at	streaming water.

![](http://www.gearpump.io/site/img/dashboard.gif)

We model streaming within the Akka actor hierarchy.

![](https://raw.githubusercontent.com/intel-hadoop/gearpump/master/doc/actor_hierarchy.png)

Per initial benchmarks we are able to process 11 million messages/second (100 bytes per message) with a 17ms latency on a 4-node cluster.

![](https://raw.githubusercontent.com/intel-hadoop/gearpump/master/doc/dashboard.png)

For steps to reproduce the performance test, please check [Performance benchmark](http://www.gearpump.io/site/0.3/performance/)

## Design Document

There is a 20 pages technical paper on typesafe blog, with technical highlights https://typesafe.com/blog/gearpump-real-time-streaming-engine-using-akka

## Introduction and Motivation

Check [Introduction](http://www.gearpump.io/site/0.3/introduction/)

## Getting Started

Check guide [Get Started](http://www.gearpump.io/site/0.3/getstarted/)

## How to Build

1). Clone the GearPump repository

```bash
  git clone https://github.com/intel-hadoop/gearpump.git
  cd gearpump
```

2). Build package

```bash
  ## Please use scala 2.11
  ## The target package path: target/gearpump-$VERSION.tar.gz
  sbt clean assembly packArchive ## Or use: sbt clean assembly pack-archive
```

  After the build, there will be a package file gearpump-${version}.tar.gz generated under target/ folder.
  
  **NOTE:**
The build requires network connection. If you are behind an enterprise proxy, make sure you have set the proxy in your env before running the build commands. 
For windows:

```bash
Set HTTP_PROXY=http://host:port
set HTTPS_PROXT= http://host:port
```

For Linux:

```bash
export HTTP_PROXY=http://host:port
export HTTPS_PROXT= http://host:port
```

After the build, the package directory layout looks like this: [Layout](http://www.gearpump.io/site/0.3/getstarted/#gearpump-package-structure)


## Concepts

Check guide [Concepts](http://www.gearpump.io/site/0.3/concepts/)

## How to write a GearPump Application

Check guide [Streaming Application Developer Guide](http://www.gearpump.io/site/0.3/how_to_write_a_streaming_application/)

## How to manage the cluster

Check [Admin Guide](http://www.gearpump.io/site/0.3/adminguide/)

# Maven dependencies

Check [Maven settings](http://www.gearpump.io/site/downloads/downloads/#maven)

## Further information

- Document site [gearpump.io](http://gearpump.io)
- User List: [gearpump-user](https://groups.google.com/forum/#!forum/gearpump-user).
- Report issues: [issue tracker](https://github.com/intel-hadoop/gearpump/issues)

## Contributors (time order)

* [Sean Zhong](https://github.com/clockfly)
* [Kam Kasravi](https://github.com/kkasravi)
* [Manu Zhang](https://github.com/manuzhang)
* [Huafeng Wang](https://github.com/huafengw)
* [Weihua Jiang](https://github.com/whjiang)
* [Suneel Marthi](https://github.com/smarthi)
* [Stanley Xu](https://github.com/stanleyxu2005)
* [Tomasz Targonski](https://github.com/TomaszT)

## Contacts:

Please use the google user list if possible. For things that are not OK to be shared in maillist, please contact:
xiang.zhong@intel.com
kam.d.kasravi@intel.com
weihua.jiang@intel.com

## License

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0

## Acknowledgement

The netty transport code work is based on [Apache Storm](http://storm.apache.org). Thanks Apache Storm contributors.

Thanks to Jetbrains for providing [IntelliJ IDEA Free Open Source License](https://www.jetbrains.com/buy/opensource/?product=idea).


