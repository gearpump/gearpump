
## Gearpump  [![GitHub release](https://img.shields.io/github/release/gearpump/gearpump.svg)](http://www.gearpump.io/download.html) [![GitHub license](https://img.shields.io/github/license/gearpump/gearpump.svg)](https://github.com/gearpump/gearpump/blob/master/LICENSE)

[![Build Status](https://travis-ci.org/gearpump/gearpump.svg?branch=master)](https://travis-ci.org/gearpump/gearpump?branch=master) [![codecov.io](https://codecov.io/github/gearpump/gearpump/coverage.svg?branch=master)](https://codecov.io/github/gearpump/gearpump?branch=master)

Web site: http://gearpump.io

[![download](https://raw.githubusercontent.com/clockfly/icons/master/gearpump-0.2-rc1.jpg)](http://www.gearpump.io/download.html)

Gearpump is a lightweight real-time big data streaming engine. It is inspired by recent advances in the [Akka](https://github.com/akka/akka) framework and a desire to improve on existing streaming frameworks.

![](https://raw.githubusercontent.com/clockfly/gearpump/master/doc/logo/logo.png)

The	name	Gearpump	is	a	reference to	the	engineering term “gear	pump,”	which	is	a	super simple
pump	that	consists of	only	two	gears,	but	is	very	powerful at	streaming water.

![](http://www.gearpump.io/img/dashboard.gif)

We model streaming within the Akka actor hierarchy.

![](https://raw.githubusercontent.com/gearpump/gearpump/master/docs/img/actor_hierarchy.png)

Per initial benchmarks we are able to process 11 million messages/second (100 bytes per message) with a 17ms latency on a 4-node cluster.

![](https://raw.githubusercontent.com/gearpump/gearpump/master/docs/img/dashboard.png)

For steps to reproduce the performance test, please check [Performance benchmark](http://www.gearpump.io/releases/latest/performance-report.html)

## Design Document

There is a 20 pages technical paper on typesafe blog, with technical highlights https://typesafe.com/blog/gearpump-real-time-streaming-engine-using-akka

## Introduction and Motivation

Check [Introduction](http://www.gearpump.io/overview.html)

## Getting Started

Check guide [Get Started](http://www.gearpump.io/releases/latest/submit-your-1st-application.html)

## How to Build

1). Clone the Gearpump repository

```bash
  git clone https://github.com/gearpump/gearpump.git
  cd gearpump
```

2). Build package

```bash
  ## Please use scala 2.11 or 2.10
  ## The target package path: output/target/gearpump-${version}.zip
  sbt clean +assembly +packArchiveZip
```

  After the build, there will be a package file gearpump-${version}.zip generated under output/target/ folder.

  To build scala document, use
```bash
   ## Will generate the scala doc under target/scala_2.xx/unidoc/
   sbt unidoc
```  

  **NOTE:**
The build requires network connection. If you are behind an enterprise proxy, make sure you have set the proxy in your env before running the build commands.
For windows:

```bash
set HTTP_PROXY=http://host:port
set HTTPS_PROXY= http://host:port
```

For Linux:

```bash
export HTTP_PROXY=http://host:port
export HTTPS_PROXY= http://host:port
```


## How to run Gearpump integration test
Gearpump has an integration test system which is based on Docker.

### Prerequisite of integration test
To run Gearpump integration test, we first must have Docker installed in your PATH.

And you need to have Docker well configured if you are behind firewall.
You can run command `docker pull stanleyxu2005/gpct-jdk8` to check whether you have Docker installed and well configured.

#### Set docker proxy if you behind a firewall
In ubuntu, you can modify file `/etc/default/docker` and enable the http_proxy setting.

### Run integration test
After Docker well configured, you can run the whole integration test via command:
```bash
sbt it:test
```

## Concepts

Check guide [Concepts](http://www.gearpump.io/releases/latest/basic-concepts.html)

## How to write a Gearpump Application

Check guide [Streaming Application Developer Guide](http://www.gearpump.io/releases/latest/dev-write-1st-app.html)


# Maven dependencies

Check [Maven settings](http://www.gearpump.io/releases/latest/maven-setting.html)

## Further information

- Document site [gearpump.io](http://gearpump.io)
- User List: [gearpump-user](https://groups.google.com/forum/#!forum/gearpump-user).
- Report issues: [issue tracker](https://github.com/gearpump/gearpump/issues)

## Contributors (time order)

* [Sean Zhong](https://github.com/clockfly)
* [Kam Kasravi](https://github.com/kkasravi)
* [Manu Zhang](https://github.com/manuzhang)
* [Huafeng Wang](https://github.com/huafengw)
* [Weihua Jiang](https://github.com/whjiang)
* [Suneel Marthi](https://github.com/smarthi)
* [Stanley Xu](https://github.com/stanleyxu2005)
* [Tomasz Targonski](https://github.com/TomaszT)
* [Sun Kewei](https://github.com/skw1992)

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
