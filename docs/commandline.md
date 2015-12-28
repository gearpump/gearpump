---
layout: global
displayTitle: Gearpump Client Commandline
title: Gearpump Client Commandline
description: Gearpump Client Commandline
---

The commands can be found at: "bin" folder of Gearpump binary.

**NOTE:** on MS Windows platform, please use window shell gear.bat script instead. bash script doesn't work well in cygwin/mingw.

### Submit an new application

You can use the command `gear` under the bin directory to submit, query and terminate an application:

```bash
gear app [-namePrefix <application name prefix>] [-conf <custom gearpump config file>] -jar xx.jar MainClass <arg1> <arg2> ...
```

### List all running applications
To list all running applications:

```bash
gear info  [-conf <custom gearpump config file>]
```

### Kill a running application
To kill an application:

```bash
gear kill -appid <application id>  [-conf <custom gearpump config file>]
```

### Submit a storm application to Gearpump Cluster
For example, to submit a storm application jar:

```bash
storm -verbose -config storm.yaml -jar storm-starter-${STORM_VERSION}.jar storm.starter.ExclamationTopology exclamation
```

[Storm Compatibility Guide](dev-storm.html)

### Start Gearpump Cluster on YARN
To start a Gearpump Cluster on YARN, you can:

```bash
yarnclient launch -package /usr/lib/gearpump/gearpump-{{ site.SCALA_BINARY_VERSION }}-{{ site.GEARPUMP_VERSION }}.zip
```
/usr/lib/gearpump/gearpump-{{ site.SCALA_BINARY_VERSION }}-{{ site.GEARPUMP_VERSION }}.zip should be available on HDFS.

Please check [YARN Deployment Guide](deployment-yarn.html) for more information.

### Start a local cluster
Masters and workers will be started in one machine:

```bash
local
```

Check [Deployment Guide for Local Cluster](deployment-local.html) for more information.

### Start master daemons

```bash
master -ip <Ip address> -port <port where this master is hooking>
```

Please check [Deployment for Standalone mode](deployment-standalone.html) for more information.

### Start worker daemons

```bash
worker
```

Please check [Deployment for Standalone mode](deployment-standalone.html) for more information.

### Start UI server

To start UI server, you can:

```bash
services  [-master <host:port>]
```