---
layout: global
displayTitle: Deploy Gearpump in Standalone Mode
title: Deploy Standalone Mode
description: Deploy Gearpump in Standalone Mode
---

Standalone mode is a distributed cluster mode. That is, Gearpump runs as service without the help from other services (e.g. YARN).

To deploy Gearpump in cluster mode, please first check that the [Pre-requisites](hardware-requirement.html) are met.


### How to Install
You need to have Gearpump binary at hand. Please refer to [How to get gearpump distribution](get-gearpump-distribution.html) to get the Gearpump binary.

You are suggested to unzip the package to same directory path on every machine you planned to install Gearpump.
To install Gearpump, you at least need to change the configuration in conf/gear.conf.

Config	| Default value	| Description
------------ | ---------------|------------
base.akka.remote.netty.tcp.hostname	| 127.0.0.1	 | Host or IP address of current machine. The ip/host need to be reachable from other machines in the cluster.
Gearpump.cluster.masters |	["127.0.0.1:3000"] |	List of all master nodes, with each item represents host and port of one master.
gearpump.worker.slots	 | 100 | how many slots this worker has

Besides this, there are other optional configurations related with logs, metrics, transports, ui. You can refer to [Configuration Guide](deployment-configuration.html) for more details.

### Start the Cluster Daemons in Standlone mode
In Standalone mode, you can start master and worker in different JVM.

##### To start master:
```bash
bin/master -ip xx -port xx
```

The ip and port will be checked against setting under conf/gear.conf, so you need to make sure they are consistent with settings in gear.conf.

**NOTE**: for high availability, please check [Master HA Guide](deployment-ha.html)

##### To start worker:
```bash
bin/worker
```

### Start UI

```bash
bin/services
```

After UI is started, you can browser http://{web_ui_host}:8090 to view the cluster status.

![Dashboard](/img/dashboard.gif)

**NOTE:** The UI port can be configured in gear.conf. Check [Configuration Guide](deployment-configuration.html) for information.
