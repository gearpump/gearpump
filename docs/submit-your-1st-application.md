---
layout: global
displayTitle: Submit Your 1st Gearpump Application
title: submitapp
description: Submit Your 1st Gearpump Application
---

Before you can submit and run your first Gearpump application, we first need a running Gearpump service.
You can have multiple ways to such a cluster running ([Local mode](deployment-local.html),
[Standalone mode](deployment-standalone.html), [YARN mode](deployment-yarn.html)) or [Docker mode](deployment-docker.html).

The easiest way for a try is to run Gearpump in [Local mode](deployment-local.html)
which you even don't need a server to run. Any Linux desktop is sufficient for a run.

In below example, we assume your are running with ([Local mode](deployment-local.html).
If you running Gearpump in other modes, you will need to configure the Gearpump client to
make it know where to look for the Gearpump service by setting the `gear.conf` configuration path in classpath.
You need to change the parameter `gearpump.cluster.masters` to make it point to the correct Gearpump masters.
See [Configuration](deployment-configuration.html) for details.

## Steps to submit your first Application

### Step 1: Submit application
After the cluster is started, you can submit an example wordcount application to the cluster

Open another shell,

```bash
### To run WordCount example
bin/gear app -jar examples/gearpump-examples-assembly-{{site.GEARPUMP_VERSION}}.jar io.gearpump.streaming.examples.wordcount.WordCount
```

###  Step 2: Congratulation! You have your first application running! Open the UI and view the status

Now, the application is running, start the Web UI at [http://127.0.0.1:8090](http://127.0.0.1:8090) and check the status.
![Dashboard](/img/dashboard.gif)

You see, now it is up and running.

**NOTE:** the UI port setting can be defined in configuration, please check section [Configuration](deployment-configuration.html).

## A quick Look at the Web UI
TBD

## Other Application Examples
Besides wordcount, there are several other example applications. Please check the source tree examples/ for detail information.
