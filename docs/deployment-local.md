---
layout: global
displayTitle: Deploy Gearpump in Local Mode
title: Deploy Local Mode
description: Deploy Gearpump in Local Mode
---

You can start the Gearpump service in single JVM(local mode), or in a distributed cluster(cluster mode). To start the cluster in local mode, you can use the local /local.bat helper scripts, it is very useful for developing or troubleshooting.

Below are the steps to start a Gearpump service in **Local** mode:

### Step 1: Get your Gearpump binary ready
To get your Gearpump service running in local mode, you first need to have a Gearpump distribution binary ready.
Please follow [this guide](get-gearpump-distribution.html) to have the binary.  

### Step 2: Start the cluster
You can start a local mode cluster in single line

```bash
## start the master and 4 workers in single JVM. The master will listen on 3000
## you can Ctrl+C to kill the local cluster after you finished the startup tutorial.
bin/local
```

**NOTE:** You can change the default port by changing config "gearpump.cluster.masters" in conf/gear.conf,

**NOTE: Change the working directory**. Log files by default will be generated under current working directory. So, please "cd" to required working directly before running the shell commands.

**NOTE: Run as Daemon**. You can run it as a background process. For example, use [nohup](http://linux.die.net/man/1/nohup) on linux.

### Step 3: Start the Web UI server
Open another shell,

```bash
bin/services
```
You can manage the applications in UI [http://127.0.0.1:8090](http://127.0.0.1:8090) or by [Command Line tool](commandline.html).
