Standalone mode is a distributed cluster mode. That is, Gearpump runs as service without the help from other services (e.g. YARN).

To deploy Gearpump in cluster mode, please first check that the [Pre-requisites](hardware-requirement) are met.

### How to Install
You need to have Gearpump binary at hand. Please refer to [How to get gearpump distribution](get-gearpump-distribution) to get the Gearpump binary.

You are suggested to unzip the package to same directory path on every machine you planned to install Gearpump.
To install Gearpump, you at least need to change the configuration in `conf/gear.conf`.

Config	| Default value	| Description
------------ | ---------------|------------
gearpump.hostname	| "127.0.0.1"	 | Host or IP address of current machine. The ip/host need to be reachable from other machines in the cluster.
gearpump.cluster.masters |	["127.0.0.1:3000"] |	List of all master nodes, with each item represents host and port of one master.
gearpump.worker.slots	 | 1000 | how many slots this worker has

Besides this, there are other optional configurations related with logs, metrics, transports, ui. You can refer to [Configuration Guide](deployment-configuration) for more details.

### Start the Cluster Daemons in Standlone mode
In Standalone mode, you can start master and worker in different JVMs.

##### To start master:

	:::bash
	bin/master -ip xx -port xx

The ip and port will be checked against settings under `conf/gear.conf`, so you need to make sure they are consistent.

**NOTE:** You may need to execute `chmod +x bin/*` in shell to make the script file `master` executable.

**NOTE**: for high availability, please check [Master HA Guide](deployment-ha)

##### To start worker:

	:::bash
	bin/worker

### Start UI

	:::bash
	bin/services
	

After UI is started, you can browse to `http://{web_ui_host}:8090` to view the cluster status.
The default username and password is "admin:admin", you can check
[UI Authentication](deployment-ui-authentication) to find how to manage users.

![Dashboard](../img/dashboard.gif)

**NOTE:** The UI port can be configured in `gear.conf`. Check [Configuration Guide](deployment-configuration) for information.

### Bash tool to start cluster

There is a bash tool `bin/start-cluster.sh` can launch the cluster conveniently. You need to change the file `conf/masters`, `conf/workers` and `conf/dashboard` to specify the corresponding machines.
Before running the bash tool, please make sure the Gearpump package is already unzipped to the same directory path on every machine.
`bin/stop-cluster.sh` is used to stop the whole cluster of course.

The bash tool is able to launch the cluster without changing the `conf/gear.conf` on every machine. The bash sets the `gearpump.cluster.masters` and other configurations using JAVA_OPTS.
However, please note when you log into any these unconfigured machine and try to launch the dashboard or submit the application, you still need to modify `conf/gear.conf` manually because the JAVA_OPTS is missing.
