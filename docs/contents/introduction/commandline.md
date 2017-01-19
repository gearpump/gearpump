The commands can be found at: "bin" folder of Gearpump binary.

**NOTE:** on MS Windows platform, please use window shell gear.bat script instead. bash script doesn't work well in cygwin/mingw.

### Creating an uber-jar

If you use Maven you can have a look [here](https://maven.apache.org/plugins/maven-shade-plugin/) whereas SBT users may find [this](https://github.com/sbt/sbt-assembly) useful.

### Submit an new application

You can use the command `gear` under the bin directory to submit, query and terminate an application:

	:::bash
	gear app [-namePrefix <application name prefix>] [-executors <number of executors to launch>] [-conf <custom gearpump config file>] -jar xx.jar MainClass <arg1> <arg2> ...
	

### List all running applications
To list all running applications:

	:::bash
	gear info  [-conf <custom gearpump config file>]


### Kill a running application
To kill an application:

	:::bash
	gear kill -appid <application id>  [-conf <custom gearpump config file>]


### Submit a storm application to Gearpump Cluster
For example, to submit a storm application jar:

	:::bash
	storm -verbose -config storm.yaml -jar storm-starter-${STORM_VERSION}.jar storm.starter.ExclamationTopology exclamation
	

[Storm Compatibility Guide](../dev/dev-storm)

### Start Gearpump Cluster on YARN
To start a Gearpump Cluster on YARN, you can:

	:::bash
	yarnclient launch -package /usr/lib/gearpump/gearpump-{{SCALA_BINARY_VERSION}}-{{GEARPUMP_VERSION}}.zip

`/usr/lib/gearpump/gearpump-{{SCALA_BINARY_VERSION}}-{{GEARPUMP_VERSION}}.zip` should be available on HDFS.

Please check [YARN Deployment Guide](../deployment/deployment-yarn) for more information.

### Start a local cluster
Masters and workers will be started in one machine:

	:::bash
	local
	

Check [Deployment Guide for Local Cluster](../deployment/deployment-local) for more information.

### Start master daemons

	:::bash
	master -ip <Ip address> -port <port where this master is hooking>


Please check [Deployment for Standalone mode](../deployment/deployment-standalone) for more information.

### Start worker daemons

	:::bash
	worker


Please check [Deployment for Standalone mode](../deployment/deployment-standalone) for more information.

### Start UI server

To start UI server, you can:

	:::bash
	services  [-master <host:port>]


The default username and password is "admin:admin", you can check
[UI Authentication](../deployment/deployment-ui-authentication) to find how to manage users.