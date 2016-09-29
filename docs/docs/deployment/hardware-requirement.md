### Pre-requisite

Gearpump cluster can be installed on Windows OS and Linux.

Before installation, you need to decide how many machines are used to run this cluster.

For each machine, the requirements are listed in table below.

**Table: Environment requirement on single machine**

Resource | Requirements
------------ | ---------------------------
Memory       | 2GB free memory is required to run the cluster. For any production system, 32GB memory is recommended.
Java	       | JRE 6 or above
User permission | Root permission is not required
Network	Ethernet |(TCP/IP)
CPU	| Nothing special
HDFS installation	| Default is not required. You only need to install it when you want to store the application jars in HDFS.
Kafka installation |	Default is not required. You need to install Kafka when you want the at-least once message delivery feature. Currently, the only supported data source for this feature is Kafka

**Table: The default port used in Gearpump:**

| usage	| Port |	Description |
------------ | ---------------|------------
  Dashboard UI	| 8090	| Web UI.
Dashboard web socket service |	8091 |	UI backend web socket service for long connection.
Master port |	3000 |	Every other role like worker, appmaster, executor, user use this port to communicate with Master.

You need to ensure that your firewall has not banned these ports to ensure Gearpump can work correctly.
And you can modify the port configuration. Check [Configuration](deployment-configuration) section for details.  
