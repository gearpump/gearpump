To support HA, we allow to start master on multiple nodes. They will form a quorum to decide consistency. For example, if we start master on 5 nodes and 2 nodes are down, then the cluster is still consistent and functional.

Here are the steps to enable the HA mode:

### 1. Configure.

#### Select master machines

Distribute the package to all nodes. Modify `conf/gear.conf` on all nodes. You MUST configure

	:::bash
	gearpump.hostname

to make it point to your hostname(or ip), and

	:::bash
	gearpump.cluster.masters
	
to a list of master nodes. For example, if I have 3 master nodes (node1, node2, and node3),  then the `gearpump.cluster.masters` can be set as

	:::bash
	gearpump.cluster {
	  masters = ["node1:3000", "node2:3000", "node3:3000"]
	}
	

#### Configure distributed storage to store application jars.
In `conf/gear.conf`, For entry `gearpump.jarstore.rootpath`, please choose the storage folder for application jars. You need to make sure this jar storage is highly available. We support two storage systems:

  1). HDFS
  
  You need to configure the `gearpump.jarstore.rootpath` like this

	:::bash
    hdfs://host:port/path/


  For HDFS HA,
  
	:::bash
   hdfs://namespace/path/


  2). Shared NFS folder
  
  First you need to map the NFS directory to local directory(same path) on all machines of master nodes.
Then you need to set the `gearpump.jarstore.rootpath` like this:

	:::bash
  	file:///your_nfs_mapping_directory


  3). If you don't set this value, we will use the local directory of master node.
  NOTE! There is no HA guarantee in this case, which means we are unable to recover running applications when master goes down.

### 2. Start Daemon.

On node1, node2, node3, Start Master

	:::bash
  	## on node1
  	bin/master -ip node1 -port 3000

  	## on node2
  	bin/master -ip node2 -port 3000

  	## on node3
  	bin/master -ip node3 -port 3000
  

### 3. Done!

Now you have a highly available HA cluster. You can kill any node, the master HA will take effect.

**NOTE**: It can take up to 15 seconds for master node to fail-over. You can change the fail-over timeout time by adding config in `gear.conf` `gearpump-master.akka.cluster.auto-down-unreachable-after=10s` or set it to a smaller value
