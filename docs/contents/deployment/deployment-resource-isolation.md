CGroup (abbreviated from control groups) is a Linux kernel feature to limit, account, and isolate resource usage (CPU, memory, disk I/O, etc.) of process groups.In Gearpump, we use cgroup to manage CPU resources.

## Start CGroup Service 

CGroup feature is only supported by Linux whose kernel version is larger than 2.6.18. Please also make sure the SELinux is disabled before start CGroup.

The following steps are supposed to be executed by root user.

1. Check `/etc/cgconfig.conf` exist or not. If not exists, please `yum install libcgroup`.

2. Run following command to see whether the **cpu** subsystem is already mounted to the file system.
 
		:::bash
   		lssubsys -m
   		
    Each subsystem in CGroup will have a corresponding mount file path in local file system. For example, the following output shows that **cpu** subsystem is mounted to file path `/sys/fs/cgroup/cpu`
   
		:::bash
		cpu /sys/fs/cgroup/cpu
		net_cls /sys/fs/cgroup/net_cls
		blkio /sys/fs/cgroup/blkio
		perf_event /sys/fs/cgroup/perf_event
  
   
3. If you want to assign permission to user **gear** to launch Gearpump Worker and applications with resource isolation enabled, you need to check gear's uid and gid in `/etc/passwd` file, let's take **500** for example.

4. Add following content to `/etc/cgconfig.conf`
    
		
		# The mount point of cpu subsystem.
		# If your system already mounted it, this segment should be eliminated.
		mount {    
		  cpu = /cgroup/cpu;
		}
		    
		# Here the group name "gearpump" represents a node in CGroup's hierarchy tree.
		# When the CGroup service is started, there will be a folder generated under the mount point of cpu subsystem,
		# whose name is "gearpump".
		    
		group gearpump {
		   perm {
		       task {
		           uid = 500;
		           gid = 500;
		        }
		       admin {
		           uid = 500;
		           gid = 500;
		       }
		   }
		   cpu {
		   }
		}
	   
   
   Please note that if the output of step 2 shows that **cpu** subsystem is already mounted, then the `mount` segment should not be included.
   
4. Then Start cgroup service
   
		:::bash
   		sudo service cgconfig restart 
   
   
5. There should be a folder **gearpump** generated under the mount point of cpu subsystem and its owner is **gear:gear**.  
  
6. Repeat the above-mentioned steps on each machine where you want to launch Gearpump.   

## Enable Cgroups in Gearpump 
1. Login into the machine which has CGroup prepared with user **gear**.

   		:::bash
   		ssh gear@node
   

2. Enter into Gearpump's home folder, edit gear.conf under folder `${GEARPUMP_HOME}/conf/`

   		:::bash
   		gearpump.worker.executor-process-launcher = "io.gearpump.cluster.worker.CGroupProcessLauncher"
   
   		gearpump.cgroup.root = "gearpump"
   

   Please note the gearpump.cgroup.root **gearpump** must be consistent with the group name in /etc/cgconfig.conf.

3. Repeat the above-mentioned steps on each machine where you want to launch Gearpump

4. Start the Gearpump cluster, please refer to [Deploy Gearpump in Standalone Mode](deployment-standalone)

## Launch Application From Command Line
1. Login into the machine which has Gearpump distribution.

2. Enter into Gearpump's home folder, edit gear.conf under folder `${GEARPUMP_HOME}/conf/`
   
   		:::bash
   		gearpump.cgroup.cpu-core-limit-per-executor = ${your_preferred_int_num}
   
  
   Here the configuration is the number of CPU cores per executor can use and -1 means no limitation

3. Submit application

   		:::bash
   		bin/gear app -jar examples/sol-{{SCALA_BINARY_VERSION}}-{{GEARPUMP_VERSION}}-assembly.jar -streamProducer 10 -streamProcessor 10 
   

4. Then you can run command `top` to monitor the cpu usage.

## Launch Application From Dashboard
If you want to submit the application from dashboard, by default the `gearpump.cgroup.cpu-core-limit-per-executor` is inherited from Worker's configuration. You can provide your own conf file to override it.

## Limitations
Windows and Mac OS X don't support CGroup, so the resource isolation will not work even if you turn it on. There will not be any limitation for single executor's cpu usage.
