## How to launch a Gearpump cluster on YARN

1. Upload the `gearpump-{{SCALA_BINARY_VERSION}}-{{GEARPUMP_VERSION}}.zip` to remote HDFS Folder, suggest to put it under `/usr/lib/gearpump/gearpump-{{SCALA_BINARY_VERSION}}-{{GEARPUMP_VERSION}}.zip`

2. Make sure the home directory on HDFS is already created and all read-write rights are granted for user. For example, user gear's home directory is `/user/gear`

3. Put the YARN configurations under classpath.
  Before calling `yarnclient launch`, make sure you have put all yarn configuration files under classpath. Typically, you can just copy all files under `$HADOOP_HOME/etc/hadoop` from one of the YARN Cluster machine to `conf/yarnconf` of gearpump. `$HADOOP_HOME` points to the Hadoop installation directory. 

4. Launch the gearpump cluster on YARN

		:::bash
    	yarnclient launch -package /usr/lib/gearpump/gearpump-{{SCALA_BINARY_VERSION}}-{{GEARPUMP_VERSION}}.zip
    

    If you don't specify package path, it will read default package-path (`gearpump.yarn.client.package-path`) from `gear.conf`.

    **NOTE:** You may need to execute `chmod +x bin/*` in shell to make the script file `yarnclient` executable.
   
5. After launching, you can browser the Gearpump UI via YARN resource manager dashboard.

## How to configure the resource limitation of Gearpump cluster

Before launching a Gearpump cluster, please change configuration section `gearpump.yarn` in `gear.conf` to configure the resource limitation, like:

1. The number of worker containers. 
2. The YARN container memory size for worker and master.

## How to submit a application to Gearpump cluster.

To submit the jar to the Gearpump cluster, we first need to know the Master address, so we need to get
a active configuration file first.

There are two ways to get an active configuration file:

1. Option 1: specify "-output" option when you launch the cluster.

    	:::bash
    	yarnclient launch -package /usr/lib/gearpump/gearpump-{{SCALA_BINARY_VERSION}}-{{GEARPUMP_VERSION}}.zip -output /tmp/mycluster.conf
    

    It will return in console like this:
    
    	:::bash
    	==Application Id: application_1449802454214_0034
    
    

2. Option 2: Query the active configuration file

    	:::bash
    	yarnclient getconfig -appid <yarn application id> -output /tmp/mycluster.conf
    

    yarn application id can be found from the output of step1 or from YARN dashboard.

3. After you downloaded the configuration file, you can launch application with that config file.

    	:::bash
    	gear app -jar examples/wordcount-{{SCALA_BINARY_VERSION}}-{{GEARPUMP_VERSION}}.jar -conf /tmp/mycluster.conf
    
  
4. To run Storm application over Gearpump on YARN, please store the configuration file with `-output application.conf` 
   and then launch Storm application with

    	:::bash
    	storm -jar examples/storm-{{SCALA_BINARY_VERSION}}-{{GEARPUMP_VERSION}}.jar storm.starter.ExclamationTopology exclamation
    
  
5. Now the application is running. To check this:

    	:::bash
    	gear info -conf /tmp/mycluster.conf
    

6. To Start a UI server, please do:
    
    	:::bash
    	services -conf /tmp/mycluster.conf
    
    
    The default username and password is "admin:admin", you can check [UI Authentication](deployment-ui-authentication) to find how to manage users.

   
## How to add/remove machines dynamically.

Gearpump yarn tool allows to dynamically add/remove machines. Here is the steps:

1. First, query to get active resources.

    	:::bash
    	yarnclient query -appid <yarn application id>
    

    The console output will shows how many workers and masters there are. For example, I have output like this:

    	:::bash
    	masters:
    	container_1449802454214_0034_01_000002(IDHV22-01:35712)
    	workers:
    	container_1449802454214_0034_01_000003(IDHV22-01:35712)
    	container_1449802454214_0034_01_000006(IDHV22-01:35712)
   

2. To add a new worker machine, you can do:

    	:::bash
    	yarnclient addworker -appid <yarn application id> -count 2
    

    This will add two new workers machines. Run the command in first step to check whether the change is effective.

3. To remove old machines, use:

    	:::bash
    	yarnclient removeworker -appid <yarn application id> -container <worker container id>
    

    The worker container id can be found from the output of step 1. For example "container_1449802454214_0034_01_000006" is a good container id.

## Other usage:
 
1. To kill a cluster,

    	:::bash
    	yarnclient kill -appid <yarn application id>
    

    **NOTE:** If the application is not launched successfully, then this command won't work. Please use "yarn application -kill <appId>" instead.

2. To check the Gearpump version

    	:::bash
    	yarnclient version -appid <yarn application id>
    
