How to Start the Gearpump cluster on YARN
=======================================
1. Create HDFS folder /user/gearpump/, make sure all read-write rights are granted.
2. Upload the gearpump-${version}.tar.gz jars to HDFS folder: /user/gearpump/
3. Modify the config file ```conf/yarn.conf.template``` or create your own config file
4. Start the gearpump yarn cluster, for example 
  ``` bash
  bin/yarnclient -version gearpump-0.3.2-SNAPSHOT -config ${LOCAL_CONFIG_FILE}
  ```
5. On some machine in the cluster, start the UI server, and bind to the master port.
 ```bash
 bin/services -master ip:host
 ```

