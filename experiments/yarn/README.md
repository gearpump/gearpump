How to Start the Gearpump cluster on YARN
=======================================
1. Create HDFS folder /user/gearpump/jars/, make sure all read-write rights are granted.
2. Upload the jars under lib/ to HDFS folder: /user/gearpump/jars/
3. Upload all file under conf/ to HDFS folder: /user/gearpump/jars/
4. Start the gearpump yarn cluster by running bin/yarnclient
5. On some machine in the cluster, start the UI server, and bind to the master port.

