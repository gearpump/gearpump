Data flow example that shows reading from a kafka queue and writing to HDFS via a parquet file
Assumes the following
- zookeeper is running and passed to the application as -zookeeper <zookeeper1:2181,zookeeper2:2181>
- topic exists and is passed to the application as -topic <topic>

