Data flow example that shows reading from a kafka queue and writing to HBase.
Assumes the following
- zookeeper is running and is configured in conf/pipeline.conf for both hbase and kafka
- kafka is running and is configured in conf/pipeline.conf
- hbase is running and is configured in conf/pipeline.conf
- kafka topic has been created and is set in pipeline.conf as hbase.zookeeper.connect
- hbase table has been created and is set in pipeline.conf as hbase.table.name

