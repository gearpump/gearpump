#!/bin/bash
#sbt "set offline := true" publishLocal pack
sbt publishLocal pack
~/hadoop/bin/yarn application -list 2>&1 | grep application_ | cut -f1 | xargs -n1 -t ~/hadoop/bin/yarn application -kill
~/hadoop/bin/hdfs dfs -put -f target/pack/lib/gearpump-experiments-yarn_2.11-0.3.0-rc2-SNAPSHOT.jar /user/gearpump/jars/
target/pack/bin/yarnclient
