The experimental storm module aims to provide binary compatibility for Storm applications over Gearpump. That is to say, users could easily grab an existing Storm jar and run it 
on Gearpump. This documentation illustrates how to do so in a local Gearpump cluster.

## How to run a Storm application over Gearpump 

  1. launch a local cluster

  ```bash
    ./target/pack/bin/local

  ```

  2. submit a topology from storm-starter

  ```bash
    ./target/pack/bin/gear app -jar storm-starter-${STORM_VERSION}.jar org.apache.gearpump.experiments.storm.StormRunner -storm_topology storm.starter.ExclamationTopology -storm_args exclamation -storm_config storm.yaml
  
  ```
  
  Users are able to configure their applications through following options. 
   * `jar` for user to set the path of a storm application jar
   * `storm_topology` for user to set the topology main class 
   * `storm_args`, a comma separated arguments list for user to set command line options which would be passed into `args` parameter of the topology's main function.
   * `storm_config` where users could pass in a customized storm configuration file
  
  That's it. Check the dashboard and you should see data flowing from `StormProducer` to `StormProcessor`.

## Limitations 

Note that not all existing topologies from storm-starter is runnable on Gearpump. The following would not work.

1. Topologies submitted to Storm `LocalCluster` 
2. Topologies making use of Storm system feature, e.g. RollingTopWords which uses Storm's tick tuple


