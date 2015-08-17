The experimental storm module aims to provide binary compatibility for Storm applications over Gearpump. That is to say, users could easily grab an existing Storm jar and run it 
on Gearpump. This documentation illustrates how to do so in a local Gearpump cluster.

## How to run a Storm application over Gearpump 

  1. launch a local cluster

  ```bash
    ./target/pack/bin/local

  ```

  2. submit a topology from storm-starter

  ```bash
    bin/storm -jar storm-starter-${STORM_VERSION}.jar storm.starter.ExclamationTopology exclamation -config storm.yaml
  
  ```
  
  Users are able to configure their applications through following options. 
   * `jar` - set the path of a storm application jar
   * `config` - submit a customized configuration file
  
  That's it. Check the dashboard and you should see data flowing from `StormProducer` to `StormProcessor`.

## Limitations 

Note that not all existing topologies from storm-starter is runnable on Gearpump. The following would not work.

1. Topologies submitted to Storm `LocalCluster` 
2. Topologies making use of Storm system feature, e.g. RollingTopWords which uses Storm's tick tuple


