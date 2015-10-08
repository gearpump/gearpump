The experimental storm module aims to provide binary compatibility for Storm applications over Gearpump. That is to say, users could easily grab an existing Storm jar and run it 
on Gearpump. This documentation illustrates how to do so in a local Gearpump cluster.

## How to run a Storm application over Gearpump 

  1. launch a local cluster

  ```bash
    ./target/pack/bin/local

  ```

  2. submit a topology from storm-starter

  ```bash
    bin/storm -verbose -config storm.yaml -jar storm-starter-${STORM_VERSION}.jar storm.starter.ExclamationTopology exclamation 
  
  ```
  
  Users are able to configure their applications through following options. 
   * `jar` - set the path of a storm application jar
   * `config` - submit a customized storm configuration file
  
  That's it. Check the dashboard and you should see data flowing through your topology.

## Limitations 

1. Trident support is ongoing. 


