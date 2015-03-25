The experimental storm module aims to provide binary compatibility for Storm applications over Gearpump. That is to say, users could easily grab an existing Storm jar and run it 
on Gearpump. This documentation illustrates how to do so in a local Gearpump cluster.

## How to run a Storm application over Gearpump 

  1. add default serializer for `StormTuple` in `gear.conf`

  ```
      serializers {
  #      ## Follow this format when adding new serializer for new message types
  #      ##    "org.apache.gearpump.Message" = "org.apache.gearpump.streaming.MessageSerializer"
        "org.apache.gearpump.experiments.storm.util.StormTuple" = ""
    }
  ```

  `StormTuple` is a simplified version of `Tuple` in Storm and carries `Spout` and `Bolt` outputs across Gearpump cluster.
  
  2. submit a Storm topology from storm-starter
  
  ```bash
    ./target/pack/bin/local -port 3000 
    ./target/pack/bin/gear app -jar examples/target/scala-2.11/gearpump-examples-assembly-$VERSION.jar org.apache.gearpump.experiments.storm.StormRunner -storm_topology storm.starter.WordCountTopology -storm_args wordcount -master 127.0.0.1:3000
  
  ```
  
  There are two options user could configure. 
   * `storm_topology` for user to set the topology to run;
   * `storm_args`, a comma separated arguments list for user to set command line options which would be passed into `args` parameter of the topology's main function.
  
  That's it. Check the dashboard and you should see data flowing from `StormProducer` to `StormProcessor`.

## Limitations 

Note that not all existing topologies from storm-starter is runnable on Gearpump. The following would not work.

1. Topologies submitted to Storm `LocalCluster` 
2. Topologies making use of Storm system feature, e.g. RollingTopWords which uses Storm's tick tuple


