---
layout: global
title: Storm Compatibility
---

Gearpump provides **binary compatibility** for Apache Storm applications. That is to say, users could easily grab an existing Storm jar and run it 
on Gearpump. This documentation illustrates Gearpump's comapatibility with Storm.  

## How to run a Storm application on Gearpump

This section shows how to run an existing Storm jar in a local Gearpump cluster.

1. launch a local cluster
  
   ```
   ./target/pack/bin/local
   ```

2. submit a topology from storm-starter. 
   ```
   bin/storm -verbose -config storm.yaml -jar storm-starter-${STORM_VERSION}.jar storm.starter.ExclamationTopology exclamation 
   ```
  
   Users are able to configure their applications through following options
   
     * `jar` - set the path of a storm application jar
     * `config` - submit a customized storm configuration file     
  
   That's it. Check the dashboard and you should see data flowing through your topology. 
   
   *Note that submission from UI is not supported yet*. 

  
## How is it different from running on Storm

### Topology submission

When a client submits a Storm topology, Gearpump launches locally a simplified version of Storm's  Nimbus server `GearpumpNimbus`. `GearpumpNimbus` then translates topology to a directed acyclic graph (DAG) of Gearpump, which is submitted to Gearpump master and deployed as a Gearpump application. 

![storm_gearpump_cluster](img/storm_gearpump_cluster.png)

`GearpumpNimbus` supports the following methods
  
* `submitTopology` / `submitTopologyWithOpts`
* `killTopology` / `killTopologyWithOpts`
* `getTopology` / `getUserTopology`
* `getClusterInfo`

### Topology translation

Here's an example of `WordCountTopology` with acker bolts (ackers) being translated into a Gearpump DAG.

![storm_gearpump_dag](img/storm_gearpump_dag.png)

Gearpump creates a `StormProducer` for each Storm spout and a `StormProcessor` for each Storm bolt (except for ackers) with the same parallelism, and wires them together using the same grouping strategy (partitioning in Gearpump) as in Storm. 

At runtime, spouts and bolts are running inside `StormProducer` tasks and `StormProcessor` tasks respectively. Messages emitted by spout are passed to `StormProducer`, transferred to `StormProcessor` and passed down to bolt.  Messages are serialized / deserialized with Storm serializers.

Storm ackers are dropped since Gearpump has a different mechanism of message tracking and flow control. 

### Task execution

Each Storm task is executed by a dedicated thread while all Gearpump tasks of an executor share a thread pool. Generally, we can achieve better performance with a shared thread pool. It's possible, however, some tasks block and take up all the threads. In that case, we can 
fall back to the Storm way by setting `gearpump.task-dispatcher` to `"gaerpump.single-thread-dispatcher"` in `gear.conf`.

### Message tracking 

Storm tracks the lineage of each message with ackers to guarantee at-least-once message delivery. Failed messages are re-sent from spout.

Gearpump [tracks messages between a sender and receiver in an efficient way](gearpump-internals.html#how-do-we-detect-message-loss). Message loss causes the whole application to replay from the [minimum timestamp of all pending messages in the system](gearpump-internals.html#application-clock-and-global-clock-service). 

*Note that ack from bolt is a no-op while fail throws an exception.*

### Flow control

Storm throttles flow rate at spout, which stops sending messages if the number of unacked messages exceeds `topology.max.spout.pending`. 

Gearpump has flow control between tasks such that [sender cannot flood receiver](gearpump-internals.html#how-do-we-do-flow-control), which is backpressured till the source.

### Configurations

All Storm configurations are respected with the following priority order 

```
defaults.yaml < storm.yaml < application config < component config < custom user config
```

where

* application config is submit from Storm application along with the topology 
* component config is set in spout / bolt with `getComponentConfiguration`
* custom user config is specified with the `-config` option when submitting Storm application from command line

## Limitations

1. Trident support is ongoing.