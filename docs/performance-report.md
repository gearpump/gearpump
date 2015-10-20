---
layout: global
displayTitle: Gearpump Performance Report
title: Gearpump Performance Report
description: Gearpump Performance Report
---

# Performance Evaluation

To illustrate the performance of Gearpump, we mainly focused on two aspects, throughput and latency, using a micro benchmark called SOL (an example in the Gearpump package) whose topology is quite simple. SOLStreamProducer delivers messages to SOLStreamProcessor constantly and SOLStreamProcessor does nothing. We set up a 4-nodes cluster with 10GbE network and each node's hardware is briefly shown as follows:

Processor: 32 core Intel(R) Xeon(R) CPU E5-2680 2.70GHz
Memory: 128GB

## Throughput

Gearpump uses Graphite for the metrics dashboard. We tried to explore the upper bound of the throughput, after launching 64 SOLStreamProducer and 64 SOLStreamProcessor the Figure below shows that the whole throughput of the cluster can reach about 13 million messages/second(100 bytes per message)

Figure: Performance Evaluation, Throughput and Latency

## Latency

When we transfer message at the max throughput above, the average latency between two tasks is 17ms, standard deviation is 13ms.

Figure: Latency between Two tasks(ms)

## Fault Recovery time

When the corruption is detected, for example the Executor is down, Gearpump will reallocate the resource and restart the application. It takes about 10 seconds to recover the application.

## How to setup the benchmark environment?

### Prepare the env

1). Set up a node running Graphite, see guide doc/dashboard/README.md.

2). Set up a 4-nodes Gearpump cluster with 10GbE network which have 3 Workers on each node. In our test environment, each node has 128GB memory and Intel? Xeon? 32-core processor E5-2680 2.70GHz. Make sure the metrics is enabled in Gearpump.

3). Submit a SOL application with 32 SteamProducers and 32 StreamProcessors:

```bash
bin/gear app -jar ./examples/sol/target/pack/lib/gearpump-examples-$VERSION.jar io.gearpump.streaming.examples.sol.SOL -streamProducer 32 -streamProcessor 32 -runseconds 600
```

4). Browser http://$HOST:801/, you should see a Grafana dashboard. The HOST should be the node runs Graphite.

5). Copy the config file doc/dashboard/graphana_dashboard, and modify the `host` filed to the actual hosts which runs Gearpump and the `source` and `target` fields. Please note that the format of the value should exactly the same as existing format and you also need to manually add the rest task ID to the value of `All` under `source` and `target` filed since now the number of each task type is 32.

6). In the Grafana web page, click the "search" button and then import the config file mentioned above.

### Metrics

We use codahale metrics library. Gearpump support to use Graphite to visualize the metrics data. Metrics is disabled by default. To use it, you need to configure the 'conf/gear.conf'

```bash
  gearpump.metrics.reporter = graphite
  gearpump.metrics.enabled = true         ## Default is false, thus metrics is not enabled.
  gearpump.metrics.graphite.host = "your actual graphite host name or ip"  
  gearpump.metrics.graphite.port = 2003   ## Your graphite port
  gearpump.metrics.sample.rate = 10       ## this means we will sample 1 message for every 10 messages
```

For guide about how to install and configure Graphite, please check the Graphite website http://graphite.wikidot.com/.  For guide about how to use Grafana, please check guide in [doc/dashboard/readme.md](https://github.com/gearpump/gearpump/blob/master/doc/dashboard/README.md)

Here is how it looks like for grafana dashboard:

![](/img/dashboard.png)
