---
layout: global
displayTitle: Frequently Asked Questions
title: faq
description: Gearpump Frequently Asked Questions
---

##### What's Relation between Gearpump and YARN?
Gearpump can run on top of YARN as a YARN application. Gearpump's ApplicationMaster provides the application management, deployment and scheduling of DAGs after arbitrating and receiving container resources from YARN

##### Relation with Storm and Spark Streaming
Storm and spark streaming are proven platforms, and there are many production deployments. Compared with them, Gearpump is less proven and there is no production deployment yet. However, there is no single platform that can cover every use case; Gearpump has its own +1 points in some special fields. As an instance, for IoT use cases, Gearpump may be considered convenient because the topology can be deployed to edge device with feature of location transparency. For another example, when users want to upgrade the application online without service interruption, Gearpump may be suitable as it can dynamically modify the computation DAG on the fly.

##### What does Gearpump mean?
The name Gearpump is a reference to the engineering term "Gear Pump", which is a super simple pump that consists of only two gears, but is very powerful at streaming water from left to right.

##### Why not using akka persistence to store the checkpoint file?
1. We only checkpoint file to disk when necessary.(not record level)
2. We have custom checkpoint file format

##### Have you considered the akka stream API for the high level DSL?
We are looking into a hands of candidates for what a good DSL should be. Akka stream API is one of the candidates.

##### Why wrapping the Task, instead of using the Actor interface directly?

1. It is easier to conduct Unit test
2. We have custom logic and messages to ensure the data consistency, like flow control and message loss detection.
3. As the Gearpump interface evolves rapidly, for now, we want to be conservative in exposing more powerful functions so that we don't tie our hands for future refactoring. It lets us feel safe.

##### Why does my task has extremely high message latency (e.g. 10 seconds) ?

Please check whether you are doing blocking jobs (e.g. sleep, IO) in your task. By default, all tasks in an executor share a thread pool. The blocking tasks could use up all the threads while other tasks don't get a chance to run. In that case, you can set `gearpump.task-dispatcher` to `"gearpump.single-thread-dispatcher"` in `gear.conf` such that a unique thread is dedicated to each task.

Generally, we recommend use the default `share-thread-pool-dispatcher` which has better performance and only turn to the `single-thread-dispatcher` when you have to.

##### Why can't I open Dashboard even if the Services process has been launched successfully ?

By default, our Services process binds to a local **IPv6 port**. It's possible that another process on your system has already taken up the same **IPv4 port**. You may check by `lsof -i -P | grep -i "Listen"` if your system is Unix/Linux. 

