---
layout: global
displayTitle: Frequently Asked Questions
title: faq
description: Gearpump Frequently Asked Questions
---

##### What's Relation between Gearpump and YARN?
Gearpump can run on top of YARN as a YARN application. Gearpump's ApplicationMaster provides the application management , deployment and scheduling of DAG's after arbitrating and receiving container resources from YARN

##### Relation with Storm and Spark Streaming
Storm and spark streaming are proven platforms, there are many production deployments. Compared with them, Gearpump is not than proven and there is no production deployment yet. However, there is no single platform that can cover every use case; Gearpump has its own +1 points in some special fields. As an instance, for IOT use cases, Gearpump may be considered convenient because the topology can be deployed to edge device with feature of location transparency. For another example, when users want to upgrade the application online without service interruption, Gearpump may be suitable as it can dynamically modify the computation DAG on the fly. 

##### What does GearPump mean?
The name GearPump is a reference the engineering term "Gear Pump", which is a super simple pump that consists of only two gears, but is very powerful at streaming water from left to right.

##### Why not using akka persistence to store the checkpoint file?
1. We only checkpoint file to disk when necessary.(not record level)
2. We have custom checkpoint file format

##### Have you considered the akka stream API for the high level DSL?
We are looking into a hands of candidate for what a good DSL should be. Akka stream API is one of the candidates.

##### Why wrapping the Task, instead of using the Actor interface directly?

1. It is more easy to conduct Unit test
2. We have custom logic and messages to ensure the data consistency, like flow control, like message loss detection.
3. As the Gearpump interface evolves rapidly. for now, we want to conservative in exposing more powerful functions so that we doesn't tie our hands for future refactory, it let us feel safe.

##### What is the open source plan for this project?
The ultimate goal is to make it an Apache project.
