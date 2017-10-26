This folder contains docker images definitions used in integration tests of Gearpump.


These include:

 * [The standalone single node Kafka cluster with Zookeeper](kafka)
 * [The Hadoop image](hadoop)
 * [The Gearpump Cluster Launcher and Storm Client](gearpump)


We decided to fork spotify/kafka image, because the project does not maintain proper tagging. 
For now our tests focus on Kafka 0.8.x, and the project does not support this version. 

Hadoop docker image (https://hub.docker.com/r/sequenceiq/hadoop-docker/) is well maintained. 
We rely on version 2.6.0 and we feel there is no need to duplicate it.

Gearpump Cluster Launcher helps developer to setup/test a local Gearpump cluster quickly. 
The image is based on a minimal JRE8 environment with Python support.
