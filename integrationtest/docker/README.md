This folder contains docker images definitions used in integration tests of Gearpump.


These include:

 * [The standalone single node Kafka cluster with Zookeeper](/kafka)
 * [The Hadoop image](/hadoop)
 * [The Gearpump Cluster Launcher and Storm Client](/gearpump)
 * [Java 8 JRE image](/java)


We decided to fork spotify/kafka image, because the project does not maintain proper tagging. 
For now our tests focus on Kafka 0.8.x, and the project does not support this version. 

Hadoop docker image (https://hub.docker.com/r/sequenceiq/hadoop-docker/) is well maintained. 
We rely on version 2.6.0 and we feel there is no need to duplicate it.

Gearpump Cluster Launcher  helps developer to setup/test a local Gearpump cluster quickly. 
The image is based on a minimal JRE8 environment with Python support.

We used to base Gearpump Cluster Launcher on errordeveloper/oracle-jre image but it stuck on version 8u66-b17 and doesn't support tagging.
We also probably hit a Java bug (http://bugs.java.com/bugdatabase/view_bug.do?bug_id=8133205) that requires newer version of JRE.
This is experimental version of Java image that we will use until we switch to some "official" image.
