A minimalistic Oracle JDK 8 container on top of busybox.

We used to base Gearpump Cluster Launcher on errordeveloper/oracle-jre image but it stuck on version 8u66-b17 and doesn't support tagging.
We also probably hit a Java bug (http://bugs.java.com/bugdatabase/view_bug.do?bug_id=8133205) that requires newer version of JRE.
This is experimental version of Java image that we will use until we switch to some "official" image.

For now, the official JRE image is twice as big as the one we used:
openjdk                           8-jre-alpine      108 MB
openjdk                           8u121-jre         309 MB

delitescere/java                  1.8.0_92          67.8 MB
errordeveloper/oracle-jre         latest            161 MB


By using this container or any derived containers you are accepting the terms of [Oracle Binary Code License Agreement for the Java SE Platform Products and JavaFX][1].

  [1]: http://www.oracle.com/technetwork/java/javase/terms/license/index.html

