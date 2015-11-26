---
layout: global
displayTitle: Maven Setting
title: Maven Setting
description: Maven Setting
---

To programming against this version, you need to add below artifact dependencies to your application's Maven setting:

{% highlight xml %}
<dependencies>
<dependency>
<groupId>com.github.intel-hadoop</groupId>
<artifactId>gearpump-core_2.11</artifactId>
<version>{{ site.GEARPUMP_VERSION }}</version>
</dependency>
<dependency>
<groupId>com.github.intel-hadoop</groupId>
<artifactId>gearpump-streaming_2.11</artifactId>
<version>{{ site.GEARPUMP_VERSION }}</version>
</dependency>
</dependencies>
{% endhighlight %}

And you will need to add following repositories to get above dependencies resolved.

{% if site.GEARPUMP_VERSION contains "SNAPSHOT" %}
{% highlight xml %}
<repositories>
<repository>
  <id>sonatype-nexus-releases</id>
  <name>Sonatype Nexus Snapshots</name>
  <url>https://oss.sonatype.org/content/repositories/snapshots</url>

</repository>

<repository>
<id>releases-oss.sonatype.org</id>
<name>Sonatype Releases Repository</name>
<url>http://oss.sonatype.org/content/repositories/releases/</url>
</repository>

<repository>
    <id>akka-data-replication</id>
    <name>Patrik at Bintray</name>
    <url>http://dl.bintray.com/patriknw/maven</url>
</repository>

<repository>
    <id>cloudera</id>
    <name>Cloudera repo</name>
    <url>https://repository.cloudera.com/artifactory/cloudera-repos</url>
</repository>

<repository>
    <id>vincent</id>
    <name>vincent</name>
    <url>http://dl.bintray.com/fvunicorn/maven</url>
</repository>

<repository>
    <id>non</id>
    <name>non</name>
    <url>http://dl.bintray.com/non/maven</url>
</repository>

<repository>
    <id>maven-repo</id>
    <name>maven-repo</name>
    <url>http://repo.maven.apache.org/maven2</url>
</repository>

<repository>
    <id>maven1-repo</id>
    <name>maven1-repo</name>
    <url>http://repo1.maven.org/maven2</url>
</repository>

<repository>
    <id>maven2-repo</id>
    <name>maven2-repo</name>
    <url>http://mvnrepository.com/artifact</url>
</repository>

</repositories>
{% endhighlight %}
{% else %}
{% highlight xml %}
<repositories>
<repository>
<id>releases-oss.sonatype.org</id>
<name>Sonatype Releases Repository</name>
<url>http://oss.sonatype.org/content/repositories/releases/</url>
</repository>

<repository>
    <id>akka-data-replication</id>
    <name>Patrik at Bintray</name>
    <url>http://dl.bintray.com/patriknw/maven</url>
</repository>

<repository>
    <id>cloudera</id>
    <name>Cloudera repo</name>
    <url>https://repository.cloudera.com/artifactory/cloudera-repos</url>
</repository>

<repository>
    <id>vincent</id>
    <name>vincent</name>
    <url>http://dl.bintray.com/fvunicorn/maven</url>
</repository>

<repository>
    <id>non</id>
    <name>non</name>
    <url>http://dl.bintray.com/non/maven</url>
</repository>

<repository>
    <id>non</id>
    <name>non</name>
    <url>http://dl.bintray.com/non/maven</url>
</repository>

<repository>
    <id>maven-repo</id>
    <name>maven-repo</name>
    <url>http://repo.maven.apache.org/maven2</url>
</repository>

<repository>
    <id>maven1-repo</id>
    <name>maven1-repo</name>
    <url>http://repo1.maven.org/maven2</url>
</repository>

<repository>
    <id>maven2-repo</id>
    <name>maven2-repo</name>
    <url>http://mvnrepository.com/artifact</url>
</repository>

</repositories>
{% endhighlight %}
{% endif %}
