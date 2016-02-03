---
layout: global
displayTitle: Maven Setting
title: Maven Setting
description: Maven Setting
---

To program against this version, you need to add below artifact dependencies to your application's Maven setting:

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

For SBT the configuration above, adding the two external modules as well, is equivalent to the following:

```
resolvers ++= Seq(
  "Sonatype releases" at "http://oss.sonatype.org/content/repositories/releases/",
  "Patrik at Bintray" at "http://dl.bintray.com/patriknw/maven",
  "Cloudera repo" at "https://repository.cloudera.com/artifactory/cloudera-repos",
  "vincent" at "http://dl.bintray.com/fvunicorn/maven",
  "non" at "http://dl.bintray.com/non/maven",
  "Maven repo" at "http://repo.maven.apache.org/maven2",
  "Maven1 repo" at "http://repo1.maven.org/maven2",
  "Maven2 repo" at "http://mvnrepository.com/artifact"
)

val gearpumpVersion = "{{ site.GEARPUMP_VERSION }}"

libraryDependencies ++= Seq(
  "com.github.intel-hadoop"                %  "gearpump-core_2.11"         % gearpumpVersion,
  "com.github.intel-hadoop"                %  "gearpump-streaming_2.11"    % gearpumpVersion,
  "com.github.intel-hadoop"                %% "gearpump-external-kafka"    % gearpumpVersion,
  "com.github.intel-hadoop"                %% "gearpump-external-hbase"    % gearpumpVersion
)
```

and you can put this in your `build.sbt` buildfile.