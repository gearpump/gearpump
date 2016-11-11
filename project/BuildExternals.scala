/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import BuildGearpump._
import Dependencies._
import Docs._
import sbt._
import sbt.Keys._

object BuildExternals extends sbt.Build {

  lazy val externals: Seq[ProjectReference] = Seq(
    external_hbase,
    external_kafka,
    external_monoid,
    external_hadoopfs
  )

  lazy val external_kafka = Project(
    id = "gearpump-external-kafka",
    base = file("external/kafka"),
    settings = commonSettings ++ javadocSettings  ++
      Seq(
        libraryDependencies ++= Seq(
          "org.apache.kafka" %% "kafka" % kafkaVersion,
          "com.twitter" %% "bijection-core" % bijectionVersion,
          ("org.apache.kafka" %% "kafka" % kafkaVersion classifier ("test")) % "test"
        )
      ))
    .dependsOn(core % "provided", streaming % "test->test; provided")
    .disablePlugins(sbtassembly.AssemblyPlugin)

  lazy val external_hbase = Project(
    id = "gearpump-external-hbase",
    base = file("external/hbase"),
    settings = commonSettings ++ javadocSettings  ++
      Seq(
        libraryDependencies ++= Seq(
          "org.apache.hadoop" % "hadoop-common" % hadoopVersion % "provided",
          "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion % "provided",
          "org.apache.hadoop" % "hadoop-mapreduce-client-core" % hadoopVersion % "provided",
          "org.codehaus.jackson" % "jackson-core-asl" % "1.9.13" % "provided",
          "org.codehaus.jackson" % "jackson-mapper-asl" % "1.9.13" % "provided",
          "org.apache.hbase" % "hbase-client" % hbaseVersion
            exclude("com.github.stephenc.findbugs", "findbugs-annotations")
            exclude("com.google.guava", "guava")
            exclude("commons-codec", "commons-codec")
            exclude("commons-io", "commons-io")
            exclude("commons-lang", "commons-lang")
            exclude("commons-logging", "commons-logging")
            exclude("io.netty", "netty")
            exclude("junit", "junit")
            exclude("log4j", "log4j")
            exclude("org.apache.zookeeper", "zookeeper")
            exclude("org.codehaus.jackson", "jackson-mapper-asl"),
          "org.apache.hbase" % "hbase-common" % hbaseVersion
            exclude("com.github.stephenc.findbugs", "findbugs-annotations")
            exclude("com.google.guava", "guava")
            exclude("commons-codec", "commons-codec")
            exclude("commons-collections", "commons-collections")
            exclude("commons-io", "commons-io")
            exclude("commons-lang", "commons-lang")
            exclude("commons-logging", "commons-logging")
            exclude("junit", "junit")
            exclude("log4j", "log4j")
        )
      ))
    .dependsOn (core % "provided", streaming % "test->test; provided")
    .disablePlugins(sbtassembly.AssemblyPlugin)

  lazy val external_monoid = Project(
    id = "gearpump-external-monoid",
    base = file("external/monoid"),
    settings = commonSettings ++ javadocSettings  ++
      Seq(
        libraryDependencies ++= Seq(
          "com.twitter" %% "algebird-core" % algebirdVersion
        )
      ))
    .dependsOn (core % "provided", streaming % "provided")
    .disablePlugins(sbtassembly.AssemblyPlugin)

  lazy val external_serializer = Project(
    id = "gearpump-external-serializer",
    base = file("external/serializer"),
    settings = commonSettings ++ javadocSettings  ++
      Seq(
        libraryDependencies ++= Seq(
          "com.twitter" %% "chill-bijection" % chillVersion
            exclude("com.esotericsoftware.kryo", "kyro")
            exclude("com.esotericsoftware.minlog", "minlog")
        )
      ))
    .dependsOn (core % "provided", streaming % "provided")
    .disablePlugins(sbtassembly.AssemblyPlugin)

  lazy val external_hadoopfs = Project(
    id = "gearpump-external-hadoopfs",
    base = file("external/hadoopfs"),
    settings = commonSettings ++ javadocSettings ++
      Seq(
        libraryDependencies ++= Seq(
          "org.apache.hadoop" % "hadoop-common" % hadoopVersion % "provided",
          "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion % "provided"
        )
      ))
    .dependsOn(core % "provided", streaming % "test->test; provided")
    .disablePlugins(sbtassembly.AssemblyPlugin)
}