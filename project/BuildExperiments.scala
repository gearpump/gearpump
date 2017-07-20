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
import BuildDashboard.services
import Dependencies._
import sbt._
import sbt.Keys._

object BuildExperiments extends sbt.Build {

  lazy val experiments: Seq[ProjectReference] = Seq(
    akkastream,
    cgroup,
    redis,
    storm,
    yarn,
    rabbitmq
  )

  lazy val yarn = Project(
    id = "gearpump-experiments-yarn",
    base = file("experiments/yarn"),
    settings = commonSettings ++ noPublish ++
      Seq(
        libraryDependencies ++= Seq(
          "org.apache.hadoop" % "hadoop-yarn-api" % hadoopVersion,
          "org.apache.hadoop" % "hadoop-yarn-client" % hadoopVersion,
          "org.apache.hadoop" % "hadoop-yarn-common" % hadoopVersion,
          "commons-httpclient" % "commons-httpclient" % commonsHttpVersion,
          "org.apache.hadoop" % "hadoop-mapreduce-client-core" % hadoopVersion,
          "org.apache.hadoop" % "hadoop-yarn-server-resourcemanager" % hadoopVersion % "provided",
          "org.apache.hadoop" % "hadoop-yarn-server-nodemanager" % hadoopVersion % "provided"
        ).map(_.exclude("org.slf4j", "slf4j-api"))
          .map(_.exclude("org.slf4j", "slf4j-log4j12"))
      ))
    .dependsOn(services % "test->test;compile->compile",
      core % "provided", gearpumpHadoop).disablePlugins(sbtassembly.AssemblyPlugin)

  lazy val akkastream = Project(
    id = "gearpump-experiments-akkastream",
    base = file("experiments/akkastream"),
    settings = commonSettings ++ noPublish ++ 
      Seq(
        libraryDependencies ++= Seq(
          "org.json4s" %% "json4s-jackson" % "3.2.11",
          "com.typesafe.akka" %% "akka-stream" % akkaVersion
        ),
        mainClass in(Compile, packageBin) := Some("akka.stream.gearpump.example.Test")
      ))
    .dependsOn (core % "provided", streaming % "test->test; provided")
    .disablePlugins(sbtassembly.AssemblyPlugin)

  lazy val redis = Project(
    id = "gearpump-experiments-redis",
    base = file("experiments/redis"),
    settings = commonSettings ++ noPublish ++
      Seq(
        libraryDependencies ++= Seq(
          "redis.clients" % "jedis" % jedisVersion
        )
      )
  ).dependsOn(core % "provided", streaming % "test->test; provided")

  lazy val storm = Project(
    id = "gearpump-experiments-storm",
    base = file("experiments/storm"),
    settings = commonSettings ++ noPublish ++
      Seq(
        libraryDependencies ++= Seq(
          "commons-io" % "commons-io" % commonsIOVersion,
          "org.apache.storm" % "storm-core" % stormVersion
            exclude("clj-stacktrace", "clj-stacktrace")
            exclude("ch.qos.logback", "logback-classic")
            exclude("ch.qos.logback", "logback-core")
            exclude("clj-time", "clj-time")
            exclude("clout", "clout")
            exclude("compojure", "compojure")
            exclude("hiccup", "hiccup")
            exclude("jline", "jline")
            exclude("joda-time", "joda-time")
            exclude("org.clojure", "core.incubator")
            exclude("org.clojure", "math.numeric-tower")
            exclude("org.clojure", "tools.logging")
            exclude("org.clojure", "tools.cli")
            exclude("org.clojure", "tools.macro")
            exclude("org.mortbay.jetty", "jetty-util")
            exclude("org.mortbay.jetty", "jetty")
            exclude("org.ow2.asm", "asm")
            exclude("org.slf4j", "log4j-over-slf4j")
            exclude("org.slf4j", "slf4j-api")
            exclude("org.apache.logging.log4j", "log4j-slf4j-impl")
            exclude("ring", "ring-core")
            exclude("ring", "ring-devel")
            exclude("ring", "ring-jetty-adapter")
            exclude("ring", "ring-servlet")
        )
      ))
    .dependsOn (core % "provided", streaming % "test->test; provided")
    .disablePlugins(sbtassembly.AssemblyPlugin)

  lazy val cgroup = Project(
    id = "gearpump-experimental-cgroup",
    base = file("experiments/cgroup"),
    settings = commonSettings ++ noPublish)
    .dependsOn (core % "provided")
    .disablePlugins(sbtassembly.AssemblyPlugin)

  lazy val rabbitmq = Project(
    id = "gearpump-experimentals-rabbitmq",
    base = file("experiments/rabbitmq"),
    settings = commonSettings ++ noPublish ++
      Seq(
        libraryDependencies ++= Seq(
          "com.rabbitmq" % "amqp-client" % rabbitmqVersion
        )
      ))
    .dependsOn(core % "provided", streaming % "test->test; provided")
    .disablePlugins(sbtassembly.AssemblyPlugin)
}
