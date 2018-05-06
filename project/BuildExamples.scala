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

import sbt.Keys._
import sbt._
import BuildGearpump._
import BuildExternals._
import Dependencies._
import sbtassembly.AssemblyPlugin.autoImport._

object BuildExamples extends sbt.Build {

  lazy val examples: Seq[ProjectReference] = Seq(
    complexdag,
    distributedshell,
    distributeservice,
    examples_kafka,
    examples_state,
    fsio,
    pagerank,
    sol,
    wordcount,
    wordcountJava,
    example_hbase,
    example_kudu
  )

  /**
   * The follow examples can be run in IDE or with `sbt run`
   */
  lazy val wordcountJava = Project(
    id = "gearpump-examples-wordcountjava",
    base = file("examples/streaming/wordcount-java"),
    settings = exampleSettings("org.apache.gearpump.streaming.examples.wordcountjava.WordCount") ++
      include("examples/streaming/wordcount-java")
  ).dependsOn(core, streaming % "compile; test->test")

  lazy val wordcount = Project(
    id = "gearpump-examples-wordcount",
    base = file("examples/streaming/wordcount"),
    settings = exampleSettings("org.apache.gearpump.streaming.examples.wordcount.dsl.WordCount") ++
      include("examples/streaming/wordcount")
  ).dependsOn(core, streaming % "compile; test->test")

  lazy val sol = Project(
    id = "gearpump-examples-sol",
    base = file("examples/streaming/sol"),
    settings = exampleSettings("org.apache.gearpump.streaming.examples.sol.SOL") ++
      include("examples/streaming/sol")
  ).dependsOn(core, streaming % "compile; test->test")

  lazy val complexdag = Project(
    id = "gearpump-examples-complexdag",
    base = file("examples/streaming/complexdag"),
    settings = exampleSettings("org.apache.gearpump.streaming.examples.complexdag.Dag") ++
      include("examples/streaming/complexdag")
  ).dependsOn(core, streaming % "compile; test->test")

  lazy val pagerank = Project(
    id = "gearpump-examples-pagerank",
    base = file("examples/pagerank"),
    settings =
      exampleSettings("org.apache.gearpump.experiments.pagerank.example.PageRankExample") ++
        include("examples/pagerank")
  ).dependsOn(core % "provided", streaming % "provided; test->test")

  /**
   * The following examples must be submitted to a deployed gearpump clutser
   */
  lazy val distributedshell = Project(
    id = "gearpump-examples-distributedshell",
    base = file("examples/distributedshell"),
    settings = commonSettings ++ noPublish ++ myAssemblySettings ++ Seq(
      mainClass in(Compile, packageBin) :=
        Some("org.apache.gearpump.examples.distributedshell.DistributedShell"),
      target in assembly := baseDirectory.value.getParentFile / "target" /
        CrossVersion.binaryScalaVersion(scalaVersion.value)
    )
  ).dependsOn(core % "provided; test->test")

  lazy val distributeservice = Project(
    id = "gearpump-examples-distributeservice",
    base = file("examples/distributeservice"),
    settings = commonSettings ++ noPublish ++ myAssemblySettings ++ Seq(
      mainClass in(Compile, packageBin) :=
        Some("org.apache.gearpump.experiments.distributeservice.DistributeService"),
      target in assembly := baseDirectory.value.getParentFile / "target" /
        CrossVersion.binaryScalaVersion(scalaVersion.value),
      libraryDependencies ++= Seq(
        "commons-httpclient" % "commons-httpclient" % commonsHttpVersion,
        "commons-lang" % "commons-lang" % commonsLangVersion,
        "commons-io" % "commons-io" % commonsIOVersion,
        "io.spray" %% "spray-can" % sprayVersion,
        "io.spray" %% "spray-routing-shapeless2" % sprayVersion
        ) ++ annotationDependencies
    ) ++ include("examples/distributeservice")
  ).dependsOn(core % "provided; test->test")

  lazy val example_hbase = Project(
    id = "gearpump-examples-hbase",
    base = file("examples/streaming/hbase"),
    settings = exampleSettings("org.apache.gearpump.streaming.examples.hbase.HBaseConn") ++
      Seq(
        libraryDependencies ++= Seq(
          "org.apache.hadoop" % "hadoop-common" % hadoopVersion % "provided"
        )
      )
  ) dependsOn(core % "provided", streaming % "provided; test->test", external_hbase)

  lazy val example_kudu = Project(
    id = "gearpump-examples-kudu",
    base = file("examples/streaming/kudu"),
    settings = exampleSettings("org.apache.gearpump.streaming.examples.kudu.KuduConn")
  ) dependsOn(core % "provided", streaming % "provided; test->test", external_kudu)

  lazy val fsio = Project(
    id = "gearpump-examples-fsio",
    base = file("examples/streaming/fsio"),
    settings = exampleSettings("org.apache.gearpump.streaming.examples.fsio.SequenceFileIO") ++
      Seq(
        libraryDependencies ++= Seq(
          "org.apache.hadoop" % "hadoop-common" % hadoopVersion % "provided"
        )
      )
  ).dependsOn(core % "provided", streaming % "provided; test->test")

  lazy val examples_kafka = Project(
    id = "gearpump-examples-kafka",
    base = file("examples/streaming/kafka"),
    settings =
      exampleSettings("org.apache.gearpump.streaming.examples.kafka.wordcount.KafkaWordCount")
  ).dependsOn(core % "provided", streaming % "provided; test->test", external_kafka)

  lazy val examples_state = Project(
    id = "gearpump-examples-state",
    base = file("examples/streaming/state"),
    settings = exampleSettings("org.apache.gearpump.streaming.examples.state.MessageCountApp") ++
      Seq(
        libraryDependencies ++= Seq(
          "org.apache.hadoop" % "hadoop-common" % hadoopVersion % "provided",
          "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion % "provided"
        )
      )
  ).dependsOn(core % "provided", streaming % "provided; test->test",
    external_hadoopfs, external_monoid, external_serializer, external_kafka)

  private def exampleSettings(className: String): Seq[Def.Setting[_]] =
    commonSettings ++ noPublish ++ myAssemblySettings ++ Seq(
      mainClass in(Compile, packageBin) :=
        Some(className),
      target in assembly := baseDirectory.value.getParentFile.getParentFile / "target" /
        CrossVersion.binaryScalaVersion(scalaVersion.value)
    )

  private def include(files: String*): Seq[Def.Setting[_]] =
    Seq(
      assemblyExcludedJars in assembly := {
        val cp = (fullClasspath in assembly).value
        cp.filterNot(p =>
          files.exists(p.data.getAbsolutePath.contains))
      }
    )
}
