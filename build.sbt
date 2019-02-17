/*
 * Licensed under the Apache License, Version 2.0 (the
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

import BuildDashboard._
import BuildExamples._
import BuildGearpump._
import Dependencies._
import Docs._
import Pack.packSettings
import sbtunidoc.GenJavadocPlugin
import sbtunidoc.JavaUnidocPlugin
import sbtunidoc.ScalaUnidocPlugin

lazy val aggregated: Seq[ProjectReference] = Seq[ProjectReference](
  core,
  streaming,
  services,
  gearpumpHadoop,
  packProject,
  complexdag,
  distributedshell,
  pagerank,
  sol,
  wordcount,
  wordcountJava
)

lazy val packProject = Project(
  id = "gearpump-pack",
  base = file("output"))
  .settings(packSettings)
  .enablePlugins(PackPlugin)
  .disablePlugins(sbtassembly.AssemblyPlugin)

lazy val root = Project(
  id = "gearpump",
  base = file("."))
  .settings(commonSettings ++ noPublish ++ gearpumpUnidocSetting(core, streaming))
  .aggregate(aggregated: _*)
  .enablePlugins(ScalaUnidocPlugin)
  .enablePlugins(JavaUnidocPlugin)
  .disablePlugins(sbtassembly.AssemblyPlugin)

lazy val core = Project(
  id = "gearpump-core",
  base = file("core"))
  .settings(commonSettings ++ myAssemblySettings ++ javadocSettings ++ coreDependencies ++
    addArtifact(artifact in (Compile, assembly), assembly) ++
    Seq(
      assemblyOption in assembly ~= {
        _.copy(includeScala = true)
      },

      artifact in (Compile, assembly) := {
        val art = (artifact in (Compile, assembly)).value
        art.withClassifier(Some("assembly"))
      },

      pomPostProcess := {
        (node: xml.Node) => changeShadedDeps(
          Set(
            "com.github.romix.akka",
            "com.google.guava",
            "com.codahale.metrics",
            "org.scoverage"
          ), List.empty[xml.Node], node)
      }
    ))
  .enablePlugins(GenJavadocPlugin)

lazy val streaming = Project(
  id = "gearpump-streaming",
  base = file("streaming"))
  .settings(commonSettings ++ myAssemblySettings ++ javadocSettings ++
    addArtifact(artifact in (Compile, assembly), assembly) ++
    Seq(
      assemblyMergeStrategy in assembly := {
        case "geardefault.conf" =>
          MergeStrategy.last
        case x =>
          val oldStrategy = (assemblyMergeStrategy in assembly).value
          oldStrategy(x)
      },

      artifact in (Compile, assembly) := {
        val art = (artifact in (Compile, assembly)).value
        art.withClassifier(Some("assembly"))
      },

      libraryDependencies ++= Seq(
        "com.goldmansachs" % "gs-collections" % gsCollectionsVersion
      ) ++ annotationDependencies ++ compilerDependencies,

      pomPostProcess := {
        (node: xml.Node) => changeShadedDeps(
          Set(
            "com.goldmansachs",
            "org.scala-lang",
            "org.scoverage"
          ),
          List(
            getShadedDepXML(organization.value, s"${core.id}_${scalaBinaryVersion.value}",
              version.value, "provided")),
          node)
      }
    ))
  .dependsOn(core % "test->test;provided")
  .enablePlugins(GenJavadocPlugin)

lazy val gearpumpHadoop = Project(
  id = "gearpump-hadoop",
  base = file("gearpump-hadoop"))
  .settings(commonSettings ++ noPublish ++ myAssemblySettings ++
    Seq(
      libraryDependencies ++= Seq(
        "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion,
        "org.apache.hadoop" % "hadoop-common" % hadoopVersion
      ).map(_.exclude("org.slf4j", "slf4j-api"))
        .map(_.exclude("org.slf4j", "slf4j-log4j12"))
    ))
  .dependsOn(core % "provided")

lazy val services: Project = Project(
  id = "gearpump-services",
  base = file("services/jvm"))
  .settings(serviceJvmSettings: _*)
  .settings(
    Seq(
      libraryDependencies ++= compilerDependencies
    )
  )
  .dependsOn(core % "provided", streaming % "test->test; provided")
  .enablePlugins(GenJavadocPlugin)

/**
 * The follow examples can be run in IDE or with `sbt run`
 */
lazy val wordcountJava = Project(
  id = "gearpump-examples-wordcountjava",
  base = file("examples/streaming/wordcount-java"))
  .settings(exampleSettings("io.gearpump.streaming.examples.wordcountjava.WordCount"))
  .dependsOn(core, streaming % "compile; test->test")

lazy val wordcount = Project(
  id = "gearpump-examples-wordcount",
  base = file("examples/streaming/wordcount"))
  .settings(exampleSettings("io.gearpump.streaming.examples.wordcount.dsl.WordCount"))
  .dependsOn(core, streaming % "compile; test->test")

lazy val sol = Project(
  id = "gearpump-examples-sol",
  base = file("examples/streaming/sol"))
  .settings(exampleSettings("io.gearpump.streaming.examples.sol.SOL"))
  .dependsOn(core, streaming % "compile; test->test")

lazy val complexdag = Project(
  id = "gearpump-examples-complexdag",
  base = file("examples/streaming/complexdag"))
  .settings(exampleSettings("io.gearpump.streaming.examples.complexdag.Dag"))
  .dependsOn(core, streaming % "compile; test->test")

lazy val pagerank = Project(
  id = "gearpump-examples-pagerank",
  base = file("examples/pagerank"))
  .settings(exampleSettings("io.gearpump.experiments.pagerank.example.PageRankExample"))
  .dependsOn(core, streaming % "compile; test->test")

/**
 * The following examples must be submitted to a deployed gearpump clutser
 */
lazy val distributedshell = Project(
  id = "gearpump-examples-distributedshell",
  base = file("examples/distributedshell"))
  .settings(exampleSettings("io.gearpump.examples.distributedshell.DistributedShell"))
  .dependsOn(core % "compile; test->test")


