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

import com.typesafe.sbt.SbtPgp.autoImport._
import BuildExamples.examples
import BuildExperiments.experiments
import BuildExternals.externals
import BuildIntegrationTests.integrationTests
import BuildDashboard.services
import Dependencies._
import Docs._
import de.johoop.jacoco4sbt.JacocoPlugin.jacoco
import sbt.Keys._
import sbt._
import Pack.packProject
import sbtassembly.AssemblyPlugin.autoImport._

import xerial.sbt.Sonatype._

object BuildGearpump extends sbt.Build {

  val apacheRepo = "https://repository.apache.org/"
  val distDirectory = "output"
  val projectName = "gearpump"

  val commonSettings = Seq(jacoco.settings: _*) ++ sonatypeSettings ++
    Seq(
      resolvers ++= Seq(
        // https://repo1.maven.org/maven2 has been added by default
        "apache-repo" at "https://repository.apache.org/content/repositories",
        Resolver.sonatypeRepo("releases"),
        "clojars" at "http://clojars.org/repo"
      )
    ) ++
    Seq(
      scalaVersion := scalaVersionNumber,
      crossScalaVersions := crossScalaVersionNumbers,
      organization := "org.apache.gearpump",
      useGpg := false,
      pgpSecretRing := file("./secring.asc"),
      pgpPublicRing := file("./pubring.asc"),
      scalacOptions ++= Seq("-Yclosure-elim", "-Yinline"),
      publishMavenStyle := true,

      pgpPassphrase := Option(System.getenv().get("PASSPHRASE")).map(_.toArray),
      credentials += Credentials(
        "Sonatype Nexus Repository Manager",
        "repository.apache.org",
        System.getenv().get("SONATYPE_USERNAME"),
        System.getenv().get("SONATYPE_PASSWORD")),

      pomIncludeRepository := { _ => false },

      publishTo := {
        if (isSnapshot.value) {
          Some("snapshots" at apacheRepo + "content/repositories/snapshots")
        } else {
          Some("releases" at apacheRepo + "content/repositories/releases")
        }
      },

      publishArtifact in Test := true,

      pomExtra := {
        <url>https://github.com/apache/incubator-gearpump</url>
          <licenses>
            <license>
              <name>Apache 2</name>
              <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
            </license>
          </licenses>
          <scm>
            <connection>scm:git://git.apache.org/incubator-gearpump.git</connection>
            <developerConnection>scm:git:git@github.com:apache/incubator-gearpump</developerConnection>
            <url>github.com/apache/incubator-gearpump</url>
          </scm>
          <developers>
            <developer>
              <id>gearpump</id>
              <name>Gearpump Team</name>
              <url>http://gearpump.incubator.apache.org/community.html#who-we-are</url>
            </developer>
          </developers>
      },

      pomPostProcess := {
        (node: xml.Node) => changeShadedDeps(
          Set(
            "org.scoverage",
            "org.scala-lang"
          ),
          List.empty[xml.Node],
          node)
      },

      cleanFiles += (baseDirectory.value / "examples" / "target")
    )

  val noPublish = Seq(
    publish := {},
    publishLocal := {},
    publishArtifact := false,
    publishArtifact in Test := false
  )

  lazy val myAssemblySettings = Seq(
    test in assembly := {},
    assemblyOption in assembly ~= {
      _.copy(includeScala = false)
    },
    assemblyJarName in assembly := {
      s"${name.value}_${scalaBinaryVersion.value}-${version.value}.jar"
    },
    assemblyShadeRules in assembly := Seq(
      ShadeRule.rename("com.romix.**" -> "org.apache.gearpump.@0").inAll,
      ShadeRule.rename("com.esotericsoftware.**" ->
        "org.apache.gearpump.@0").inAll,
      ShadeRule.rename("org.objenesis.**" -> "org.apache.gearpump.@0").inAll,
      ShadeRule.rename("com.google.common.**" -> "org.apache.gearpump.@0").inAll,
      ShadeRule.rename("com.google.thirdparty.**" -> "org.apache.gearpump.@0").inAll,
      ShadeRule.rename("com.codahale.metrics.**" ->
        "org.apache.gearpump.@0").inAll,
      ShadeRule.rename("com.gs.collections.**" ->
        "org.apache.gearpump.gs.collections.@0").inAll
    ),
    target in assembly := baseDirectory.value / "target" / scalaBinaryVersion.value
  )

  lazy val aggregated: Seq[ProjectReference] = Seq[ProjectReference](
    core,
    streaming,
    services,
    gearpumpHadoop,
    packProject
  ) ++ examples ++ experiments ++ externals ++ integrationTests

  lazy val root = Project(
    id = "gearpump",
    base = file("."),
    settings = commonSettings ++ noPublish ++ gearpumpUnidocSetting,
    aggregate = aggregated)
    .settings(Defaults.itSettings: _*)
    .disablePlugins(sbtassembly.AssemblyPlugin)

  lazy val core = Project(
    id = "gearpump-core",
    base = file("core"),
    settings = commonSettings ++ myAssemblySettings ++ javadocSettings ++ coreDependencies ++
      addArtifact(Artifact("gearpump-core"), sbtassembly.AssemblyKeys.assembly) ++ Seq(

      assemblyOption in assembly ~= {
        _.copy(includeScala = true)
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

  lazy val streaming = Project(
    id = "gearpump-streaming",
    base = file("streaming"),
    settings = commonSettings ++ myAssemblySettings ++ javadocSettings ++
      addArtifact(Artifact("gearpump-streaming"), sbtassembly.AssemblyKeys.assembly) ++
      Seq(
        assemblyMergeStrategy in assembly := {
          case "geardefault.conf" =>
            MergeStrategy.last
          case x =>
            val oldStrategy = (assemblyMergeStrategy in assembly).value
            oldStrategy(x)
        },

        libraryDependencies ++= Seq(
          "com.goldmansachs" % "gs-collections" % gsCollectionsVersion
        ),

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
      )
  ).dependsOn(core % "test->test;provided")

  lazy val gearpumpHadoop = Project(
    id = "gearpump-hadoop",
    base = file("gearpump-hadoop"),
    settings = commonSettings ++ noPublish ++
      Seq(
        libraryDependencies ++= Seq(
          "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion,
          "org.apache.hadoop" % "hadoop-common" % hadoopVersion
        ).map(_.exclude("org.slf4j", "slf4j-api"))
          .map(_.exclude("org.slf4j", "slf4j-log4j12"))
      )
  ).dependsOn(core % "provided").disablePlugins(sbtassembly.AssemblyPlugin)

  private def changeShadedDeps(toExclude: Set[String], toInclude: List[xml.Node],
      node: xml.Node): xml.Node = {
    node match {
      case elem: xml.Elem =>
        val child =
          if (elem.label == "dependencies") {
            elem.child.filterNot { dep =>
              dep.child.find(_.label == "groupId").exists(gid => toExclude.contains(gid.text))
            } ++ toInclude
          } else {
            elem.child.map(changeShadedDeps(toExclude, toInclude, _))
          }
        xml.Elem(elem.prefix, elem.label, elem.attributes, elem.scope, false, child: _*)
      case _ =>
        node
    }
  }

  private def getShadedDepXML(groupId: String, artifactId: String,
      version: String, scope: String): scala.xml.Node = {
    <dependency>
      <groupId>{groupId}</groupId>
      <artifactId>{artifactId}</artifactId>
      <version>{version}</version>
      <scope>{scope}</scope>
    </dependency>
  }
}
