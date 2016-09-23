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
import sbtassembly.AssemblyPlugin.autoImport._

object BuildShaded extends sbt.Build {

  val guavaVersion = "16.0.1"
  val codahaleVersion = "3.0.2"
  val kryoVersion = "0.4.1"
  val gsCollectionsVersion = "6.2.0"
  private val scalaVersionMajor = "2.11"

  val shadeAssemblySettings = Build.commonSettings ++ Seq(
    scalaVersion := Build.scalaVersionNumber,
    test in assembly := {},
    assemblyOption in assembly ~= {
      _.copy(includeScala = false)
    },
    assemblyJarName in assembly := {
      s"${name.value}-$scalaVersionMajor-${version.value}-assembly.jar"
    },
    target in assembly := baseDirectory.value.getParentFile / "target" / scalaVersionMajor
  )

  val shaded = Project(
    id = "gearpump-shaded",
    base = file("shaded")
  ).aggregate(shaded_akka_kryo, shaded_gs_collections, shaded_guava, shaded_metrics_graphite)
      .disablePlugins(sbtassembly.AssemblyPlugin)


  lazy val shaded_akka_kryo = Project(
    id = "gearpump-shaded-akka-kryo",
    base = file("shaded/akka-kryo"),
    settings = shadeAssemblySettings ++ addArtifact(Artifact("gearpump-shaded-akka-kryo",
      "assembly"), sbtassembly.AssemblyKeys.assembly) ++
        Seq(
          assemblyShadeRules in assembly := Seq(
            ShadeRule.zap("com.google.protobuf.**").inAll,
            ShadeRule.zap("com.typesafe.config.**").inAll,
            ShadeRule.zap("akka.**").inAll,
            ShadeRule.zap("org.jboss.netty.**").inAll,
            ShadeRule.zap("net.jpountz.lz4.**").inAll,
            ShadeRule.zap("org.uncommons.maths.**").inAll,
            ShadeRule.rename("com.romix.**" -> "org.apache.gearpump.romix.@1").inAll,
            ShadeRule.rename("com.esotericsoftware.**" ->
                "org.apache.gearpump.esotericsoftware.@1").inAll,
            ShadeRule.rename("org.objenesis.**" -> "org.apache.gearpump.objenesis.@1").inAll
          )
        ) ++
        Seq(
          libraryDependencies ++= Seq(
            "com.github.romix.akka" %% "akka-kryo-serialization" % kryoVersion
          )
        )
  )

  lazy val shaded_gs_collections = Project(
    id = "gearpump-shaded-gs-collections",
    base = file("shaded/gs-collections"),
    settings = shadeAssemblySettings ++ addArtifact(Artifact("gearpump-shaded-gs-collections",
      "assembly"), sbtassembly.AssemblyKeys.assembly) ++
        Seq(
          assemblyShadeRules in assembly := Seq(
            ShadeRule.rename("com.gs.collections.**" ->
                "org.apache.gearpump.gs.collections.@1").inAll
          )
        ) ++
        Seq(
          libraryDependencies ++= Seq(
            "com.goldmansachs" % "gs-collections" % gsCollectionsVersion
          )
        )
  )

  lazy val shaded_guava = Project(
    id = "gearpump-shaded-guava",
    base = file("shaded/guava"),
    settings = shadeAssemblySettings ++ addArtifact(Artifact("gearpump-shaded-guava",
      "assembly"), sbtassembly.AssemblyKeys.assembly) ++
        Seq(
          assemblyShadeRules in assembly := Seq(
            ShadeRule.rename("com.google.**" -> "org.apache.gearpump.google.@1").inAll
          )
        ) ++
        Seq(
          libraryDependencies ++= Seq(
            "com.google.guava" % "guava" % guavaVersion
          )
        )
  )

  lazy val shaded_metrics_graphite = Project(
    id = "gearpump-shaded-metrics-graphite",
    base = file("shaded/metrics-graphite"),
    settings = shadeAssemblySettings ++ addArtifact(Artifact("gearpump-shaded-metrics-graphite",
      "assembly"), sbtassembly.AssemblyKeys.assembly) ++
        Seq(
          assemblyShadeRules in assembly := Seq(
            ShadeRule.rename("com.codahale.metrics.**" ->
                "org.apache.gearpump.codahale.metrics.@1").inAll
          )
        ) ++
        Seq(
          libraryDependencies ++= Seq(
            "com.codahale.metrics" % "metrics-graphite" % codahaleVersion,
            "com.codahale.metrics" % "metrics-jvm" % codahaleVersion
          )
        )
  )

  def getShadedJarFile(name: String, gearpumpVersion: String): File = {
    shaded.base / "target" / scalaVersionMajor /
      s"gearpump-shaded-$name-$scalaVersionMajor-$gearpumpVersion-assembly.jar"
  }

}