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
import Build._
import xerial.sbt.Pack._

object Pack extends sbt.Build {
  val daemonClassPath = Seq(
    "${PROG_HOME}/conf",
    "${PROG_HOME}/lib/daemon/*",
    // This is for DFSJarStore
    "${PROG_HOME}/lib/yarn/*"
  )

  val applicationClassPath = Seq(
    // Current working directory
    ".",
    "${PROG_HOME}/conf"
  )

  val serviceClassPath = Seq(
    "${PROG_HOME}/conf",
    "${PROG_HOME}/lib/daemon/*",
    "${PROG_HOME}/lib/services/*",
    "${PROG_HOME}/dashboard"
  )

  val yarnClassPath = Seq(
    "${PROG_HOME}/conf",
    "${PROG_HOME}/lib/daemon/*",
    "${PROG_HOME}/lib/services/*",
    "${PROG_HOME}/lib/yarn/*",
    "${PROG_HOME}/conf/yarnconf",
    "/etc/hadoop/conf",
    "/etc/hbase/conf"
  )

  val stormClassPath = daemonClassPath ++ Seq(
    "${PROG_HOME}/lib/storm/*"
  )

  lazy val packProject = Project(
    id = "gearpump-pack",
    base = file(s"$distDirectory"),
    settings = commonSettings ++ noPublish ++
      packSettings ++
      Seq(
        packMain := Map(
          "gear" -> "org.apache.gearpump.cluster.main.Gear",
          "local" -> "org.apache.gearpump.cluster.main.Local",
          "master" -> "org.apache.gearpump.cluster.main.Master",
          "worker" -> "org.apache.gearpump.cluster.main.Worker",
          "services" -> "org.apache.gearpump.services.main.Services",
          "yarnclient" -> "org.apache.gearpump.experiments.yarn.client.Client",
          "storm" -> "org.apache.gearpump.experiments.storm.StormRunner"
        ),
        packJvmOpts := Map(
          "gear" -> Seq("-Djava.net.preferIPv4Stack=true", "-Dgearpump.home=${PROG_HOME}"),
          "local" -> Seq(
            "-server",
            "-Djava.net.preferIPv4Stack=true",
            "-DlogFilename=local",
            "-Dgearpump.home=${PROG_HOME}",
            "-Djava.rmi.server.hostname=localhost"),

          "master" -> Seq(
            "-server",
            "-Djava.net.preferIPv4Stack=true",
            "-DlogFilename=master",
            "-Dgearpump.home=${PROG_HOME}",
            "-Djava.rmi.server.hostname=localhost"),

          "worker" -> Seq(
            "-server",
            "-Djava.net.preferIPv4Stack=true",
            "-DlogFilename=worker",
            "-Dgearpump.home=${PROG_HOME}",
            "-Djava.rmi.server.hostname=localhost"),

          "services" -> Seq(
            "-server",
            "-Djava.net.preferIPv4Stack=true",
            "-Dgearpump.home=${PROG_HOME}",
            "-Djava.rmi.server.hostname=localhost"),

          "yarnclient" -> Seq(
            "-server",
            "-Djava.net.preferIPv4Stack=true",
            "-Dgearpump.home=${PROG_HOME}",
            "-Djava.rmi.server.hostname=localhost"),

          "storm" -> Seq(
            "-server",
            "-Djava.net.preferIPv4Stack=true",
            "-Dgearpump.home=${PROG_HOME}")
        ),
        packLibDir := Map(
          "lib" -> new ProjectsToPack(core.id, streaming.id),
          "lib/daemon" -> new ProjectsToPack(daemon.id, cgroup.id).exclude(core.id, streaming.id),
          "lib/yarn" -> new ProjectsToPack(yarn.id).exclude(services.id, daemon.id),
          "lib/services" -> new ProjectsToPack(services.id).exclude(daemon.id),
          "lib/storm" -> new ProjectsToPack(storm.id).exclude(streaming.id)
        ),
        packExclude := Seq(thisProjectRef.value.project),

        packResourceDir += (baseDirectory.value / ".." / "bin" -> "bin"),
        packResourceDir += (baseDirectory.value / ".." / "conf" -> "conf"),
        packResourceDir += (baseDirectory.value / ".." / "yarnconf" -> "conf/yarnconf"),
        packResourceDir += (baseDirectory.value / ".." / "shaded" / "target" /
          CrossVersion.binaryScalaVersion(scalaVersion.value) -> "lib"),
        packResourceDir += (baseDirectory.value / ".." / "services" / "dashboard" -> "dashboard"),
        packResourceDir += (baseDirectory.value / ".." / "examples" / "target" /
          CrossVersion.binaryScalaVersion(scalaVersion.value) -> "examples"),
        packResourceDir += (baseDirectory.value / ".." / "integrationtest" / "target" /
          CrossVersion.binaryScalaVersion(scalaVersion.value) -> "integrationtest"),

        // The classpath should not be expanded. Otherwise, the classpath maybe too long.
        // On windows, it may report shell error "command line too long"
        packExpandedClasspath := false,
        packExtraClasspath := Map(
          "gear" -> applicationClassPath,
          "local" -> daemonClassPath,
          "master" -> daemonClassPath,
          "worker" -> daemonClassPath,
          "services" -> serviceClassPath,
          "yarnclient" -> yarnClassPath,
          "storm" -> stormClassPath
        ),

        packArchivePrefix := projectName + "-" + scalaBinaryVersion.value,
        packArchiveExcludes := Seq("integrationtest")

      )
  ).dependsOn(core, streaming, services, yarn, storm).
    disablePlugins(sbtassembly.AssemblyPlugin)
}
