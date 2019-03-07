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

import BuildGearpump._
import sbt._
import sbt.Keys._
import xerial.sbt.pack.PackPlugin.autoImport._

object Pack {
  val daemonClassPath = Seq(
    "${PROG_HOME}/conf",
    // This is for DFSJarStore
    "${PROG_HOME}/hadoop/*"
  )

  val applicationClassPath = Seq(
    // Current working directory
    ".",
    "${PROG_HOME}/conf"
  )

  val serviceClassPath = Seq(
    "${PROG_HOME}/conf",
    "${PROG_HOME}/services/*",
    "${PROG_HOME}/dashboard"
  )

  lazy val packSettings = commonSettings ++ noPublish ++
    Seq(
      packMain := Map(
        "gear" -> "io.gearpump.cluster.main.Gear",
        "local" -> "io.gearpump.cluster.main.Local",
        "master" -> "io.gearpump.cluster.main.Master",
        "worker" -> "io.gearpump.cluster.main.Worker",
        "services" -> "io.gearpump.services.main.Services"
      ),
      packJvmOpts := Map(
        "gear" -> Seq(
          "-Djava.net.preferIPv4Stack=true",
          "-Dgearpump.home=${PROG_HOME}"),

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
          "-Djava.rmi.server.hostname=localhost")
      ),
      packExcludeLibJars := Seq(thisProjectRef.value.project),

      packGenerateMakefile := false,

      packResourceDir += (baseDirectory.value / ".." / "bin" -> "bin"),
      packResourceDir += (baseDirectory.value / ".." / "conf" -> "conf"),
      packResourceDir += (baseDirectory.value / ".." / "core" / "target" /
        CrossVersion.binaryScalaVersion(scalaVersion.value) -> "lib"),
      packResourceDir += (baseDirectory.value / ".." / "streaming" / "target" /
        CrossVersion.binaryScalaVersion(scalaVersion.value) -> "lib"),
      packResourceDir += (baseDirectory.value / ".." / "services" / "dashboard" -> "dashboard"),
      packResourceDir += (baseDirectory.value / ".." / "services" / "jvm" / "target" /
        CrossVersion.binaryScalaVersion(scalaVersion.value) -> "services"),
      packResourceDir += (baseDirectory.value / ".." / "gearpump-hadoop" / "target" /
        CrossVersion.binaryScalaVersion(scalaVersion.value) -> "hadoop"),
      packResourceDir += (baseDirectory.value / ".." / "examples" / "target" /
        CrossVersion.binaryScalaVersion(scalaVersion.value) -> "examples"),

      // The classpath should not be expanded. Otherwise, the classpath maybe too long.
      // On windows, it may report shell error "command line too long"
      packExpandedClasspath := false,
      packExtraClasspath := Map(
        "gear" -> applicationClassPath,
        "local" -> daemonClassPath,
        "master" -> daemonClassPath,
        "worker" -> applicationClassPath,
        "services" -> serviceClassPath
      ),

      packArchivePrefix := projectName + "-" + scalaBinaryVersion.value
    )
}
