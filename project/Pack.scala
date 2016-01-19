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

  val applicationClassPath = daemonClassPath ++ Seq(
    // current working directory
    "."
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
    settings = commonSettings ++ noPublish  ++
        packSettings ++
        Seq(
          packMain := Map(
            "gear" -> "io.gearpump.cluster.main.Gear",
            "local" -> "io.gearpump.cluster.main.Local",
            "master" -> "io.gearpump.cluster.main.Master",
            "worker" -> "io.gearpump.cluster.main.Worker",
            "services" -> "io.gearpump.services.main.Services",
            "yarnclient" -> "io.gearpump.experiments.yarn.client.Client",
            "storm" -> "io.gearpump.experiments.storm.StormRunner"
          ),
          packJvmOpts := Map(
            "gear" -> Seq("-Djava.net.preferIPv4Stack=true", "-Dgearpump.home=${PROG_HOME}"),
            "local" -> Seq("-server", "-Djava.net.preferIPv4Stack=true", "-DlogFilename=local", "-Dgearpump.home=${PROG_HOME}", "-Djava.rmi.server.hostname=localhost"),
            "master" -> Seq("-server", "-Djava.net.preferIPv4Stack=true", "-DlogFilename=master", "-Dgearpump.home=${PROG_HOME}", "-Djava.rmi.server.hostname=localhost"),
            "worker" -> Seq("-server", "-Djava.net.preferIPv4Stack=true", "-DlogFilename=worker", "-Dgearpump.home=${PROG_HOME}", "-Djava.rmi.server.hostname=localhost"),
            "services" -> Seq("-server", "-Djava.net.preferIPv4Stack=true", "-Dgearpump.home=${PROG_HOME}", "-Djava.rmi.server.hostname=localhost"),
            "yarnclient" -> Seq("-server", "-Djava.net.preferIPv4Stack=true", "-Dgearpump.home=${PROG_HOME}", "-Djava.rmi.server.hostname=localhost"),
            "storm" -> Seq("-server", "-Djava.net.preferIPv4Stack=true", "-Dgearpump.home=${PROG_HOME}", "-Djava.rmi.server.hostname=localhost")
          ),
          packLibDir := Map(
            "lib" -> new ProjectsToPack(core.id, streaming.id),
            "lib/daemon" -> new ProjectsToPack(daemon.id).exclude(core.id, streaming.id),
            "lib/yarn" -> new ProjectsToPack(yarn.id).exclude(services.id, daemon.id),
            "lib/services" -> new ProjectsToPack(services.id).exclude(daemon.id),
            "lib/storm" -> new ProjectsToPack(storm.id).exclude(streaming.id)
          ),
          packExclude := Seq(thisProjectRef.value.project),
          //This is a work-around for https://github.com/gearpump/gearpump/issues/1816
          //Will be removed in the future when Akka release a new version which includes the fix.
          packExcludeJars := Seq(s"akka-actor_${scalaBinaryVersion.value}-$akkaVersion.jar"),
          packResourceDir += (baseDirectory.value / ".." / "conf" -> "conf"),
          packResourceDir += (baseDirectory.value / ".." / "yarnconf" -> "conf/yarnconf"),
          packResourceDir += (baseDirectory.value / ".." / "unmanagedlibs" / scalaBinaryVersion.value -> "lib"),
          packResourceDir += (baseDirectory.value / ".." / "services" / "dashboard" -> "dashboard"),
          packResourceDir += (baseDirectory.value / ".." / "examples" / "target" / CrossVersion.binaryScalaVersion(scalaVersion.value) -> "examples"),

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

          packArchivePrefix := projectName + "-" + scalaBinaryVersion.value

        )
  ).dependsOn(core, streaming, services, yarn, storm)
}
