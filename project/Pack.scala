import sbt.Keys._
import sbt._
import Build._
import xerial.sbt.Pack._

object Pack extends sbt.Build {
  val daemonClassPath = Seq(
    "${PROG_HOME}/conf",
    "${PROG_HOME}/lib/daemon/*"
  )

  val serviceClassPath = daemonClassPath ++ Seq(
    "${PROG_HOME}/lib/services/*",
    "${PROG_HOME}/dashboard"
  )

  val yarnClassPath = serviceClassPath ++ Seq(
    "${PROG_HOME}/lib/yarn/*",
    "/etc/hadoop/conf",
    "/etc/hbase/conf"
  )

  val stormClassPath = daemonClassPath ++ Seq(
    "${PROG_HOME}/lib/storm/*"
  )

  lazy val packProject = Project(
    id = "gearpump-pack",
    base = file("output"),
    settings = commonSettings ++
        packSettings ++
        Seq(
          packMain := Map("gear" -> "org.apache.gearpump.cluster.main.Gear",
            "local" -> "org.apache.gearpump.cluster.main.Local",
            "master" -> "org.apache.gearpump.cluster.main.Master",
            "worker" -> "org.apache.gearpump.cluster.main.Worker",
            "services" -> "org.apache.gearpump.services.main.Services",
            "yarnclient" -> "org.apache.gearpump.experiments.yarn.client.Client",
            "storm" -> "org.apache.gearpump.experiments.storm.StormRunner"
          ),
          packJvmOpts := Map(
            "local" -> Seq("-server", "-DlogFilename=local", "-Dgearpump.home=${PROG_HOME}", "-Djava.rmi.server.hostname=localhost"),
            "master" -> Seq("-server", "-DlogFilename=master", "-Dgearpump.home=${PROG_HOME}", "-Djava.rmi.server.hostname=localhost"),
            "worker" -> Seq("-server", "-DlogFilename=worker", "-Dgearpump.home=${PROG_HOME}", "-Djava.rmi.server.hostname=localhost"),
            "services" -> Seq("-server", "-Dgearpump.home=${PROG_HOME}", "-Djava.rmi.server.hostname=localhost")
          ),
          packLibDir := Map(
            "lib" -> new ProjectsToPack(core.id, streaming.id),
            "lib/daemon" -> new ProjectsToPack(daemon.id).exclude(core.id),
            "lib/yarn" -> new ProjectsToPack(yarn.id).exclude(services.id, daemon.id),
            "lib/services" -> new ProjectsToPack(services.id).exclude(daemon.id),
            "lib/storm" -> new ProjectsToPack(storm.id).exclude(streaming.id)
          ),
          packExclude := Seq(thisProjectRef.value.project),
          packResourceDir += (baseDirectory.value / ".." / "conf" -> "conf"),
          packResourceDir += (baseDirectory.value / ".." / "services" / "dashboard" -> "dashboard"),
          packResourceDir += (baseDirectory.value / ".." / "examples" / "target" / scalaVersionMajor -> "examples"),

          // The classpath should not be expanded. Otherwise, the classpath maybe too long.
          // On windows, it may report shell error "command line too long"
          packExpandedClasspath := false,
          packExtraClasspath := Map(
            "gear" -> daemonClassPath,
            "local" -> daemonClassPath,
            "master" -> daemonClassPath,
            "worker" -> daemonClassPath,
            "services" -> serviceClassPath,
            "yarnclient" -> yarnClassPath,
            "storm" -> stormClassPath
          )
        )
  ).dependsOn(core, streaming, services, yarn, storm, Build.dsl, state_api)
}
