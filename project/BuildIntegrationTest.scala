import sbt.Keys._
import sbt._
import sbtassembly.Plugin.AssemblyKeys._
import Build._

object BuildIntegrationTest extends sbt.Build {

  val jsonSimpleVersion = "1.1"
  val storm09Version = "0.9.6"

  lazy val integration_test = Project(
    id = "gearpump-integrationtest",
    base = file("integrationtest"),
    settings = commonSettings ++ noPublish
  ) aggregate (it_core, it_storm09, it_storm010)

  val itTestFilter: String => Boolean = { name => name endsWith "Suite" }
  lazy val it_core = Project(
    id = "gearpump-integrationtest-core",
    base = file("integrationtest/core"),
    settings = commonSettings ++ noPublish ++ Seq(
      testOptions in IntegrationTest += Tests.Filter(itTestFilter),
      libraryDependencies ++= Seq(
        "com.lihaoyi" %% "upickle" % upickleVersion,
        "org.scalatest" %% "scalatest" % scalaTestVersion % "it",
        "org.pegdown" % "pegdown" % "1.4.2" % "it",
        "org.parboiled" % "parboiled-core" % "1.1.7" % "it",
        "org.parboiled" % "parboiled-java" % "1.1.7" % "it",
        "org.ow2.asm" % "asm-all" % "5.0.3" % "it"
      )
    )
  ).configs(IntegrationTest).settings(Defaults.itSettings : _*)
      .dependsOn(
        streaming % "test->test; provided",
        services % "test->test; provided",
        external_kafka,
        storm
      )

  // integration test for Storm 0.9.x
  lazy val it_storm09 = Project(
    id = "gearpump-integrationtest-storm09",
    base = file("integrationtest/storm09"),
        settings = commonSettings ++ noPublish ++ myAssemblySettings ++
        Seq(
          libraryDependencies ++= Seq(
            "org.apache.storm" % "storm-kafka" % storm09Version,
            "org.apache.storm" % "storm-starter" % storm09Version,
            "com.googlecode.json-simple" % "json-simple" % jsonSimpleVersion,
            "org.apache.kafka" %% "kafka" % kafkaVersion
          ),
          target in assembly := baseDirectory.value.getParentFile / "target" /
              CrossVersion.binaryScalaVersion(scalaVersion.value)
        )
  ) dependsOn(storm % "provided")

  // integration test for Storm 0.10.x
  lazy val it_storm010 = Project(
    id = "gearpump-integrationtest-storm010",
    base = file("integrationtest/storm010"),
    settings = commonSettings ++ noPublish ++ myAssemblySettings ++
        Seq(
          libraryDependencies ++= Seq(
            "org.apache.storm" % "storm-kafka" % stormVersion,
            "org.apache.storm" % "storm-starter" % stormVersion,
            "org.apache.kafka" %% "kafka" % kafkaVersion
          ),
          target in assembly := baseDirectory.value.getParentFile / "target" /
              CrossVersion.binaryScalaVersion(scalaVersion.value)
        )
  ) dependsOn(storm % "provided")

}
