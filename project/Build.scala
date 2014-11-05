import sbt.Keys._
import sbt._
import xerial.sbt.Pack._

import scala.collection.immutable.Map.WithDefault

object Build extends sbt.Build {


  class DefaultValueMap[+B](value : B) extends WithDefault[String, B](null, (key) => value) {
    override def get(key: String) = Some(value)
  }
  
  val akkaVersion = "2.3.6"
  val kyroVersion = "0.3.2"
  val codahaleVersion = "3.0.2"
  val commonsLangVersion = "3.3.2"
  val commonsHttpVersion = "3.1"
  val gearPumpVersion = "0.2"
  val dataReplicationVersion = "0.7"
  val hadoopVersion = "2.5.1"
  val jgraphtVersion = "0.9.0"
  val json4sVersion = "3.2.10"
  val kafkaVersion = "0.8.1.1"
  val slf4jVersion = "1.7.7"
  val scalaVersionNumber = "2.10.4"
  val sprayVersion = "1.3.2"
  val sprayJsonVersion = "1.2.6"
  val spraySwaggerVersion = "0.5.0"
  val swaggerUiVersion = "2.0.24"

  val commonSettings = Defaults.defaultSettings ++ packAutoSettings ++
    Seq(
      scalaVersion := scalaVersionNumber,
      version := gearPumpVersion,
      organization := "com.github.intel-hadoop",
      crossPaths := false,
      scalacOptions ++= Seq(
        "-Yclosure-elim",
        "-Yinline"
      ),
      packResourceDir := Map(baseDirectory.value / "conf" -> "conf"),
      packExtraClasspath := new DefaultValueMap(Seq("${PROG_HOME}/conf"))
  )

  lazy val root = Project(
    id = "gearpump",
    base = file("."),
    settings = commonSettings ++ 
      Seq(
        resolvers ++= Seq(
          "patriknw at bintray" at "http://dl.bintray.com/patriknw/maven",
          "maven-repo" at "http://repo.maven.apache.org/maven2",
          "maven1-repo" at "http://repo1.maven.org/maven2",
          "maven2-repo" at "http://mvnrepository.com",
          "sonatype" at "https://oss.sonatype.org/content/repositories/releases",
          "clockfly" at "http://dl.bintray.com/clockfly/maven"
        )
      )
  )  dependsOn(core, streaming, examples, rest)

  lazy val core = Project(
    id = "gearpump-core",
    base = file("core"),
    settings = commonSettings  ++
    Seq(
        packResourceDir := Map(baseDirectory.value / "src/main/resources" -> "conf"),

        libraryDependencies ++= Seq(
        "org.jgrapht" % "jgrapht-core" % jgraphtVersion,
        "com.codahale.metrics" % "metrics-core" % codahaleVersion,
        "com.codahale.metrics" % "metrics-graphite" % codahaleVersion,
        "org.slf4j" % "slf4j-api" % slf4jVersion,
        "org.slf4j" % "slf4j-log4j12" % slf4jVersion,
        "org.slf4j" % "jul-to-slf4j" % slf4jVersion,
        "org.slf4j" % "jcl-over-slf4j" % slf4jVersion,
        "org.apache.commons" % "commons-lang3" % commonsLangVersion,
        "commons-httpclient" % "commons-httpclient" % commonsHttpVersion,
        "com.typesafe.akka" %% "akka-actor" % akkaVersion,
        "com.typesafe.akka" %% "akka-remote" % akkaVersion,
        "com.typesafe.akka" %% "akka-cluster" % akkaVersion,
        "com.typesafe.akka" %% "akka-contrib" % akkaVersion,
        "com.typesafe.akka" %% "akka-agent" % akkaVersion,
        "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
        "org.scala-lang" % "scala-compiler" % scalaVersionNumber,
        "com.github.romix.akka" %% "akka-kryo-serialization" % kyroVersion,
        "com.github.patriknw" %% "akka-data-replication" % dataReplicationVersion
      )
    ) 
  )

  lazy val streaming = Project(
    id = "gearpump-streaming",
    base = file("streaming"),
    settings = commonSettings ++
      Seq(
        libraryDependencies ++= Seq(
          "org.apache.kafka" %% "kafka" % kafkaVersion
        )
      )
  )  dependsOn(core)
  
  lazy val examples = Project(
    id = "gearpump-examples",
    base = file("examples"),
    settings = commonSettings  ++
      Seq(
        libraryDependencies ++= Seq(
          "org.apache.hadoop" % "hadoop-common" % hadoopVersion
        )
      )
  ) dependsOn(streaming)

  lazy val rest = Project(
    id = "gearpump-rest",
    base = file("rest"),
    settings = commonSettings  ++
      Seq(
        libraryDependencies ++= Seq(
          "io.spray" %%  "spray-can"       % sprayVersion,
          "io.spray" %%  "spray-routing"   % sprayVersion,
          "io.spray" %%  "spray-testkit"   % sprayVersion,
          "io.spray" %%  "spray-httpx"     % sprayVersion,
          "io.spray" %%  "spray-client"    % sprayVersion,
          "io.spray" %%  "spray-json"    % sprayJsonVersion,
          "com.gettyimages" %% "spray-swagger" % spraySwaggerVersion excludeAll( ExclusionRule(organization = "org.json4s"), ExclusionRule(organization = "io.spray") ),
          "org.json4s" %% "json4s-jackson" % json4sVersion,
          "org.json4s" %% "json4s-native"   % json4sVersion,
          "org.webjars" % "swagger-ui" % swaggerUiVersion
        )
      ) 
  ) dependsOn(core, streaming, examples)
}
