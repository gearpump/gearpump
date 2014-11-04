import sbt.Keys._
import sbt._
import xerial.sbt.Pack._
import xerial.sbt.Sonatype._
import xerial.sbt.Sonatype.SonatypeKeys._
import de.johoop.jacoco4sbt.JacocoPlugin.jacoco

import scala.collection.immutable.Map.WithDefault

object Build extends sbt.Build {

  class DefaultValueMap[+B](value : B) extends WithDefault[String, B](null, (key) => value) {
    override def get(key: String) = Some(value)
  }
  
  val akkaVersion = "2.3.6"
  val kryoVersion = "0.3.2"
  val codahaleVersion = "3.0.2"
  val commonsLangVersion = "3.3.2"
  val commonsHttpVersion = "3.1"
  val gearpumpVersion = "0.2-SNAPSHOT"
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
  val scalaTestVersion = "2.2.0"
  val scalaCheckVersion = "1.11.3"

  val commonSettings = Defaults.defaultSettings ++ packAutoSettings ++ Seq(jacoco.settings:_*) ++ sonatypeSettings ++
    Seq(
        resolvers ++= Seq(
          "patriknw at bintray" at "http://dl.bintray.com/patriknw/maven",
          "maven-repo" at "http://repo.maven.apache.org/maven2",
          "maven1-repo" at "http://repo1.maven.org/maven2",
          "maven2-repo" at "http://mvnrepository.com",
          "sonatype" at "https://oss.sonatype.org/content/repositories/releases",
          "clockfly" at "http://dl.bintray.com/clockfly/maven"
        )
    ) ++
    Seq(
      scalaVersion := scalaVersionNumber,
      version := gearpumpVersion,
      organization := "com.github.intel-hadoop",
      crossPaths := false,
      scalacOptions ++= Seq("-Yclosure-elim","-Yinline"),
      packResourceDir := Map(baseDirectory.value / "conf" -> "conf"),
      packExtraClasspath := new DefaultValueMap(Seq("${PROG_HOME}/conf")),
      pomExtra := {
      <url>https://github.com/intel-hadoop/gearpump</url>
      <licenses>
        <license>
          <name>Apache 2</name>
          <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
        </license>
      </licenses>
      <scm>
        <connection>scm:git:github.com/intel-hadoop/gearpump</connection>
        <developerConnection>scm:git:git@github.com:intel-hadoop/gearpump</developerConnection>
        <url>github.com/intel-hadoop/gearpump</url>
      </scm>
      <developers>
        <developer>
          <id>gearpump</id>
          <name>Gearpump Team</name>
          <url>https://github.com/intel-hadoop/teams/gearpump</url>
        </developer>
      </developers>
    }
  )

  lazy val root = Project(
    id = "gearpump",
    base = file("."),
    settings = commonSettings 
  )  aggregate(core, streaming, fsio, kafka, sol, wordcount, rest)


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
        "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test",
        "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
        "org.scala-lang" % "scala-compiler" % scalaVersionNumber,
        "com.github.romix.akka" %% "akka-kryo-serialization" % kryoVersion,
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
  )  dependsOn(core % "test->test;compile->compile")
  
  lazy val fsio = Project(
    id = "gearpump-examples-fsio",
    base = file("examples/fsio"),
    settings = commonSettings  ++
      Seq(
        libraryDependencies ++= Seq(
          "org.apache.hadoop" % "hadoop-common" % hadoopVersion
        )
      )
  ) dependsOn streaming

  lazy val kafka = Project(
    id = "gearpump-examples-kafka",
    base = file("examples/kafka"),
    settings = commonSettings ++
      Seq(
        libraryDependencies ++= Seq(
          "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
          "org.scalacheck" %% "scalacheck" % scalaCheckVersion % "test"
        )
      )
  ) dependsOn streaming

  lazy val sol = Project(
    id = "gearpump-examples-sol",
    base = file("examples/sol"),
    settings = commonSettings
  ) dependsOn streaming

  lazy val wordcount = Project(
    id = "gearpump-examples-wordcount",
    base = file("examples/wordcount"),
    settings = commonSettings
  ) dependsOn streaming

  lazy val rest = Project(
    id = "gearpump-rest",
    base = file("rest"),
    settings = commonSettings  ++
      Seq(
        libraryDependencies ++= Seq(
          "io.spray" %%  "spray-can"       % sprayVersion,
          "io.spray" %%  "spray-routing"   % sprayVersion,
          "io.spray" %%  "spray-testkit"   % sprayVersion % "test",
          "io.spray" %%  "spray-httpx"     % sprayVersion,
          "io.spray" %%  "spray-client"    % sprayVersion,
          "io.spray" %%  "spray-json"    % sprayJsonVersion,
          "com.gettyimages" %% "spray-swagger" % spraySwaggerVersion excludeAll( ExclusionRule(organization = "org.json4s"), ExclusionRule(organization = "io.spray") ),
          "org.json4s" %% "json4s-jackson" % json4sVersion,
          "org.json4s" %% "json4s-native"   % json4sVersion,
          "org.webjars" % "swagger-ui" % swaggerUiVersion,
          "org.scalatest" %% "scalatest" % scalaTestVersion % "test"
        )
      ) 
  ) dependsOn(streaming % "test->test;compile->compile")
}
