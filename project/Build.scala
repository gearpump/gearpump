import sbt.Keys._
import sbt._
import xerial.sbt.Pack._
import xerial.sbt.Sonatype._
import xerial.sbt.Sonatype.SonatypeKeys._
import de.johoop.jacoco4sbt.JacocoPlugin.jacoco

import scala.collection.immutable.Map.WithDefault

import sbtassembly.Plugin._
import AssemblyKeys._

object Build extends sbt.Build {

  class DefaultValueMap[+B](value : B) extends WithDefault[String, B](null, (key) => value) {
    override def get(key: String) = Some(value)
  }
  
  val akkaVersion = "2.3.6"
  val kryoVersion = "0.3.2"
  val codahaleVersion = "3.0.2"
  val commonsCodecVersion = "1.6"
  val commonsHttpVersion = "3.1"
  val commonsLangVersion = "3.3.2"
  val commonsLoggingVersion = "1.1.3"
  val commonsIOVersion = "2.4"
  val findbugsVersion = "2.0.1"
  val gearPumpVersion = "0.2-SNAPSHOT"
  val guavaVersion = "15.0"
  val dataReplicationVersion = "0.7"
  val hadoopVersion = "2.5.1"
  val jgraphtVersion = "0.9.0"
  val json4sVersion = "3.2.10"
  val kafkaVersion = "0.8.2-beta"
  val sigarVersion = "1.6.4"
  val slf4jVersion = "1.7.7"
  
  val scalaVersionMajor = "scala-2.11"
  val scalaVersionNumber = "2.11.4"
  val sprayVersion = "1.3.2"
  val sprayJsonVersion = "1.3.1"
  val spraySwaggerVersion = "0.5.0"
  val swaggerUiVersion = "2.0.24"
  val scalaTestVersion = "2.2.0"
  val scalaCheckVersion = "1.11.3"
  val mockitoVersion = "1.10.8"
  val bijectionVersion = "0.7.0"

  val commonSettings = Defaults.defaultSettings ++ Seq(jacoco.settings:_*) ++ sonatypeSettings  ++ net.virtualvoid.sbt.graph.Plugin.graphSettings ++
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
      version := gearPumpVersion,
      organization := "com.github.intel-hadoop",
      scalacOptions ++= Seq("-Yclosure-elim","-Yinline"),
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

  val coreDependencies = Seq(
        libraryDependencies ++= Seq(
        "org.jgrapht" % "jgrapht-core" % jgraphtVersion,
        "com.codahale.metrics" % "metrics-core" % codahaleVersion,
        "com.codahale.metrics" % "metrics-graphite" % codahaleVersion,
        "org.slf4j" % "slf4j-api" % slf4jVersion,
        "org.slf4j" % "slf4j-log4j12" % slf4jVersion,
        "org.slf4j" % "jul-to-slf4j" % slf4jVersion,
        "org.slf4j" % "jcl-over-slf4j" % slf4jVersion,
        "org.fusesource" % "sigar" % sigarVersion classifier("native"),
        "com.google.code.findbugs" % "jsr305" % findbugsVersion,
        "org.apache.commons" % "commons-lang3" % commonsLangVersion,
        "commons-logging" % "commons-logging" % commonsLoggingVersion,
        "commons-httpclient" % "commons-httpclient" % commonsHttpVersion,
        "commons-codec" % "commons-codec" % commonsCodecVersion,
        "com.google.guava" % "guava" % guavaVersion,
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
        "com.github.patriknw" %% "akka-data-replication" % dataReplicationVersion,
        "org.apache.hadoop" % "hadoop-common" % hadoopVersion,
        "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion,
        "io.spray" %%  "spray-can"       % sprayVersion,
        "io.spray" %%  "spray-routing"   % sprayVersion,
        "commons-io" % "commons-io" % commonsIOVersion
      )
  )

  val myAssemblySettings = assemblySettings ++ Seq(
    assemblyOption in assembly ~= { _.copy(includeScala = false) }
  )

  lazy val root = Project(
    id = "gearpump",
    base = file("."),
    settings = commonSettings ++
      packSettings ++
      Seq(
        packMain := Map("gear" -> "org.apache.gearpump.cluster.main.Gear",
                        "local" -> "org.apache.gearpump.cluster.main.Local",
                        "master" -> "org.apache.gearpump.cluster.main.Master",
                        "worker" -> "org.apache.gearpump.cluster.main.Worker",
                        "rest" -> "org.apache.gearpump.cluster.main.Rest"
                       ),
        packJvmOpts := Map("local" -> Seq("-DlogFilename=local"),
                           "master" -> Seq("-DlogFilename=master"),
                           "worker" -> Seq("-DlogFilename=worker")
                        ),
        packExclude := Seq(fsio.id, examples_kafka.id, sol.id, wordcount.id, examples.id, distributedshell.id),
        packResourceDir += (baseDirectory.value / "conf" -> "conf"),
        packResourceDir += (baseDirectory.value / "examples" / "target" / scalaVersionMajor -> "examples"),
        packExpandedClasspath := false,
        packExtraClasspath := new DefaultValueMap(Seq("${PROG_HOME}/conf"))
      )
  ).dependsOn(core, streaming, rest, external_kafka, distributedshell).aggregate(core, streaming, fsio, examples_kafka,
      sol, wordcount, rest, external_kafka, examples, distributedshell)

  lazy val core = Project(
    id = "gearpump-core",
    base = file("core"),
    settings = commonSettings ++ coreDependencies
  )

  lazy val streaming = Project(
    id = "gearpump-streaming",
    base = file("streaming"),
    settings = commonSettings
  )  dependsOn(core % "test->test;compile->compile")

  lazy val external_kafka = Project(
    id = "gearpump-external-kafka",
    base = file("external/kafka"),
    settings = commonSettings ++ myAssemblySettings ++
      Seq(
        libraryDependencies ++= Seq(
          "org.apache.kafka" %% "kafka" % kafkaVersion,
          "com.twitter" %% "bijection-core" % bijectionVersion,
          "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
          "org.scalacheck" %% "scalacheck" % scalaCheckVersion % "test",
          "org.mockito" % "mockito-core" % mockitoVersion % "test"
        )
      )
  ) dependsOn (streaming % "provided")

  lazy val examples_kafka = Project(
    id = "gearpump-examples-kafka",
    base = file("examples/kafka"),
    settings = commonSettings ++
      Seq(
        libraryDependencies ++= Seq(
          "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
          "org.scalacheck" %% "scalacheck" % scalaCheckVersion % "test",
          "org.mockito" % "mockito-core" % mockitoVersion % "test"
        )
      )
  ) dependsOn(streaming % "provided", external_kafka  % "provided")

  lazy val fsio = Project(
    id = "gearpump-examples-fsio",
    base = file("examples/fsio"),
    settings = commonSettings
  ) dependsOn (streaming % "provided")

  lazy val sol = Project(
    id = "gearpump-examples-sol",
    base = file("examples/sol"),
    settings = commonSettings
  ) dependsOn (streaming % "provided")

  lazy val wordcount = Project(
    id = "gearpump-examples-wordcount",
    base = file("examples/wordcount"),
    settings = commonSettings
  ) dependsOn (streaming % "provided")

  lazy val examples = Project(
    id = "gearpump-examples",
    base = file("examples"),
    settings = commonSettings ++ myAssemblySettings
  ) dependsOn (wordcount, sol, fsio, examples_kafka)
  
  lazy val rest = Project(
    id = "gearpump-rest",
    base = file("rest"),
    settings = commonSettings  ++ 
      Seq(
        libraryDependencies ++= Seq(
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

  lazy val distributedshell = Project(
    id = "gearpump-experiments-distributedshell",
    base = file("experiments/distributedshell"),
    settings = commonSettings ++ packSettings
  ) dependsOn(core % "test->test;compile->compile")
}
