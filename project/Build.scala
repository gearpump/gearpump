import de.johoop.jacoco4sbt.JacocoPlugin.jacoco
import sbt.Keys._
import sbt._
import sbtassembly.Plugin.AssemblyKeys._
import sbtassembly.Plugin._
import xerial.sbt.Pack._
import xerial.sbt.Sonatype._
import com.typesafe.sbt.SbtPgp.autoImport._
import sbtrelease._

import scala.collection.immutable.Map.WithDefault

object Build extends sbt.Build {

  class DefaultValueMap[+B](value : B) extends WithDefault[String, B](null, (key) => value) {
    override def get(key: String) = Some(value)
  }

  /**
   * deploy can recognize the path
   */
  val travis_deploy = taskKey[Unit]("use this after sbt assembly packArchive, it will rename the package so that travis deploy can find the package.")
  
  val akkaVersion = "2.3.6"
  val kryoVersion = "0.3.2"
  val codahaleVersion = "3.0.2"
  val commonsCodecVersion = "1.6"
  val commonsHttpVersion = "3.1"
  val commonsLangVersion = "3.3.2"
  val commonsLoggingVersion = "1.1.3"
  val commonsIOVersion = "2.4"
  val findbugsVersion = "2.0.1"
  val guavaVersion = "15.0"
  val dataReplicationVersion = "0.7"
  val hadoopVersion = "2.5.1"
  val jgraphtVersion = "0.9.0"
  val json4sVersion = "3.2.10"
  val kafkaVersion = "0.8.2.0"
  val sigarVersion = "1.6.4"
  val slf4jVersion = "1.7.7"
  val uPickleVersion = "0.2.5"
  
  val scalaVersionMajor = "scala-2.11"
  val scalaVersionNumber = "2.11.5"
  val sprayVersion = "1.3.2"
  val sprayJsonVersion = "1.3.1"
  val sprayWebSocketsVersion = "0.1.4"
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
          "maven2-repo" at "http://mvnrepository.com/artifact",
          "sonatype" at "https://oss.sonatype.org/content/repositories/releases",
          "bintray/non" at "http://dl.bintray.com/non/maven",
          "clockfly" at "http://dl.bintray.com/clockfly/maven"
        )
    ) ++
    Seq(
      scalaVersion := scalaVersionNumber,
      organization := "com.github.intel-hadoop",
      parallelExecution in Test := false,
      parallelExecution in ThisBuild := false,
      useGpg := false,
      pgpSecretRing := file("./secring.asc"),
      pgpPublicRing := file("./pubring.asc"),
      scalacOptions ++= Seq("-Yclosure-elim","-Yinline"),
      publishMavenStyle := true,

      pgpPassphrase := Option(System.getenv().get("PASSPHRASE")).map(_.toArray),
      credentials += Credentials(
                   "Sonatype Nexus Repository Manager", 
                   "oss.sonatype.org", 
                   System.getenv().get("SONATYPE_USERNAME"), 
                   System.getenv().get("SONATYPE_PASSWORD")),
      
      pomIncludeRepository := { _ => false },

      publishTo := {
        val nexus = "https://oss.sonatype.org/"
        if (isSnapshot.value)
          Some("snapshots" at nexus + "content/repositories/snapshots")
        else
          Some("releases"  at nexus + "service/local/staging/deploy/maven2")
      },

      publishArtifact in Test := true,
      
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
        "org.scala-lang" % "scala-compiler" % scalaVersionNumber,
        "com.github.romix.akka" %% "akka-kryo-serialization" % kryoVersion,
        "com.github.patriknw" %% "akka-data-replication" % dataReplicationVersion,
        "org.apache.hadoop" % "hadoop-common" % hadoopVersion,
        "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion,
        "io.spray" %%  "spray-can"       % sprayVersion,
        "io.spray" %%  "spray-routing-shapeless2"   % sprayVersion,
        "commons-io" % "commons-io" % commonsIOVersion,
        "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test",
        "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
        "org.scalacheck" %% "scalacheck" % scalaCheckVersion % "test",
        "org.mockito" % "mockito-core" % mockitoVersion % "test"
      )
  )

  val myAssemblySettings = assemblySettings ++ Seq(
    test in assembly := {},
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
                        "services" -> "org.apache.gearpump.cluster.main.Services"
                       ),
        packJvmOpts := Map("local" -> Seq("-DlogFilename=local"),
                           "master" -> Seq("-DlogFilename=master"),
                           "worker" -> Seq("-DlogFilename=worker")
                        ),
        packExclude := Seq(fsio.id/*, examples_kafka.id*/, sol.id, wordcount.id, complexdag.id, examples.id, distributedshell.id),
        packResourceDir += (baseDirectory.value / "conf" -> "conf"),
        packResourceDir += (baseDirectory.value / "services" / "dashboard" -> "dashboard"),
        packResourceDir += (baseDirectory.value / "examples" / "target" / scalaVersionMajor -> "examples"),
        parallelExecution in ThisBuild := false,
        travis_deploy := {
          val packagePath = s"target/gearpump-${version.value}.tar.gz"
          val target = s"target/binary.gearpump.tar.gz"
          println(s"[Travis-Deploy] Move file $packagePath to $target")
          new File(packagePath).renameTo(new File(target))
        },
        
        // The classpath should not be expanded. Otherwise, the classpath maybe too long.
        // On windows, it may report shell error "command line too long"
        packExpandedClasspath := false,
        packExtraClasspath := new DefaultValueMap(Seq("${PROG_HOME}/conf", "${PROG_HOME}/dashboard"))
      )
  ).dependsOn(core, streaming, services, external_kafka)
   .aggregate(core, streaming, fsio/*, examples_kafka*/, sol, wordcount, complexdag, services, external_kafka, examples, distributedshell, distributeservice)

  lazy val core = Project(
    id = "gearpump-core",
    base = file("core"),
    settings = commonSettings ++ coreDependencies
  )

  lazy val streaming = Project(
    id = "gearpump-streaming",
    base = file("streaming"),
    settings = commonSettings ++
      Seq(
        libraryDependencies ++= Seq(
          "com.lihaoyi" %% "upickle" % "0.2.5"
        )
     )
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
          "org.mockito" % "mockito-core" % mockitoVersion % "test",
          ("org.apache.kafka" %% "kafka" % kafkaVersion classifier("test")) % "test"
        ),
        unmanagedClasspath in Test += baseDirectory.value.getParentFile.getParentFile / "conf"
      )
  ) dependsOn (streaming % "provided")
/*
  lazy val examples_kafka = Project(
    id = "gearpump-examples-kafka",
    base = file("examples/streaming/kafka"),
    settings = commonSettings ++
      Seq(
        libraryDependencies ++= Seq(
          "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
          "org.scalacheck" %% "scalacheck" % scalaCheckVersion % "test",
          "org.mockito" % "mockito-core" % mockitoVersion % "test"
        )
      )
  ) dependsOn(streaming % "test->test", streaming % "provided", external_kafka  % "test->test; provided")
*/
  lazy val fsio = Project(
    id = "gearpump-examples-fsio",
    base = file("examples/streaming/fsio"),
    settings = commonSettings ++
      Seq(
        libraryDependencies ++= Seq(
          "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
          "org.scalacheck" %% "scalacheck" % scalaCheckVersion % "test",
          "org.mockito" % "mockito-core" % mockitoVersion % "test"
        )
      )
  ) dependsOn (streaming % "test->test", streaming % "provided")

  lazy val sol = Project(
    id = "gearpump-examples-sol",
    base = file("examples/streaming/sol"),
    settings = commonSettings ++
      Seq(
        libraryDependencies ++= Seq(
          "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
          "org.scalacheck" %% "scalacheck" % scalaCheckVersion % "test",
          "org.mockito" % "mockito-core" % mockitoVersion % "test"
        )
      )
  ) dependsOn (streaming % "test->test", streaming % "provided")

  lazy val wordcount = Project(
    id = "gearpump-examples-wordcount",
    base = file("examples/streaming/wordcount"),
    settings = commonSettings ++
      Seq(
        libraryDependencies ++= Seq(
          "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
          "org.scalacheck" %% "scalacheck" % scalaCheckVersion % "test",
          "org.mockito" % "mockito-core" % mockitoVersion % "test"
        )
      )
  ) dependsOn (streaming % "test->test", streaming % "provided")

  lazy val complexdag = Project(
    id = "gearpump-examples-complexdag",
    base = file("examples/streaming/complexdag"),
    settings = commonSettings ++
      Seq(
        libraryDependencies ++= Seq(
          "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
          "org.scalacheck" %% "scalacheck" % scalaCheckVersion % "test",
          "org.mockito" % "mockito-core" % mockitoVersion % "test"
        )
      )
  ) dependsOn (streaming % "test->test", streaming % "provided")

  lazy val examples = Project(
    id = "gearpump-examples",
    base = file("examples"),
    settings = commonSettings ++ myAssemblySettings
  ) dependsOn (wordcount, complexdag, sol, fsio/*, examples_kafka*/, distributedshell)
  
  lazy val services = Project(
    id = "gearpump-services",
    base = file("services"),
    settings = commonSettings  ++ 
      Seq(
        libraryDependencies ++= Seq(
          "io.spray" %%  "spray-testkit"   % sprayVersion % "test",
          "io.spray" %%  "spray-httpx"     % sprayVersion,
          "io.spray" %%  "spray-client"    % sprayVersion,
          "io.spray" %%  "spray-json"    % sprayJsonVersion,
          "com.wandoulabs.akka" %% "spray-websocket" % sprayWebSocketsVersion,
          "org.webjars" % "angularjs" % "1.2.26",
          "org.webjars" % "angular-ui-bootstrap" % "0.11.0",
          "org.webjars" % "angular-route-segment" % "1.3.3",
          "org.webjars" % "font-awesome" % "4.3.0",
          "org.webjars" % "jquery" % "2.0.3",
          "org.webjars" % "jquery-ui" % "1.10.3",
          "org.webjars" % "angular-ui-sortable" % "0.12.11-1",
          "org.webjars" % "angular-local-storage" % "0.1.1",
          "org.webjars" % "bootstrap" % "3.2.0",
          "org.webjars" % "d3js" % "3.5.3",
          "org.webjars" % "visjs" % "3.10.0",
          "org.webjars" % "json3" % "3.3.2",
          "org.webjars" % "es5-shim" % "2.1.0",
          "org.webjars" % "ng-table" % "0.3.3",
          "org.json4s" %% "json4s-jackson" % json4sVersion,
          "org.json4s" %% "json4s-native"   % json4sVersion,
          "org.scalatest" %% "scalatest" % scalaTestVersion % "test"
        )
      )
  ) dependsOn(streaming % "test->test;compile->compile")

  lazy val distributedshell = Project(
    id = "gearpump-examples-distributedshell",
    base = file("examples/distributedshell"),
    settings = commonSettings
  ) dependsOn(core % "test->test", core % "provided")

  lazy val distributeservice = Project(
    id = "gearpump-experiments-distributeservice",
    base = file("experiments/distributeservice"),
    settings = commonSettings
  ) dependsOn(core % "test->test;compile->compile")

}
