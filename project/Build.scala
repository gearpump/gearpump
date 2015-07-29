import com.typesafe.sbt.SbtPgp.autoImport._
import de.johoop.jacoco4sbt.JacocoPlugin.jacoco
import sbt.Keys._
import sbt._
import sbtassembly.Plugin.AssemblyKeys._
import sbtassembly.Plugin._
import xerial.sbt.Pack._
import xerial.sbt.Sonatype._

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
  val clouderaVersion = "2.6.0-cdh5.4.2"
  val clouderaHBaseVersion = "1.0.0-cdh5.4.2"
  val codahaleVersion = "3.0.2"
  val commonsHttpVersion = "3.1"
  val commonsLoggingVersion = "1.1.3"
  val commonsLangVersion = "2.6"
  val commonsIOVersion = "2.4"
  val guavaVersion = "16.0.1"
  val dataReplicationVersion = "0.7"
  val upickleVersion = "0.3.4"
  val junitVersion = "4.12"
  val kafkaVersion = "0.8.2.1"
  val stormVersion = "0.9.3"
  val slf4jVersion = "1.7.7"
  
  val scalaVersionMajor = "scala-2.11"
  val scalaVersionNumber = "2.11.5"
  val sprayVersion = "1.3.2"
  val sprayJsonVersion = "1.3.1"
  val sprayWebSocketsVersion = "0.1.4"
  val scalaTestVersion = "2.2.0"
  val scalaCheckVersion = "1.11.3"
  val mockitoVersion = "1.10.17"
  val bijectionVersion = "0.7.0"
  val scalazVersion = "7.1.1"
  val algebirdVersion = "0.9.0"
  val chillVersion = "0.6.0"

  val commonSettings = Defaults.defaultSettings ++ Seq(jacoco.settings:_*) ++ sonatypeSettings  ++ net.virtualvoid.sbt.graph.Plugin.graphSettings ++
    Seq(
        resolvers ++= Seq(
          "patriknw at bintray" at "http://dl.bintray.com/patriknw/maven",
          "maven-repo" at "http://repo.maven.apache.org/maven2",
          "maven1-repo" at "http://repo1.maven.org/maven2",
          "maven2-repo" at "http://mvnrepository.com/artifact",
          "sonatype" at "https://oss.sonatype.org/content/repositories/releases",
          "bintray/non" at "http://dl.bintray.com/non/maven",
          "cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos",
          "clockfly" at "http://dl.bintray.com/clockfly/maven"
        ),
        addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0-M5" cross CrossVersion.full)
    ) ++
    Seq(
      scalaVersion := scalaVersionNumber,
      crossScalaVersions := Seq("2.10.5"),
      organization := "com.github.intel-hadoop",
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

  val hadoopDependency = Seq(
    ("org.apache.hadoop" % "hadoop-common" % clouderaVersion).
      exclude("org.mortbay.jetty", "jetty-util")
      exclude("org.mortbay.jetty", "jetty")
      exclude("org.fusesource.leveldbjni", "leveldbjni-all")
      exclude("tomcat", "jasper-runtime")
      exclude("commons-beanutils", "commons-beanutils-core")
      exclude("commons-beanutils", "commons-beanutils")
  )

  val daemonDependencies = Seq(
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-contrib" % akkaVersion
        exclude("com.typesafe.akka", "akka-persistence-experimental_2.11"),
      "com.typesafe.akka" %% "akka-cluster" % akkaVersion,
      "io.spray" %%  "spray-can"       % sprayVersion,
      "io.spray" %%  "spray-routing-shapeless2"   % sprayVersion,
      "commons-httpclient" % "commons-httpclient" % commonsHttpVersion,
      "commons-logging" % "commons-logging" % commonsLoggingVersion,
      "com.github.patriknw" %% "akka-data-replication" % dataReplicationVersion
    ) ++ hadoopDependency
  )

  val streamingDependencies = Seq(
    libraryDependencies ++= Seq(
      "com.goldmansachs" % "gs-collections-api" % "6.2.0",
      "com.goldmansachs" % "gs-collections" % "6.2.0"
    )
  )

  val coreDependencies = Seq(
        libraryDependencies ++= Seq(
        "com.codahale.metrics" % "metrics-core" % codahaleVersion,
        "com.codahale.metrics" % "metrics-graphite" % codahaleVersion,
        "org.slf4j" % "slf4j-api" % slf4jVersion,
        "org.slf4j" % "slf4j-log4j12" % slf4jVersion,
        "com.google.guava" % "guava" % guavaVersion,
        "commons-lang" % "commons-lang" % commonsLangVersion,
        "com.typesafe.akka" %% "akka-actor" % akkaVersion,
        "com.typesafe.akka" %% "akka-remote" % akkaVersion,
        "com.typesafe.akka" %% "akka-agent" % akkaVersion,
        "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
        "com.typesafe.akka" %% "akka-kernel" % akkaVersion,
        "com.github.romix.akka" %% "akka-kryo-serialization" % kryoVersion
          exclude("net.jpountz.lz4", "lz4"),
        "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test",
        "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
        "org.scalacheck" %% "scalacheck" % scalaCheckVersion % "test",
        "org.mockito" % "mockito-core" % mockitoVersion % "test",
        "junit" % "junit" % junitVersion % "test"
      ),
     libraryDependencies <+= (scalaVersion)("org.scala-lang" % "scala-reflect" % _),
     libraryDependencies ++= (
        if (scalaVersion.value.startsWith("2.10")) List("org.scalamacros" %% "quasiquotes" % "2.1.0-M5")
        else List("org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.3")
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
      Seq(
        travis_deploy := {
          val packagePath = s"output/target/gearpump-pack-${version.value}.tar.gz"
          val target = s"target/binary.gearpump.tar.gz"
          println(s"[Travis-Deploy] Move file $packagePath to $target")
          new File(packagePath).renameTo(new File(target))
        }
      )
  ).aggregate(core, daemon, streaming, fsio, examples_kafka, sol, wordcount, complexdag, services, external_kafka, stockcrawler,
      transport, examples, distributedshell, distributeservice, storm, yarn, dsl, pagerank, external_hbase, packProject, state)

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
          daemon.id -> "lib/daemon",
          services.id -> "lib/services",
          storm.id -> "lib/storm",
          yarn.id -> "lib/yarn"),
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
  ).dependsOn(core, streaming, services, yarn, storm, dsl)//, state)

  lazy val core = Project(
    id = "gearpump-core",
    base = file("core"),
    settings = commonSettings ++ coreDependencies
  )

  lazy val daemon = Project(
    id = "gearpump-daemon",
    base = file("daemon"),
    settings = commonSettings ++ daemonDependencies
  ) dependsOn(core % "test->test;compile->compile")

  lazy val streaming = Project(
    id = "gearpump-streaming",
    base = file("streaming"),
    settings = commonSettings ++ streamingDependencies
  )  dependsOn(core % "test->test;compile->compile", daemon % "test->test")

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
        )
      )
  ) dependsOn (streaming % "provided", dsl % "provided")

  lazy val examples_kafka = Project(
    id = "gearpump-examples-kafka",
    base = file("examples/streaming/kafka"),
    settings = commonSettings ++ myAssemblySettings ++
      Seq(
        libraryDependencies ++= Seq(
          "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
          "org.scalacheck" %% "scalacheck" % scalaCheckVersion % "test",
          "org.mockito" % "mockito-core" % mockitoVersion % "test",
          "junit" % "junit" % junitVersion % "test"
        ),
        target in assembly := baseDirectory.value.getParentFile.getParentFile / "target" / scalaVersionMajor
      )
  ) dependsOn(streaming % "test->test; provided", external_kafka)

  lazy val fsio = Project(
    id = "gearpump-examples-fsio",
    base = file("examples/streaming/fsio"),
    settings = commonSettings ++ myAssemblySettings ++
      Seq(
        libraryDependencies ++= Seq(
          "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
          "org.scalacheck" %% "scalacheck" % scalaCheckVersion % "test",
          "org.mockito" % "mockito-core" % mockitoVersion % "test"
        ) ++ hadoopDependency,
        mainClass in (Compile, packageBin) := Some("org.apache.gearpump.streaming.examples.fsio.SequenceFileIO"),
        target in assembly := baseDirectory.value.getParentFile.getParentFile / "target" / scalaVersionMajor
      )
  ) dependsOn (streaming % "test->test", streaming % "provided")

  lazy val sol = Project(
    id = "gearpump-examples-sol",
    base = file("examples/streaming/sol"),
    settings = commonSettings ++ myAssemblySettings ++
      Seq(
        libraryDependencies ++= Seq(
          "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
          "org.scalacheck" %% "scalacheck" % scalaCheckVersion % "test",
          "org.mockito" % "mockito-core" % mockitoVersion % "test"
        ),
        mainClass in (Compile, packageBin) := Some("org.apache.gearpump.streaming.examples.sol.SOL"),
        target in assembly := baseDirectory.value.getParentFile.getParentFile / "target" / scalaVersionMajor
      )
  ) dependsOn (streaming % "test->test", streaming % "provided")

  lazy val wordcount = Project(
    id = "gearpump-examples-wordcount",
    base = file("examples/streaming/wordcount"),
    settings = commonSettings ++ myAssemblySettings ++
      Seq(
        libraryDependencies ++= Seq(
          "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
          "org.scalacheck" %% "scalacheck" % scalaCheckVersion % "test",
          "org.mockito" % "mockito-core" % mockitoVersion % "test"
        ),
        mainClass in (Compile, packageBin) := Some("org.apache.gearpump.streaming.examples.wordcount.WordCount"),
        target in assembly := baseDirectory.value.getParentFile.getParentFile / "target" / scalaVersionMajor
      )
  ) dependsOn (streaming % "test->test", streaming % "provided")

  lazy val stockcrawler = Project(
    id = "gearpump-examples-stockcrawler",
    base = file("examples/streaming/stockcrawler"),
    settings = commonSettings ++ myAssemblySettings ++
      Seq(
        libraryDependencies ++= Seq(
          "io.spray" %%  "spray-can"       % sprayVersion,
          "io.spray" %%  "spray-routing-shapeless2"   % sprayVersion,
          "com.lihaoyi" %% "upickle" % upickleVersion,
          "commons-httpclient" % "commons-httpclient" % commonsHttpVersion,
          "net.sourceforge.htmlcleaner" % "htmlcleaner" % "2.2",
          "joda-time" % "joda-time" % "2.7",
          "io.spray" %%  "spray-json"    % sprayJsonVersion
        ),
        mainClass in (Compile, packageBin) := Some("org.apache.gearpump.streaming.examples.stock.main.Stock"),
        target in assembly := baseDirectory.value.getParentFile.getParentFile / "target" / scalaVersionMajor
      )
  ) dependsOn (streaming % "test->test", streaming % "provided", external_kafka  % "test->test; provided")

  lazy val complexdag = Project(
    id = "gearpump-examples-complexdag",
    base = file("examples/streaming/complexdag"),
    settings = commonSettings ++ myAssemblySettings ++
      Seq(
        libraryDependencies ++= Seq(
          "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
          "org.scalacheck" %% "scalacheck" % scalaCheckVersion % "test",
          "org.mockito" % "mockito-core" % mockitoVersion % "test"
        ),
        mainClass in (Compile, packageBin) := Some("org.apache.gearpump.streaming.examples.complexdag.Dag"),
        target in assembly := baseDirectory.value.getParentFile.getParentFile / "target" / scalaVersionMajor
      )
  ) dependsOn (streaming % "test->test", streaming % "provided")

  lazy val transport = Project(
    id = "gearpump-examples-transport",
    base = file("examples/streaming/transport"),
    settings = commonSettings ++ myAssemblySettings ++
      Seq(
        libraryDependencies ++= Seq(
          "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
          "org.scalacheck" %% "scalacheck" % scalaCheckVersion % "test",
          "org.mockito" % "mockito-core" % mockitoVersion % "test",
          "io.spray" %%  "spray-can"       % sprayVersion,
          "io.spray" %%  "spray-routing-shapeless2"   % sprayVersion,
          "io.spray" %%  "spray-json"    % sprayJsonVersion,
          "com.lihaoyi" %% "upickle" % upickleVersion
        ),
        mainClass in (Compile, packageBin) := Some("org.apache.gearpump.streaming.examples.transport.Transport"),
        target in assembly := baseDirectory.value.getParentFile.getParentFile / "target" / scalaVersionMajor
      )
  ) dependsOn (streaming % "test->test", streaming % "provided")

  lazy val examples = Project(
    id = "gearpump-examples",
    base = file("examples"),
    settings = commonSettings
  ) dependsOn (wordcount, complexdag, sol, fsio, examples_kafka, distributedshell, stockcrawler, transport)
  
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
          "com.wandoulabs.akka" %% "spray-websocket" % sprayWebSocketsVersion
            exclude("com.typesafe.akka", "akka-actor_2.11"),
          "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
          "org.webjars" % "angularjs" % "1.4.3", // We stick on 1.3.x until angular-ui-select works
          "org.webjars" % "bootstrap" % "3.3.5",
          "com.lihaoyi" %% "upickle" % upickleVersion,
          "org.webjars" % "d3js" % "3.5.5",
          "org.webjars" % "momentjs" % "2.10.3",
          "org.webjars.bower" % "angular-loading-bar" % "0.8.0",
          "org.webjars.bower" % "angular-smart-table" % "2.1.1",
          "org.webjars.bower" % "angular-motion" % "0.4.2",
          "org.webjars.bower" % "bootstrap-additions" % "0.3.1",
          "org.webjars.bower" % "angular-strap" % "2.3.1",
          "org.webjars.bower" % "angular-ui-select" % "0.12.0",
          "org.webjars" % "select2" % "3.5.2",
          "org.webjars" % "select2-bootstrap-css" % "1.4.6",
          "org.webjars.bower" % "ng-file-upload" % "5.0.9",
          "org.webjars.bower" % "vis" % "4.5.1"
        ).map(_.exclude("org.scalamacros", "quasiquotes_2.10")).map(_.exclude("org.scalamacros", "quasiquotes_2.10.3"))
      )
  ) dependsOn(streaming % "test->test;compile->compile", daemon % "test->test;compile->compile")

  lazy val distributedshell = Project(
    id = "gearpump-examples-distributedshell",
    base = file("examples/distributedshell"),
    settings = commonSettings ++ myAssemblySettings ++
      Seq(
        target in assembly := baseDirectory.value.getParentFile / "target" / scalaVersionMajor
      )
  ) dependsOn(core % "test->test", core % "provided")

  lazy val distributeservice = Project(
    id = "gearpump-experiments-distributeservice",
    base = file("experiments/distributeservice"),
    settings = commonSettings ++ Seq(
      libraryDependencies ++= Seq(
        "commons-lang" % "commons-lang" % commonsLangVersion,
        "commons-io" % "commons-io" % commonsIOVersion
      )
    )
  ) dependsOn(daemon % "test->test;compile->compile")

  lazy val storm = Project(
    id = "gearpump-experiments-storm",
    base = file("experiments/storm"),
    settings = commonSettings ++
      Seq(
        libraryDependencies ++= Seq(
          //"commons-lang" % "commons-lang" % commonsLangVersion,
          "commons-io" % "commons-io" % commonsIOVersion,
          "org.apache.storm" % "storm-core" % stormVersion
            exclude("ch.qos.logback", "logback-classic")
            exclude("ch.qos.logback", "logback-core")
            exclude("clj-stacktrace", "clj-stacktrace")
            exclude("clj-time", "clj-time")
            exclude("clout", "clout")
            exclude("compojure", "compojure")
            exclude("com.esotericsoftware.kryo", "kryo")
            exclude("com.esotericsoftware.minlog", "minlog")
            exclude("com.esotericsoftware.reflectasm", "reflectasm")
            exclude("com.googlecode.disruptor", "disruptor")
            exclude("com.twitter", "carbonite")
            exclude("com.twitter", "chill-java")
            exclude("commons-codec", "commons-codec")
            exclude("commons-fileupload", "commons-fileupload")
            exclude("commons-logging", "commons-logging")
            exclude("hiccup", "hiccup")
            exclude("javax.servlet", "servlet-api")
            exclude("jgrapht", "jgrapht-core")
            exclude("jline", "jline")
            exclude("joda-time", "joda-time")
            exclude("org.apache.commons", "commons-exec")
            exclude("org.clojure", "core.incubator")
            exclude("org.clojure", "math.numeric-tower")
            exclude("org.clojure", "tools.logging")
            exclude("org.clojure", "tools.cli")
            exclude("org.clojure", "tools.macro")
            exclude("org.mortbay.jetty", "jetty-util")
            exclude("org.mortbay.jetty", "jetty")
            exclude("org.objenisis", "objenisis")
            exclude("org.ow2.asm", "asm")
            exclude("org.slf4j", "log4j-over-slf4j")
            exclude("org.slf4j", "slf4j-api")
            exclude("ring", "ring-core")
            exclude("ring", "ring-devel")
            exclude("ring", "ring-jetty-adapter")
            exclude("ring", "ring-servlet"),
          "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
          "org.scalacheck" %% "scalacheck" % scalaCheckVersion % "test",
          "org.mockito" % "mockito-core" % mockitoVersion % "test"
        )
      )
  ) dependsOn (streaming % "test->test; provided")


  lazy val yarn = Project(
    id = "gearpump-experiments-yarn",
    base = file("experiments/yarn"),
    settings = commonSettings ++
      Seq(
        libraryDependencies ++= Seq(
        ("org.apache.hadoop" % "hadoop-hdfs" % clouderaVersion).
            exclude("org.mortbay.jetty", "jetty-util")
            exclude("org.mortbay.jetty", "jetty")
            exclude("tomcat", "jasper-runtime"),
          "org.apache.hadoop" % "hadoop-yarn-api" % clouderaVersion
            exclude("com.google.guava", "guava")
            exclude("com.google.protobuf", "protobuf-java")
            exclude("commons-lang", "commons-lang")
            exclude("commons-logging", "commons-logging")
            exclude("org.apache.hadoop", "hadoop-annotations"),
          "org.apache.hadoop" % "hadoop-yarn-client" % clouderaVersion
            exclude("com.google.guava", "guava")
            exclude("com.sun.jersey", "jersey-client")
            exclude("commons-cli", "commons-cli")
            exclude("commons-lang", "commons-lang")
            exclude("commons-logging", "commons-logging")
            exclude("log4j", "log4j")
            exclude("org.apache.hadoop", "hadoop-annotations")
            exclude("org.mortbay.jetty", "jetty-util")
            exclude("org.apache.hadoop", "hadoop-yarn-api")
            exclude("org.apache.hadoop", "hadoop-yarn-common"),
          "org.apache.hadoop" % "hadoop-yarn-common" % clouderaVersion
            exclude("com.google.guava", "guava")
            exclude("com.google.inject.extensions", "guice-servlet")
            exclude("com.google.inject", "guice")
            exclude("com.google.protobuf", "protobuf-java")
            exclude("com.sun.jersey.contribs", "jersey.guice")
            exclude("com.sun.jersey", "jersey-core")
            exclude("com.sun.jersey", "jersey-json")
            exclude("commons-cli", "commons-cli")
            exclude("commons-codec", "commons-codec")
            exclude("commons-io", "commons-io")
            exclude("commons-lang", "commons-lang")
            exclude("commons-logging", "commons-logging")
            exclude("javax.servlet", "servlet-api")
            exclude("javax.xml.bind", "jaxb-api")
            exclude("log4j", "log4j")
            exclude("org.apache.commons", "commons-compress")
            exclude("org.apache.hadoop", "hadoop-annotations")
            exclude("org.codehaus.jackson", "jackson-core-asl")
            exclude("org.codehaus.jackson", "jackson-jaxrs")
            exclude("org.codehaus.jackson", "jackson-mapper-asl")
            exclude("org.codehaus.jackson", "jackson-xc")
            exclude("org.slf4j", "slf4j-api"),
          "org.apache.hadoop" % "hadoop-yarn-server-resourcemanager" % clouderaVersion % "provided",
          "org.apache.hadoop" % "hadoop-yarn-server-nodemanager" % clouderaVersion % "provided"
        ) ++ hadoopDependency
      )
  ) dependsOn(services % "test->test;compile->compile", core % "provided", services % "provided")

  lazy val dsl = Project(
    id = "gearpump-experiments-dsl",
    base = file("experiments/dsl"),
    settings = commonSettings
  ) dependsOn(streaming % "test->test;compile->compile")
  
  lazy val pagerank = Project(
    id = "gearpump-experiments-pagerank",
    base = file("experiments/pagerank"),
    settings = commonSettings
  ) dependsOn(streaming % "test->test;compile->compile")

  lazy val external_hbase = Project(
    id = "gearpump-external-hbase",
    base = file("external/hbase"),
    settings = commonSettings ++
      Seq(
        resolvers ++= Seq(
          "cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos"
        )
      ) ++
      Seq(
        libraryDependencies ++= Seq(
          "org.apache.hadoop" % "hadoop-common" % clouderaVersion % "provided",
          "org.apache.hadoop" % "hadoop-hdfs" % clouderaVersion % "provided",
          "org.apache.hbase" % "hbase-client" % clouderaHBaseVersion
            exclude("com.github.stephenc.findbugs", "findbugs-annotations")
            exclude("com.google.guava", "guava")
            exclude("commons-codec", "commons-codec")
            exclude("commons-io", "commons-io")
            exclude("commons-lang", "commons-lang")
            exclude("commons-logging", "commons-logging")
            exclude("io.netty", "netty")
            exclude("junit", "junit")
            exclude("log4j", "log4j")
            exclude("org.apache.zookeeper", "zookeeper")
            exclude("org.codehaus.jackson", "jackson-mapper-asl"),
          "org.apache.hbase" % "hbase-common" % clouderaHBaseVersion
            exclude("com.github.stephenc.findbugs", "findbugs-annotations")
            exclude("com.google.guava", "guava")
            exclude("commons-codec", "commons-codec")
            exclude("commons-collections", "commons-collections")
            exclude("commons-io", "commons-io")
            exclude("commons-lang", "commons-lang")
            exclude("commons-logging", "commons-logging")
            exclude("junit", "junit")
            exclude("log4j", "log4j"),
          "org.scalaz" %% "scalaz-core" % scalazVersion,
          "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
          "org.scalacheck" %% "scalacheck" % scalaCheckVersion % "test",
          "org.mockito" % "mockito-core" % mockitoVersion % "test"
        )
      )
  ) dependsOn(streaming % "provided", dsl)

  lazy val state = Project(
    id = "gearpump-experiments-state",
    base = file("experiments/state"),
    settings = commonSettings ++
      Seq(
        libraryDependencies ++= Seq(
          "com.twitter" %% "bijection-core" % bijectionVersion,
          "com.twitter" %% "algebird-core" % algebirdVersion,
          "com.twitter" %% "chill-bijection" % chillVersion,
          "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
          "org.scalacheck" %% "scalacheck" % scalaCheckVersion % "test",
          "org.mockito" % "mockito-core" % mockitoVersion % "test"
        )
      )
  ) dependsOn(streaming % "test->test; provided", external_kafka % "test->test; provided")
}
