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

import com.typesafe.sbt.SbtPgp.autoImport._
import BuildExample.examples
import BuildIntegrationTest.integration_test
import de.johoop.jacoco4sbt.JacocoPlugin.jacoco
import org.scalajs.sbtplugin.ScalaJSPlugin.autoImport._
import sbt.Keys._
import sbt._
import Pack.packProject
import org.scalajs.sbtplugin.cross.CrossProject
import sbtassembly.Plugin.AssemblyKeys._
import sbtassembly.Plugin._
import sbtunidoc.Plugin.UnidocKeys._
import sbtunidoc.Plugin._
import xerial.sbt.Sonatype._

object Build extends sbt.Build {

  val copySharedSourceFiles = TaskKey[Unit]("copied shared services source code")

  val akkaVersion = "2.4.3"
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
  val stormVersion = "0.10.0"
  val slf4jVersion = "1.7.7"
  val gsCollectionsVersion = "6.2.0"

  val crossScalaVersionNumbers = Seq("2.11.8")
  val scalaVersionNumber = crossScalaVersionNumbers.last
  val sprayVersion = "1.3.2"
  val sprayJsonVersion = "1.3.1"
  val scalaTestVersion = "2.2.0"
  val scalaCheckVersion = "1.11.3"
  val mockitoVersion = "1.10.17"
  val bijectionVersion = "0.8.0"
  val scalazVersion = "7.1.1"
  val algebirdVersion = "0.9.0"
  val chillVersion = "0.6.0"
  val distDirectory = "output"
  val projectName = "gearpump"

  override def projects: Seq[Project] = (super.projects.toList ++ BuildExample.projects.toList
    ++ Pack.projects.toList).toSeq

  val commonSettings = Seq(jacoco.settings: _*) ++ sonatypeSettings ++
    Seq(
      resolvers ++= Seq(
        "patriknw at bintray" at "http://dl.bintray.com/patriknw/maven",
        "maven-repo" at "http://repo.maven.apache.org/maven2",
        "maven1-repo" at "http://repo1.maven.org/maven2",
        "maven2-repo" at "http://mvnrepository.com/artifact",
        "sonatype" at "https://oss.sonatype.org/content/repositories/releases",
        "bintray/non" at "http://dl.bintray.com/non/maven",
        "cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos",
        "clockfly" at "http://dl.bintray.com/clockfly/maven",
        "vincent" at "http://dl.bintray.com/fvunicorn/maven",
        "clojars" at "http://clojars.org/repo"
      )
      // ,addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0-M5" cross CrossVersion.full)
    ) ++
    Seq(
      scalaVersion := scalaVersionNumber,
      crossScalaVersions := crossScalaVersionNumbers,
      organization := "com.github.intel-hadoop",
      useGpg := false,
      pgpSecretRing := file("./secring.asc"),
      pgpPublicRing := file("./pubring.asc"),
      scalacOptions ++= Seq("-Yclosure-elim", "-Yinline"),
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
        if (isSnapshot.value) {
          Some("snapshots" at nexus + "content/repositories/snapshots")
        } else {
          Some("releases" at nexus + "service/local/staging/deploy/maven2")
        }
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

  val noPublish = Seq(
    publish := {},
    publishLocal := {},
    publishArtifact := false,
    publishArtifact in Test := false
  )

  val daemonDependencies = Seq(
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-cluster" % akkaVersion,
      "com.typesafe.akka" %% "akka-cluster-tools" % akkaVersion,
      "com.typesafe.akka" %% "akka-http-experimental" % akkaVersion,
      "com.typesafe.akka" %% "akka-http-spray-json-experimental" % akkaVersion,
      "commons-logging" % "commons-logging" % commonsLoggingVersion,
      "com.typesafe.akka" %% "akka-distributed-data-experimental" % akkaVersion,
      "org.apache.hadoop" % "hadoop-common" % clouderaVersion % "provided"
    )
  )

  val streamingDependencies = Seq(
    libraryDependencies ++= Seq(
      "com.github.intel-hadoop" % "gearpump-shaded-gs-collections" % gsCollectionsVersion
    )
  )

  val coreDependencies = Seq(
    libraryDependencies ++= Seq(
      "com.github.intel-hadoop" % "gearpump-shaded-metrics-graphite" % codahaleVersion,
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.slf4j" % "slf4j-log4j12" % slf4jVersion,
      "com.github.intel-hadoop" % "gearpump-shaded-guava" % guavaVersion,
      "commons-lang" % "commons-lang" % commonsLangVersion,
      "com.google.code.findbugs" % "jsr305" % "1.3.9" % "compile",

      /**
       * Overrides Netty version 3.10.3.Final used by Akka 2.4.2 to work-around netty hang issue
       * (https://github.com/gearpump/gearpump/issues/2020)
       *
       * Akka 2.4.2 by default use Netty 3.10.3.Final, which has a serious issue which can hang
       * the network. The same issue also happens in version range (3.10.0.Final, 3.10.5.Final)
       * Netty 3.10.6.Final have this issue fixed, however, we find there is a 20% performance
       * drop. So we decided to downgrade netty to 3.8.0.Final (Same version used in akka 2.3.12).
       *
       * @see https://github.com/gearpump/gearpump/pull/2017 for more discussions.
       */
      "io.netty" % "netty" % "3.8.0.Final",
      "com.typesafe.akka" %% "akka-remote" % akkaVersion
        exclude("io.netty", "netty"),

      "com.typesafe.akka" %% "akka-actor" % akkaVersion,
      "com.typesafe.akka" %% "akka-agent" % akkaVersion,
      "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
      "com.typesafe.akka" %% "akka-kernel" % akkaVersion,
      "com.github.intel-hadoop" %% "gearpump-shaded-akka-kryo" % kryoVersion,
      "org.scala-lang" % "scala-reflect" % scalaVersionNumber,
      "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.4",
      "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test",
      "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
      "org.scalacheck" %% "scalacheck" % scalaCheckVersion % "test",
      "org.mockito" % "mockito-core" % mockitoVersion % "test",
      "junit" % "junit" % junitVersion % "test"
    )
  )

  lazy val javadocSettings = Seq(
    addCompilerPlugin("com.typesafe.genjavadoc" %% "genjavadoc-plugin" %
      "0.9" cross CrossVersion.full),
    scalacOptions += s"-P:genjavadoc:out=${target.value}/java"
  )

  val myAssemblySettings = assemblySettings ++ Seq(
    test in assembly := {},
    assemblyOption in assembly ~= {
      _.copy(includeScala = false)
    },
    jarName in assembly := {
      s"${name.value.split("-").last}-${scalaBinaryVersion.value}-${version.value}-assembly.jar"
    }
  )

  val projectsWithDoc = inProjects(
    core,
    streaming,
    external_kafka,
    external_monoid,
    external_serializer,
    external_hbase,
    external_hadoopfs,
    streaming)

  lazy val gearpumpUnidocSetting = scalaJavaUnidocSettings ++ Seq(
    unidocProjectFilter in(ScalaUnidoc, unidoc) := projectsWithDoc,
    unidocProjectFilter in(JavaUnidoc, unidoc) := projectsWithDoc,

    unidocAllSources in(ScalaUnidoc, unidoc) := {
      ignoreUndocumentedPackages((unidocAllSources in(ScalaUnidoc, unidoc)).value)
    },

    // Skip class names containing $ and some internal packages in Javadocs
    unidocAllSources in(JavaUnidoc, unidoc) := {
      ignoreUndocumentedPackages((unidocAllSources in(JavaUnidoc, unidoc)).value)
    }
  )

  private def ignoreUndocumentedPackages(packages: Seq[Seq[File]]): Seq[Seq[File]] = {
    packages.map(_.filterNot(_.getCanonicalPath.contains("akka")))
  }

  lazy val root = Project(
    id = "gearpump",
    base = file("."),
    settings = commonSettings ++ noPublish ++ gearpumpUnidocSetting)
    .aggregate(core, daemon, streaming, services, external_kafka, external_monoid,
      external_serializer, examples, storm, yarn, external_hbase, packProject,
      external_hadoopfs, integration_test).settings(Defaults.itSettings: _*)

  lazy val core = Project(
    id = "gearpump-core",
    base = file("core"),
    settings = commonSettings ++ javadocSettings ++ coreDependencies
  )

  lazy val daemon = Project(
    id = "gearpump-daemon",
    base = file("daemon"),
    settings = commonSettings ++ daemonDependencies
  ) dependsOn(core % "test->test; compile->compile", cgroup % "test->test; compile->compile")

  lazy val cgroup = Project(
    id = "gearpump-experimental-cgroup",
    base = file("experiments/cgroup"),
    settings = commonSettings ++ noPublish ++ daemonDependencies
  ) dependsOn (core % "test->test; compile->compile")

  lazy val streaming = Project(
    id = "gearpump-streaming",
    base = file("streaming"),
    settings = commonSettings ++ javadocSettings ++ streamingDependencies
  ) dependsOn(core % "test->test; compile->compile", daemon % "test->test")

  lazy val external_kafka = Project(
    id = "gearpump-external-kafka",
    base = file("external/kafka"),
    settings = commonSettings ++ javadocSettings  ++
      Seq(
        libraryDependencies ++= Seq(
          "org.apache.kafka" %% "kafka" % kafkaVersion,
          "com.twitter" %% "bijection-core" % bijectionVersion,
          ("org.apache.kafka" %% "kafka" % kafkaVersion classifier ("test")) % "test"
        )
      )
  ) dependsOn (streaming % "test->test; provided")

  lazy val services_full = CrossProject("gearpump-services", file("services"), CrossType.Full).
    settings(
      publish := {},
      publishLocal := {}
    )

  val distDashboardDirectory = s"${distDirectory}/target/pack/dashboard/views/scalajs"

  // ScalaJs project need to be build seperately.
  // sbt "project gearpump-servicesJS" compile
  lazy val serviceJS: Project = services_full.js.settings(serviceJSSettings: _*)

  lazy val services: Project = services_full.jvm.
    settings(serviceJvmSettings: _*)
    .settings(compile in Compile <<= (compile in Compile))
    .dependsOn(streaming % "test->test;compile->compile",
      daemon % "test->test;compile->compile;provided")

  lazy val serviceJvmSettings = commonSettings ++ noPublish ++ Seq(
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-http-testkit" % akkaVersion % "test",
      "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
      "com.lihaoyi" %% "upickle" % upickleVersion,
      "com.softwaremill.akka-http-session" %% "core" % "0.2.5",
      "com.typesafe.akka" %% "akka-http-spray-json-experimental" % akkaVersion,
      "com.github.scribejava" % "scribejava-apis" % "2.4.0",
      "com.ning" % "async-http-client" % "1.9.33",
      "org.webjars" % "angularjs" % "1.4.9",

      // angular 1.5 breaks ui-select, but we need ng-touch 1.5
      "org.webjars.npm" % "angular-touch" % "1.5.0",
      "org.webjars" % "angular-ui-router" % "0.2.15",
      "org.webjars" % "bootstrap" % "3.3.6",
      "org.webjars" % "d3js" % "3.5.6",
      "org.webjars" % "momentjs" % "2.10.6",
      "org.webjars" % "lodash" % "3.10.1",
      "org.webjars" % "font-awesome" % "4.5.0",
      "org.webjars" % "jquery" % "2.2.0",
      "org.webjars" % "jquery-cookie" % "1.4.1",
      "org.webjars.bower" % "angular-loading-bar" % "0.8.0",
      "org.webjars.bower" % "angular-smart-table" % "2.1.6",
      "org.webjars.bower" % "angular-motion" % "0.4.3",
      "org.webjars.bower" % "bootstrap-additions" % "0.3.1",
      "org.webjars.bower" % "angular-strap" % "2.3.5",
      "org.webjars.npm" % "ui-select" % "0.14.2",
      "org.webjars.bower" % "ng-file-upload" % "5.0.9",
      "org.webjars.bower" % "vis" % "4.7.0",
      "org.webjars.bower" % "clipboard.js" % "0.1.1",
      "org.webjars.npm" % "dashing-deps" % "0.1.2",
      "org.webjars.npm" % "dashing" % "0.4.8"
    ).map(_.exclude("org.scalamacros", "quasiquotes_2.10"))
      .map(_.exclude("org.scalamacros", "quasiquotes_2.10.3")))

  lazy val serviceJSSettings = Seq(
    scalaVersion := scalaVersionNumber,
    crossScalaVersions := crossScalaVersionNumbers,
    checksums := Seq(""),
    requiresDOM := true,
    libraryDependencies ++= Seq(
      "com.lihaoyi" %%% "upickle" % upickleVersion,
      "com.lihaoyi" %%% "utest" % "0.3.1"
    ),
    scalaJSStage in Global := FastOptStage,
    testFrameworks += new TestFramework("utest.runner.Framework"),
    requiresDOM := true,
    persistLauncher in Compile := false,
    persistLauncher in Test := false,
    skip in packageJSDependencies := false,
    scoverage.ScoverageSbtPlugin.ScoverageKeys.coverageExcludedPackages :=
      ".*gearpump\\.dashboard.*",

    copySharedSourceFiles := {
      // scalastyle:off println
      println(s"Copy shared source code to project services...")
      // scalastyle:on println
    },

    artifactPath in fastOptJS in Compile :=
      new java.io.File(distDashboardDirectory, moduleName.value + "-fastopt.js"),

    fastOptJS in Compile <<= (fastOptJS in Compile).dependsOn(copySharedSourceFiles),

    relativeSourceMaps := true,
    jsEnv in Test := new PhantomJS2Env(scalaJSPhantomJSClassLoader.value))

  lazy val akkastream = Project(
    id = "gearpump-experiments-akkastream",
    base = file("experiments/akkastream"),
    settings = commonSettings ++ noPublish ++ myAssemblySettings ++
      Seq(
        libraryDependencies ++= Seq(
          "org.json4s" %% "json4s-jackson" % "3.2.11"
        ),
        mainClass in(Compile, packageBin) := Some("akka.stream.gearpump.example.Test")
      )
  ) dependsOn(streaming % "test->test; provided", daemon % "test->test; provided")

  lazy val storm = Project(
    id = "gearpump-experiments-storm",
    base = file("experiments/storm"),
    settings = commonSettings ++ noPublish ++
      Seq(
        libraryDependencies ++= Seq(
          "commons-io" % "commons-io" % commonsIOVersion,
          "org.apache.storm" % "storm-core" % stormVersion
            exclude("clj-stacktrace", "clj-stacktrace")
            exclude("ch.qos.logback", "logback-classic")
            exclude("ch.qos.logback", "logback-core")
            exclude("clj-time", "clj-time")
            exclude("clout", "clout")
            exclude("compojure", "compojure")
            exclude("hiccup", "hiccup")
            exclude("jline", "jline")
            exclude("joda-time", "joda-time")
            exclude("org.clojure", "core.incubator")
            exclude("org.clojure", "math.numeric-tower")
            exclude("org.clojure", "tools.logging")
            exclude("org.clojure", "tools.cli")
            exclude("org.clojure", "tools.macro")
            exclude("org.mortbay.jetty", "jetty-util")
            exclude("org.mortbay.jetty", "jetty")
            exclude("org.ow2.asm", "asm")
            exclude("org.slf4j", "log4j-over-slf4j")
            exclude("ring", "ring-core")
            exclude("ring", "ring-devel")
            exclude("ring", "ring-jetty-adapter")
            exclude("ring", "ring-servlet")
        )
      )
  ) dependsOn (streaming % "test->test; compile->compile")

  lazy val yarn = Project(
    id = "gearpump-experiments-yarn",
    base = file("experiments/yarn"),
    settings = commonSettings ++ noPublish ++
      Seq(
        libraryDependencies ++= Seq(
          "org.apache.hadoop" % "hadoop-hdfs" % clouderaVersion,
          "org.apache.hadoop" % "hadoop-common" % clouderaVersion,
          "org.apache.hadoop" % "hadoop-yarn-api" % clouderaVersion,
          "org.apache.hadoop" % "hadoop-yarn-client" % clouderaVersion,
          "org.apache.hadoop" % "hadoop-yarn-common" % clouderaVersion,
          "commons-httpclient" % "commons-httpclient" % commonsHttpVersion,
          "org.apache.hadoop" % "hadoop-mapreduce-client-core" % clouderaVersion,
          "org.apache.hadoop" % "hadoop-yarn-server-resourcemanager" % clouderaVersion % "provided",
          "org.apache.hadoop" % "hadoop-yarn-server-nodemanager" % clouderaVersion % "provided"
        )
      )
  ) dependsOn(services % "test->test;compile->compile", core % "provided")

  lazy val external_hbase = Project(
    id = "gearpump-external-hbase",
    base = file("external/hbase"),
    settings = commonSettings ++ javadocSettings  ++
      Seq(
        resolvers ++= Seq(
          "cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos"
        )
      ) ++
      Seq(
        libraryDependencies ++= Seq(
          "org.apache.hadoop" % "hadoop-common" % clouderaVersion % "provided",
          "org.apache.hadoop" % "hadoop-hdfs" % clouderaVersion % "provided",
          "org.apache.hadoop" % "hadoop-mapreduce-client-core" % clouderaVersion % "provided",
          "org.codehaus.jackson" % "jackson-core-asl" % "1.9.13" % "provided",
          "org.codehaus.jackson" % "jackson-mapper-asl" % "1.9.13" % "provided",
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
          "org.apache.hbase" % "hbase-client" % clouderaHBaseVersion,
          "org.apache.hbase" % "hbase-common" % clouderaHBaseVersion
            exclude("com.github.stephenc.findbugs", "findbugs-annotations")
            exclude("com.google.guava", "guava")
            exclude("commons-codec", "commons-codec")
            exclude("commons-collections", "commons-collections")
            exclude("commons-io", "commons-io")
            exclude("commons-lang", "commons-lang")
            exclude("commons-logging", "commons-logging")
            exclude("junit", "junit")
            exclude("log4j", "log4j")
        )
      )
  ) dependsOn (streaming % "test->test; provided")

  lazy val external_monoid = Project(
    id = "gearpump-external-monoid",
    base = file("external/monoid"),
    settings = commonSettings ++ javadocSettings  ++
      Seq(
        libraryDependencies ++= Seq(
          "com.twitter" %% "algebird-core" % algebirdVersion
        )
      )
  ) dependsOn (streaming % "provided")

  lazy val external_serializer = Project(
    id = "gearpump-external-serializer",
    base = file("external/serializer"),
    settings = commonSettings ++ javadocSettings  ++
      Seq(
        libraryDependencies ++= Seq(
          "com.twitter" %% "chill-bijection" % chillVersion
            exclude("com.esotericsoftware.kryo", "kyro")
            exclude("com.esotericsoftware.minlog", "minlog")
        )
      )
  ) dependsOn (streaming % "provided")

  lazy val external_hadoopfs = Project(
    id = "gearpump-external-hadoopfs",
    base = file("external/hadoopfs"),
    settings = commonSettings ++ javadocSettings ++
      Seq(
        libraryDependencies ++= Seq(
          "org.apache.hadoop" % "hadoop-common" % clouderaVersion % "provided",
          "org.apache.hadoop" % "hadoop-hdfs" % clouderaVersion % "provided"
        )
      )
  ) dependsOn (streaming % "test->test; provided")
}
