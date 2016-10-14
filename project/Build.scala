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
import BuildShaded._
import de.johoop.jacoco4sbt.JacocoPlugin.jacoco
import org.scalajs.sbtplugin.ScalaJSPlugin.autoImport._
import sbt.Keys._
import sbt._
import Pack.packProject
import org.scalajs.sbtplugin.cross.CrossProject
import sbtassembly.AssemblyPlugin.autoImport._
import sbtunidoc.Plugin.UnidocKeys._
import sbtunidoc.Plugin._
import xerial.sbt.Sonatype._

object Build extends sbt.Build {

  val copySharedSourceFiles = TaskKey[Unit]("copied shared services source code")

  val akkaVersion = "2.4.3"
  val apacheRepo = "https://repository.apache.org/"
  val hadoopVersion = "2.6.0"
  val hbaseVersion = "1.0.0"
  val commonsHttpVersion = "3.1"
  val commonsLoggingVersion = "1.1.3"
  val commonsLangVersion = "2.6"
  val commonsIOVersion = "2.4"
  val dataReplicationVersion = "0.7"
  val upickleVersion = "0.3.4"
  val junitVersion = "4.12"
  val kafkaVersion = "0.8.2.1"
  val stormVersion = "0.10.0"
  val slf4jVersion = "1.7.7"

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
        "apache-repo" at "https://repository.apache.org/content/repositories",
        "maven1-repo" at "http://repo1.maven.org/maven2",
        "maven2-repo" at "http://mvnrepository.com/artifact",
        "sonatype" at "https://oss.sonatype.org/content/repositories/releases",
        "bintray/non" at "http://dl.bintray.com/non/maven",
        "clockfly" at "http://dl.bintray.com/clockfly/maven",
        "clojars" at "http://clojars.org/repo"
      )
      // ,addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0-M5" cross CrossVersion.full)
    ) ++
    Seq(
      scalaVersion := scalaVersionNumber,
      crossScalaVersions := crossScalaVersionNumbers,
      organization := "org.apache.gearpump",
      useGpg := false,
      pgpSecretRing := file("./secring.asc"),
      pgpPublicRing := file("./pubring.asc"),
      scalacOptions ++= Seq("-Yclosure-elim", "-Yinline"),
      publishMavenStyle := true,

      pgpPassphrase := Option(System.getenv().get("PASSPHRASE")).map(_.toArray),
      credentials += Credentials(
        "Sonatype Nexus Repository Manager",
        "repository.apache.org",
        System.getenv().get("SONATYPE_USERNAME"),
        System.getenv().get("SONATYPE_PASSWORD")),

      pomIncludeRepository := { _ => false },

      publishTo := {
        if (isSnapshot.value) {
          Some("snapshots" at apacheRepo + "content/repositories/snapshots")
        } else {
          Some("releases" at apacheRepo + "content/repositories/releases")
        }
      },

      publishArtifact in Test := true,

      pomExtra := {
        <url>https://github.com/apache/incubator-gearpump</url>
          <licenses>
            <license>
              <name>Apache 2</name>
              <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
            </license>
          </licenses>
          <scm>
            <connection>scm:git://git.apache.org/incubator-gearpump.git</connection>
            <developerConnection>scm:git:git@github.com:apache/incubator-gearpump</developerConnection>
            <url>github.com/apache/incubator-gearpump</url>
          </scm>
          <developers>
            <developer>
              <id>gearpump</id>
              <name>Gearpump Team</name>
              <url>http://gearpump.incubator.apache.org/community.html#who-we-are</url>
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

  val coreDependencies = Seq(
    libraryDependencies ++= Seq(
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.slf4j" % "slf4j-log4j12" % slf4jVersion,
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

      "com.typesafe.akka" %% "akka-cluster" % akkaVersion,
      "com.typesafe.akka" %% "akka-cluster-tools" % akkaVersion,
      "commons-logging" % "commons-logging" % commonsLoggingVersion,
      "com.typesafe.akka" %% "akka-distributed-data-experimental" % akkaVersion,
      "com.typesafe.akka" %% "akka-actor" % akkaVersion,
      "com.typesafe.akka" %% "akka-agent" % akkaVersion,
      "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
      "com.typesafe.akka" %% "akka-kernel" % akkaVersion,
      "com.typesafe.akka" %% "akka-http-experimental" % akkaVersion,
      "com.typesafe.akka" %% "akka-http-spray-json-experimental" % akkaVersion,
      "org.scala-lang" % "scala-reflect" % scalaVersionNumber,
      "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.4",
      "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test",
      "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
      "org.scalacheck" %% "scalacheck" % scalaCheckVersion % "test",
      "org.mockito" % "mockito-core" % mockitoVersion % "test",
      "junit" % "junit" % junitVersion % "test"
    ),

    unmanagedJars in Compile ++= Seq(
      getShadedJarFile(shaded_metrics_graphite.id, version.value),
      getShadedJarFile(shaded_guava.id, version.value),
      getShadedJarFile(shaded_akka_kryo.id, version.value)
    )
  )

  lazy val javadocSettings = Seq(
    addCompilerPlugin("com.typesafe.genjavadoc" %% "genjavadoc-plugin" %
      "0.9" cross CrossVersion.full),
    scalacOptions += s"-P:genjavadoc:out=${target.value}/java"
  )

  val myAssemblySettings = Seq(
    test in assembly := {},
    assemblyOption in assembly ~= {
      _.copy(includeScala = false)
    },
    assemblyJarName in assembly := {
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
    external_hadoopfs)

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
    packages
      .map(_.filterNot(_.getName.contains("$")))
      .map(_.filterNot(_.getCanonicalPath.contains("akka")))
  }

  private def addShadedDeps(deps: Seq[xml.Node], node: xml.Node): xml.Node = {
    node match {
      case elem: xml.Elem =>
        val child = if (elem.label == "dependencies") {
          elem.child ++ deps
        } else {
          elem.child.map(addShadedDeps(deps, _))
        }
        xml.Elem(elem.prefix, elem.label, elem.attributes, elem.scope, false, child: _*)
      case _ =>
        node
    }
  }

  lazy val root = Project(
    id = "gearpump",
    base = file("."),
    settings = commonSettings ++ noPublish ++ gearpumpUnidocSetting)
      .aggregate(shaded, core, streaming, services, external_kafka, external_monoid,
      external_serializer, examples, storm, yarn, external_hbase, gearpumpHadoop, packProject,
      external_hadoopfs, integration_test).settings(Defaults.itSettings: _*)
      .disablePlugins(sbtassembly.AssemblyPlugin)

  lazy val core = Project(
    id = "gearpump-core",
    base = file("core"),
    settings = commonSettings ++ javadocSettings ++ coreDependencies ++ Seq(
      pomPostProcess := {
        (node: xml.Node) => addShadedDeps(List(
          getShadedDepXML(organization.value, shaded_akka_kryo.id, version.value),
          getShadedDepXML(organization.value, shaded_guava.id, version.value),
          getShadedDepXML(organization.value, shaded_metrics_graphite.id, version.value)), node)
      }
    )).disablePlugins(sbtassembly.AssemblyPlugin)


  lazy val cgroup = Project(
    id = "gearpump-experimental-cgroup",
    base = file("experiments/cgroup"),
    settings = commonSettings ++ noPublish)
      .dependsOn (core % "test->test; compile->compile")
      .disablePlugins(sbtassembly.AssemblyPlugin)

  lazy val streaming = Project(
    id = "gearpump-streaming",
    base = file("streaming"),
    settings = commonSettings ++ javadocSettings ++ Seq(
      unmanagedJars in Compile ++= Seq(
        getShadedJarFile(shaded_gs_collections.id, version.value)
      ),

      pomPostProcess := {
        (node: xml.Node) => addShadedDeps(List(
          getShadedDepXML(organization.value, shaded_gs_collections.id, version.value)), node)
      }
    ))
    .dependsOn(core % "test->test; compile->compile", shaded_gs_collections)
    .disablePlugins(sbtassembly.AssemblyPlugin)

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
      ))
      .dependsOn (streaming % "test->test; provided")
      .disablePlugins(sbtassembly.AssemblyPlugin)

  lazy val services_full = CrossProject("gearpump-services", file("services"), CrossType.Full).
    settings(
      publish := {},
      publishLocal := {}
    ).disablePlugins(sbtassembly.AssemblyPlugin)

  val distDashboardDirectory = s"${distDirectory}/target/pack/dashboard/views/scalajs"

  // ScalaJs project need to be build seperately.
  // sbt "project gearpump-servicesJS" compile
  lazy val serviceJS: Project = services_full.js.settings(serviceJSSettings: _*)

  lazy val services: Project = services_full.jvm.
    settings(serviceJvmSettings: _*)
    .settings(compile in Compile <<= (compile in Compile))
    .dependsOn(streaming % "test->test;compile->compile")

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
      ))
      .dependsOn(streaming % "test->test; provided")

  lazy val redis = Project(
    id = "gearpump-experiments-redis",
    base = file("experiments/redis"),
    settings = commonSettings ++ noPublish ++
      Seq(
        libraryDependencies ++= Seq(
          "redis.clients" % "jedis" % "2.9.0"
        )
      )
  ).dependsOn(streaming % "test->test; provided")

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
            exclude("org.apache.logging.log4j", "log4j-slf4j-impl")
            exclude("ring", "ring-core")
            exclude("ring", "ring-devel")
            exclude("ring", "ring-jetty-adapter")
            exclude("ring", "ring-servlet")
        )
      ))
      .dependsOn (streaming % "test->test; compile->compile")
      .disablePlugins(sbtassembly.AssemblyPlugin)

  lazy val gearpumpHadoop = Project(
    id = "gearpump-hadoop",
    base = file("gearpump-hadoop"),
    settings = commonSettings ++ noPublish ++
      Seq(
        libraryDependencies ++= Seq(
          "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion,
          "org.apache.hadoop" % "hadoop-common" % hadoopVersion
        )
      )
  ).dependsOn(core % "compile->compile").disablePlugins(sbtassembly.AssemblyPlugin)

  lazy val yarn = Project(
    id = "gearpump-experiments-yarn",
    base = file("experiments/yarn"),
    settings = commonSettings ++ noPublish ++
      Seq(
        libraryDependencies ++= Seq(
          "org.apache.hadoop" % "hadoop-yarn-api" % hadoopVersion,
          "org.apache.hadoop" % "hadoop-yarn-client" % hadoopVersion,
          "org.apache.hadoop" % "hadoop-yarn-common" % hadoopVersion,
          "commons-httpclient" % "commons-httpclient" % commonsHttpVersion,
          "org.apache.hadoop" % "hadoop-mapreduce-client-core" % hadoopVersion,
          "org.apache.hadoop" % "hadoop-yarn-server-resourcemanager" % hadoopVersion % "provided",
          "org.apache.hadoop" % "hadoop-yarn-server-nodemanager" % hadoopVersion % "provided"
        )
      ))
      .dependsOn(services % "test->test;compile->compile",
        core % "provided", gearpumpHadoop).disablePlugins(sbtassembly.AssemblyPlugin)

  lazy val external_hbase = Project(
    id = "gearpump-external-hbase",
    base = file("external/hbase"),
    settings = commonSettings ++ javadocSettings  ++
      Seq(
        libraryDependencies ++= Seq(
          "org.apache.hadoop" % "hadoop-common" % hadoopVersion % "provided",
          "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion % "provided",
          "org.apache.hadoop" % "hadoop-mapreduce-client-core" % hadoopVersion % "provided",
          "org.codehaus.jackson" % "jackson-core-asl" % "1.9.13" % "provided",
          "org.codehaus.jackson" % "jackson-mapper-asl" % "1.9.13" % "provided",
          "org.apache.hbase" % "hbase-client" % hbaseVersion
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
          "org.apache.hbase" % "hbase-client" % hbaseVersion,
          "org.apache.hbase" % "hbase-common" % hbaseVersion
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
      ))
      .dependsOn (streaming % "test->test; provided")
      .disablePlugins(sbtassembly.AssemblyPlugin)

  lazy val external_monoid = Project(
    id = "gearpump-external-monoid",
    base = file("external/monoid"),
    settings = commonSettings ++ javadocSettings  ++
      Seq(
        libraryDependencies ++= Seq(
          "com.twitter" %% "algebird-core" % algebirdVersion
        )
      ))
      .dependsOn (streaming % "provided")
      .disablePlugins(sbtassembly.AssemblyPlugin)

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
      ))
      .dependsOn (streaming % "provided")
      .disablePlugins(sbtassembly.AssemblyPlugin)

  lazy val external_hadoopfs = Project(
    id = "gearpump-external-hadoopfs",
    base = file("external/hadoopfs"),
    settings = commonSettings ++ javadocSettings ++
      Seq(
        libraryDependencies ++= Seq(
          "org.apache.hadoop" % "hadoop-common" % hadoopVersion % "provided",
          "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion % "provided"
        )
      ))
      .dependsOn (streaming % "test->test; provided")
      .disablePlugins(sbtassembly.AssemblyPlugin)
}
