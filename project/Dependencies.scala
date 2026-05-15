/*
 * Licensed under the Apache License, Version 2.0 (the
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

import sbt._
import sbt.Keys._

object Dependencies {

  val crossScalaVersionNumbers = Seq("2.13.18")
  val scalaVersionNumber = crossScalaVersionNumbers.last
  val pekkoVersion = "1.6.0"
  val pekkoHttpVersion = "1.3.0"
  val pekkoHttpSessionVersion = "0.7.1"
  val hadoopVersion = "3.1.4"
  val commonsHttpVersion = "3.1"
  val commonsLoggingVersion = "1.3.6"
  val commonsLangVersion = "2.6"
  val commonsIOVersion = "2.4"
  val dataReplicationVersion = "0.7"
  val upickleVersion = "0.9.9"
  val junitVersion = "4.12"
  val junitInterfaceVersion = "0.13.3"
  val jsonSimpleVersion = "1.1"
  val slf4jVersion = "1.7.16"
  val slf4jSimpleVersion = "2.0.17"
  val guavaVersion = "33.6.0-jre"
  val codahaleVersion = "3.0.2"
  val pekkoKryoVersion = "1.5.1"
  val pekkoNettyVersion = "4.2.7.Final"
  val gsCollectionsVersion = "6.2.0"
  val sprayVersion = "1.3.2"
  val sprayJsonVersion = "1.3.1"
  val scalaTestVersion = "3.0.9"
  val scalaCheckVersion = "1.14.0"
  val mockitoVersion = "1.10.17"
  val beamVersion = "2.73.0"
  val snappyJavaVersion = "1.1.10.7"
  val bijectionVersion = "0.8.0"
  val scalazVersion = "7.1.1"
  val algebirdVersion = "0.13.5"
  val chillVersion = "0.6.0"
  val jedisVersion = "2.9.0"
  val rabbitmqVersion = "3.5.3"
  val calciteVersion = "1.12.0"
  val annotationDependencies = Seq(
    // work around for compiler warnings like
    // "Class javax.annotation.CheckReturnValue not found - continuing with a stub"
    // see https://issues.scala-lang.org/browse/SI-8978
    // marked as "provided" to be excluded from assembling
    "com.google.code.findbugs" % "jsr305" % "3.0.2" % "provided"
  )

  val compilerDependencies = Seq.empty

  val coreDependencies = Seq(
    libraryDependencies ++= Seq(
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.slf4j" % "slf4j-log4j12" % slf4jVersion,
      "commons-lang" % "commons-lang" % commonsLangVersion,

      /**
       * Gearpump's internal transport still uses the legacy Netty 3 API
       * (`org.jboss.netty.*`), so keep the original Netty 3 line on the classpath.
       */
      "io.netty" % "netty" % "3.8.0.Final",
      /**
       * Pekko classic remoting now expects Netty 4 (`io.netty.channel.Channel`) and publishes
       * those modules as optional dependencies, so add them explicitly for runtime/test startup.
       */
      "io.netty" % "netty-handler" % pekkoNettyVersion,
      "io.netty" % "netty-transport" % pekkoNettyVersion,
      "org.apache.pekko" %% "pekko-remote" % pekkoVersion
        exclude("io.netty", "netty"),

      "org.apache.pekko" %% "pekko-cluster" % pekkoVersion,
      "org.apache.pekko" %% "pekko-cluster-tools" % pekkoVersion,
      "commons-logging" % "commons-logging" % commonsLoggingVersion,
      "org.apache.pekko" %% "pekko-distributed-data" % pekkoVersion,
      "org.apache.pekko" %% "pekko-actor" % pekkoVersion,
      "org.apache.pekko" %% "pekko-slf4j" % pekkoVersion,
      "org.apache.pekko" %% "pekko-stream" % pekkoVersion,
      "org.apache.pekko" %% "pekko-http" % pekkoHttpVersion,
      "org.apache.pekko" %% "pekko-http-spray-json" % pekkoHttpVersion,
      "org.scala-lang" % "scala-reflect" % scalaVersionNumber,
      "io.altoo" %% "pekko-kryo-serialization" % pekkoKryoVersion,
      "com.google.guava" % "guava" % guavaVersion,
      "com.codahale.metrics" % "metrics-graphite" % codahaleVersion
        exclude("org.slf4j", "slf4j-api"),
      "com.codahale.metrics" % "metrics-jvm" % codahaleVersion
        exclude("org.slf4j", "slf4j-api"),

      "org.apache.pekko" %% "pekko-testkit" % pekkoVersion % "test",
      "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
      "org.scalacheck" %% "scalacheck" % scalaCheckVersion % "test",
      "org.mockito" % "mockito-core" % mockitoVersion % "test",
      "junit" % "junit" % junitVersion % "test"
    ) ++ annotationDependencies ++ compilerDependencies
  )

  val beamRunnerDependencies = Seq(
    libraryDependencies ++= Seq(
      "org.apache.beam" % "beam-sdks-java-core" % beamVersion,
      "org.apache.beam" % "beam-runners-core-java" % beamVersion,
      // Override Beam's transitive snappy-java so Java 17 tests work on macOS/aarch64.
      "org.xerial.snappy" % "snappy-java" % snappyJavaVersion,
      "org.slf4j" % "slf4j-simple" % slf4jSimpleVersion % "test",
      "junit" % "junit" % junitVersion % "test",
      "com.github.sbt" % "junit-interface" % junitInterfaceVersion % "test"
    ) ++ annotationDependencies
  )
}
