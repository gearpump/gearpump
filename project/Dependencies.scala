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

  val crossScalaVersionNumbers = Seq("2.12.7")
  val scalaVersionNumber = crossScalaVersionNumbers.last
  val pekkoVersion = "1.4.0"
  val pekkoHttpVersion = "1.3.0"
  val pekkoHttpSessionVersion = "0.7.1"
  val hadoopVersion = "3.1.1"
  val commonsHttpVersion = "3.1"
  val commonsLoggingVersion = "1.1.3"
  val commonsLangVersion = "2.6"
  val commonsIOVersion = "2.4"
  val dataReplicationVersion = "0.7"
  val upickleVersion = "0.7.1"
  val junitVersion = "4.12"
  val jsonSimpleVersion = "1.1"
  val slf4jVersion = "1.7.16"
  val guavaVersion = "16.0.1"
  val codahaleVersion = "3.0.2"
  val pekkoKryoVersion = "1.4.0"
  val gsCollectionsVersion = "6.2.0"
  val sprayVersion = "1.3.2"
  val sprayJsonVersion = "1.3.1"
  val scalaTestVersion = "3.0.5"
  val scalaCheckVersion = "1.14.0"
  val mockitoVersion = "1.10.17"
  val bijectionVersion = "0.8.0"
  val scalazVersion = "7.1.1"
  val algebirdVersion = "0.13.5"
  val chillVersion = "0.6.0"
  val jedisVersion = "2.9.0"
  val rabbitmqVersion = "3.5.3"
  val calciteVersion = "1.12.0"
  val silencerVersion = "1.3.1"

  val annotationDependencies = Seq(
    // work around for compiler warnings like
    // "Class javax.annotation.CheckReturnValue not found - continuing with a stub"
    // see https://issues.scala-lang.org/browse/SI-8978
    // marked as "provided" to be excluded from assembling
    "com.google.code.findbugs" % "jsr305" % "3.0.2" % "provided"
  )

  val compilerDependencies = Seq(
    compilerPlugin("com.github.ghik" %% "silencer-plugin" % silencerVersion),
    "com.github.ghik" %% "silencer-lib" % silencerVersion % Provided
  )

  val coreDependencies = Seq(
    libraryDependencies ++= Seq(
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.slf4j" % "slf4j-log4j12" % slf4jVersion,
      "commons-lang" % "commons-lang" % commonsLangVersion,

      /**
       * Overrides Netty version 3.10.3.Final used by the original Akka 2.4.2 stack to
       * work around a Netty hang issue
       * (https://github.com/gearpump/gearpump/issues/2020)
       *
       * Akka 2.4.2 used Netty 3.10.3.Final by default, which has a serious issue that can hang
       * the network. The same issue also happens in version range (3.10.0.Final, 3.10.5.Final)
       * Netty 3.10.6.Final have this issue fixed, however, we find there is a 20% performance
       * drop. So we decided to downgrade netty to 3.8.0.Final (the same version used in
       * Akka 2.3.12).
       *
       * @see https://github.com/gearpump/gearpump/pull/2017 for more discussions.
       */
      "io.netty" % "netty" % "3.8.0.Final",
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
}
