import sbt._
import sbt.Keys._
import xerial.sbt.Pack._

name := "gearpump-core"

version := "0.1"

organization := "com.github.intel-hadoop"

val scalaVersionNumber = "2.10.4"

scalaVersion := scalaVersionNumber

scalacOptions ++= Seq(
  "-Yclosure-elim",
  "-Yinline"
)

crossScalaVersions := Seq("2.10.4", "2.11.2")

packSettings

packMain := Map("local" -> "org.apache.gearpump.cluster.main.Local",
                "master" -> "org.apache.gearpump.cluster.main.Master",
                "worker" -> "org.apache.gearpump.cluster.main.Worker",
                "shell" -> "org.apache.gearpump.cluster.main.Shell")
				
packResourceDir += (baseDirectory.value / "src/main/resources" -> "conf")		
		
packExtraClasspath := Map("local" -> Seq("${PROG_HOME}/conf"),
                          "master" -> Seq("${PROG_HOME}/conf"),
			  "worker" -> Seq("${PROG_HOME}/conf"),
			  "shell" -> Seq("${PROG_HOME}/conf"))

pack <<= pack.dependsOn(publishLocal)

resolvers ++= Seq(
  "maven-repo" at "http://repo.maven.apache.org/maven2",
  "maven1-repo" at "http://repo1.maven.org/maven2",
  "maven2-repo" at "http://mvnrepository.com",
  "apache-repo" at "https://repository.apache.org/content/repositories/releases",
  "sonatype" at "https://oss.sonatype.org/content/repositories/releases",
  "clockfly" at "http://dl.bintray.com/clockfly/maven",
  "patrik" at "http://dl.bintray.com/patriknw/maven"
)

parallelExecution in Test := false

val akkaVersion = "2.3.5"
val kyroVersion = "0.3.2"
val codahaleVersion = "3.0.2"
val commonsLangVersion = "3.3.2"
val commonsHttpVersion = "3.1"
val guavaVersion = "14.0.1"
val jettyVersion = "8.1.14.v20131031"
val jgraphtVersion = "0.9.0"
val slf4jVersion = "1.7.5"
val gearpumpExamplesVersion = "0.1"

libraryDependencies ++= Seq(
  ("org.eclipse.jetty" % "jetty-plus" % jettyVersion).
  exclude("org.eclipse.jetty.orbit", "javax.servlet").
  exclude("org.eclipse.jetty.orbit", "javax.transaction").
  exclude("org.eclipse.jetty.orbit", "javax.mail.glassfish").
  exclude("org.eclipse.jetty.orbit", "javax.activation"),
  "org.jgrapht" % "jgrapht-core" % jgraphtVersion,
  ("org.eclipse.jetty" % "jetty-server" % jettyVersion).
  exclude("org.eclipse.jetty.orbit", "javax.transaction").
  exclude("org.eclipse.jetty.orbit", "javax.mail.glassfish").
  exclude("org.eclipse.jetty.orbit", "javax.activation"),
  "com.google.guava" % "guava" % guavaVersion,
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
  "com.github.patriknw" %% "akka-data-replication" % "0.4",
  "com.github.intel-hadoop" %% "gearpump-examples" % gearpumpExamplesVersion
)
