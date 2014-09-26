import sbt._
import sbt.Keys._
import xerial.sbt.Pack._

name := "gearpump-rest"

version := "0.1"

organization := "com.github.intel-hadoop"

val scalaVersionNumber = "2.11.2"

scalaVersion := scalaVersionNumber

scalacOptions ++= Seq(
  "-Yclosure-elim",
  "-Yinline"
)

packSettings

packMain := Map("rest" -> "org.apache.gearpump.cluster.main.Rest")
				
packResourceDir += (baseDirectory.value / "src/main/resources" -> "conf")		
		
packExtraClasspath := Map("rest" -> Seq("${PROG_HOME}/conf"))

resolvers ++= Seq(
  "maven-repo" at "http://repo.maven.apache.org/maven2",
  "maven1-repo" at "http://repo1.maven.org/maven2",
  "maven2-repo" at "http://mvnrepository.com",
  "apache-repo" at "https://repository.apache.org/content/repositories/releases",
  "sonatype" at "https://oss.sonatype.org/content/repositories/releases",
  "spray repo" at "http://repo.spray.io",
  "webjars" at "http://webjars.github.com/m2"
)

parallelExecution in Test := false

val gearpumpCoreVersion = "0.1"

libraryDependencies ++= Seq(
  "com.github.intel-hadoop" %% "gearpump-core" % gearpumpCoreVersion
)
