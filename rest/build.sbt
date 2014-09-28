import sbt._
import sbt.Keys._
import xerial.sbt.Pack._

name := "gearpump-rest"

version := "0.1"

organization := "com.github.intel-hadoop"

val scalaVersionNumber = "2.10.4"

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

val sprayVersion = "1.3.1"
val sprayJsonVersion = "1.2.6"

libraryDependencies ++= Seq(
  "io.spray" %%  "spray-can"       % sprayVersion,
  "io.spray" %%  "spray-routing"   % sprayVersion,
  "io.spray" %%  "spray-testkit"   % sprayVersion,
  "io.spray" %%  "spray-httpx"     % sprayVersion,
  "io.spray" %%  "spray-client"    % sprayVersion,
  "io.spray" %%  "spray-json"    % sprayJsonVersion,
  "com.gettyimages" %% "spray-swagger" % "0.4.3" excludeAll( ExclusionRule(organization = "org.json4s"), ExclusionRule(organization = "io.spray") ),
  "org.json4s" %% "json4s-jackson" % "3.2.10",
  "org.json4s" %% "json4s-native"   % "3.2.10",
  "org.webjars" % "swagger-ui" % "2.0.21",
  "com.github.intel-hadoop" %% "gearpump-core" % gearpumpCoreVersion
)
