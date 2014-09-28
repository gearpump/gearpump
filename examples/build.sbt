import sbt._
import sbt.Keys._
import xerial.sbt.Pack._

name := "gearpump-examples"

version := "0.1"

organization := "com.github.intel-hadoop"

val scalaVersionNumber = "2.10.4"

scalaVersion := scalaVersionNumber

scalacOptions ++= Seq(
  "-Yclosure-elim",
  "-Yinline"
)

packSettings

packMain := Map("sol" -> "org.apache.gearpump.streaming.examples.sol.SOL",
                "wordcount" -> "org.apache.gearpump.streaming.examples.wordcount.WordCount",
                "fsio" -> "org.apache.gearpump.streaming.examples.fsio.SequenceFileIO")
				
packResourceDir += (baseDirectory.value / "src/main/resources" -> "conf")		
		
packExtraClasspath := Map("sol" -> Seq("${PROG_HOME}/conf"),
                          "wordcount" -> Seq("${PROG_HOME}/conf"),
			  "fs" -> Seq("${PROG_HOME}/conf"))

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

val gearpumpCoreVersion = "0.1"

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-common" % "2.4.1",
  "com.github.intel-hadoop" %% "gearpump-core" % gearpumpCoreVersion
)
