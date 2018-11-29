import BuildGearpump._
import Dependencies._
import org.scalajs.sbtplugin.cross.{CrossProject, CrossType}
import org.scalajs.sbtplugin.ScalaJSPlugin.autoImport._
import sbt._
import sbt.Keys._

object BuildDashboard extends sbt.Build {

  lazy val services: Project = services_full.jvm
    .settings(serviceJvmSettings: _*)
    .dependsOn(core % "provided", streaming % "test->test; provided")
  
  private lazy val services_full = CrossProject("gearpump-services", file("services"),
    CrossType.Full)
    .settings(
      publish := {},
      publishLocal := {}
    ).disablePlugins(sbtassembly.AssemblyPlugin)
  
  private lazy val serviceJvmSettings = commonSettings ++ noPublish ++ Seq(
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-http-testkit" % akkaHttpVersion % "test",
      "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
      "com.lihaoyi" %% "upickle" % upickleVersion,
      "com.softwaremill.akka-http-session" %% "core" % "0.3.0",
      "com.typesafe.akka" %% "akka-http-spray-json" % akkaHttpVersion,
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
      "org.webjars.bower" % "angular-loading-bar" % "0.8.0"
        exclude("org.webjars.bower", "angular"),
      "org.webjars.bower" % "angular-smart-table" % "2.1.6"
        exclude("org.webjars.bower", "angular"),
      "org.webjars.bower" % "angular-motion" % "0.4.3",
      "org.webjars.bower" % "bootstrap-additions" % "0.3.1",
      "org.webjars.bower" % "angular-strap" % "2.3.5"
        exclude("org.webjars.bower", "angular"),
      "org.webjars.npm" % "ui-select" % "0.14.2",
      "org.webjars.bower" % "ng-file-upload" % "5.0.9",
      "org.webjars.bower" % "vis" % "4.7.0",
      "org.webjars.bower" % "clipboard.js" % "0.1.1",
      "org.webjars.npm" % "dashing-deps" % "0.1.2",
      "org.webjars.npm" % "dashing" % "0.4.8"
    ).map(_.exclude("org.scalamacros", "quasiquotes_2.10"))
      .map(_.exclude("org.scalamacros", "quasiquotes_2.10.3")))
  
}