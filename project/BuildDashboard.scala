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

import BuildGearpump._
import Dependencies._
import Docs._
import sbt._
import sbt.Keys._

object BuildDashboard {

  lazy val serviceJvmSettings = commonSettings ++ noPublish ++ myAssemblySettings ++
    javadocSettings ++ Seq(
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