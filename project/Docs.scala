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

import BuildGearpump.{core, streaming}
import BuildExternals.externals
import sbt.Keys._
import sbt._
import sbtunidoc.Plugin.UnidocKeys._
import sbtunidoc.Plugin._

object Docs extends sbt.Build {
  lazy val javadocSettings = Seq(
    addCompilerPlugin(
      "com.typesafe.genjavadoc" %% "genjavadoc-plugin" % "0.9" cross CrossVersion.full),
    scalacOptions += s"-P:genjavadoc:out=${target.value}/java"
  )

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

  private lazy val projectsWithDoc = {
    val projects: Seq[ProjectReference] = Seq[ProjectReference](
      core,
      streaming
    ) ++ externals

    inProjects(projects: _*)
  }

  private def ignoreUndocumentedPackages(packages: Seq[Seq[File]]): Seq[Seq[File]] = {
    packages
      .map(_.filterNot(_.getName.contains("$")))
      .map(_.filterNot(_.getCanonicalPath.contains("akka")))
  }
}
