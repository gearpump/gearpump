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

import Dependencies._
import sbt._
import sbt.Keys._
import sbtunidoc.BaseUnidocPlugin.autoImport.{unidoc, unidocAllSources, unidocProjectFilter}
import sbtunidoc.JavaUnidocPlugin.autoImport.JavaUnidoc
import sbtunidoc.ScalaUnidocPlugin.autoImport.ScalaUnidoc

object Docs {
  lazy val javadocSettings = Seq(
    scalacOptions += s"-P:genjavadoc:out=${target.value}/java",
    scalacOptions -= "-Xfatal-warnings"
  )

  def gearpumpUnidocSetting(projects: ProjectReference*) = Seq(
    unidocProjectFilter in(ScalaUnidoc, unidoc) := inProjects(projects: _*),

    unidocProjectFilter in(JavaUnidoc, unidoc) := inProjects(projects: _*),

    unidocAllSources in(ScalaUnidoc, unidoc) := {
     ignoreUndocumentedPackages((unidocAllSources in(ScalaUnidoc, unidoc)).value)
    },

    // Skip class names containing $ and some internal packages in Javadocs
    unidocAllSources in(JavaUnidoc, unidoc) := {
     ignoreUndocumentedPackages((unidocAllSources in(JavaUnidoc, unidoc)).value)
    },

    libraryDependencies ++= compilerDependencies
  )

  private def ignoreUndocumentedPackages(packages: Seq[Seq[File]]): Seq[Seq[File]] = {
    packages
      .map(_.filterNot(_.getName.contains("$")))
      .map(_.filterNot(_.getCanonicalPath.contains("akka")))
  }
}
