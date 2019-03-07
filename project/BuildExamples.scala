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
import sbt._
import sbt.Keys._
import sbtassembly.AssemblyPlugin.autoImport._

import scala.annotation.tailrec

object BuildExamples {

  def exampleSettings(className: String): Seq[Def.Setting[_]] = {
    commonSettings ++ noPublish ++ myAssemblySettings ++ Seq(
      mainClass in(Compile, packageBin) :=
        Some(className),
      target in assembly := {
        @tailrec
        def getExamplesPath(file: File): File = {
          if (file.getName.equals("examples")) {
            file
          } else {
            getExamplesPath(file.getParentFile)
          }
        }

        getExamplesPath(baseDirectory.value) / "target" /
          CrossVersion.binaryScalaVersion(scalaVersion.value)
      },
      assemblyMergeStrategy in assembly := {
        x =>
          // core and streaming dependencies are not marked as provided
          // such that the examples can be run with sbt or Intellij
          // so they have to be excluded manually here
          if (x.contains("examples")) {
            val oldStrategy = (assemblyMergeStrategy in assembly).value
            oldStrategy(x)
          } else {
            MergeStrategy.discard
          }
      }
    )
  }
}
