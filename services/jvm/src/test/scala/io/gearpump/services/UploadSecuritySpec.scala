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

package io.gearpump.services

import io.gearpump.jarstore.FileDirective
import java.nio.file.Files
import org.apache.pekko.http.scaladsl.model.{HttpEntity, Multipart}
import org.apache.pekko.http.scaladsl.server.Directives._
import org.apache.pekko.http.scaladsl.testkit.ScalatestRouteTest
import org.apache.pekko.stream.scaladsl.Source
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class UploadSecuritySpec extends AnyFlatSpec with Matchers with ScalatestRouteTest {
  private val route = FileDirective.uploadFile { form =>
    val path = form.getFileInfo("jar").get.file.toPath
    assert(Files.exists(path))
    complete(path.toString)
  }

  private def temporaryFiles(): Set[String] = {
    import scala.jdk.CollectionConverters._
    val files = Files.list(java.nio.file.Paths.get(System.getProperty("java.io.tmpdir")))
    try files.iterator().asScala.filter(_.getFileName.toString.startsWith("gearpump-upload-"))
      .map(_.toString).toSet finally files.close()
  }

  private def part(name: String, value: String, file: Boolean = false) = {
    Multipart.FormData.BodyPart.Strict(name, HttpEntity(value),
      if (file) Map("filename" -> "uploaded.jar") else Map.empty)
  }

  it should "remove successful upload temporary files after route completion" in {
    val form = Multipart.FormData(Source.single(part("jar", "bytes", file = true)))
    Post("/", form) ~> route ~> check {
      assert(status.intValue() == 200)
      assert(!Files.exists(java.nio.file.Paths.get(responseAs[String])))
    }
  }

  it should "omit empty file parts and clean up their temporary files" in {
    val before = temporaryFiles()
    val emptyRoute = FileDirective.uploadFile { form =>
      assert(form.getFileInfo("jar").isEmpty)
      assert(form.getFileInfo("configfile").isEmpty)
      complete("empty")
    }
    val form = Multipart.FormData(Source(List(part("jar", "", file = true),
      part("configfile", "", file = true))))
    Post("/", form) ~> emptyRoute ~> check {
      assert(status.intValue() == 200)
      assert(responseAs[String] == "empty")
      assert((temporaryFiles() -- before).isEmpty)
    }
  }

  it should "reject oversized text fields and duplicate or unexpected fields" in {
    val oversized = Multipart.FormData(Source.single(
      part("args", "x" * (FileDirective.MaxFieldBytes + 1))))
    Post("/", oversized) ~> route ~> check { assert(status.intValue() == 413) }
    val before = temporaryFiles()
    Seq(List(part("jar", "first", file = true), part("jar", "second", file = true)),
      List(part("unknown", "value"))).foreach { parts =>
      Post("/", Multipart.FormData(Source(parts))) ~> route ~> check {
        assert(status.intValue() == 400)
      }
    }
    assert((temporaryFiles() -- before).isEmpty)
  }

  it should "remove temporary files when route construction throws" in {
    var file: java.nio.file.Path = null
    val failing = handleExceptions(org.apache.pekko.http.scaladsl.server.ExceptionHandler {
      case _: IllegalStateException =>
        complete(org.apache.pekko.http.scaladsl.model.StatusCodes.InternalServerError)
    }) {
      FileDirective.uploadFile { form =>
        file = form.getFileInfo("jar").get.file.toPath
        throw new IllegalStateException("backend failed")
      }
    }
    Post("/", Multipart.FormData(Source.single(part("jar", "bytes", file = true)))) ~>
      failing ~> check {
        assert(status.intValue() == 500)
        assert(file != null && !Files.exists(file))
      }
  }
}
