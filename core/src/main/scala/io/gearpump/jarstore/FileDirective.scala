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

package io.gearpump.jarstore

import org.apache.pekko.http.scaladsl.model.{HttpEntity, MediaTypes, Multipart}
import org.apache.pekko.http.scaladsl.server._
import org.apache.pekko.http.scaladsl.server.Directives._
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{FileIO, StreamConverters}
import org.apache.pekko.util.ByteString
import java.io.File
import java.nio.file.Files
import java.util.UUID
import java.util.concurrent.{ConcurrentLinkedQueue, Semaphore}
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal
import scala.concurrent.{ExecutionContext, Future}


/**
 * FileDirective is a set of Pekko HTTP directives to upload/download
 * huge binary files to/from a Pekko HTTP server.
 */
object FileDirective {

  // Form field name
  type Name = String

  val CHUNK_SIZE = 262144
  val MaxFieldBytes = 64 * 1024
  val MaxFileBytes = 64L * 1024 * 1024
  val MaxRequestBytes = 128L * 1024 * 1024
  private val uploads = new Semaphore(8)
  private val allowedFields = Set("jar", "configfile", "configstring", "executorcount", "args")
  private class InvalidUpload extends java.io.IOException("Invalid multipart fields")

  /**
   * File information after a file is uploaded to server.
   *
   * @param originFileName original file name when user upload it in browser.
   * @param file file name after the file is saved to server.
   * @param length the length of the file
   */
  case class FileInfo(originFileName: String, file: File, length: Long)

  class Form(val fields: Map[Name, FormField], cleanup: () => Unit = () => ()) {
    def cleanupTempFiles(): Unit = cleanup()

    def getFileInfo(fieldName: String): Option[FileInfo] = {
      fields.get(fieldName).flatMap {
        case Left(file) => Option(file)
        case Right(_) => None
      }
    }

    def getValue(fieldName: String): Option[String] = {
      fields.get(fieldName).flatMap {
        case Left(_) => None
        case Right(value) => Option(value)
      }
    }
  }

  type FormField = Either[FileInfo, String]

  /**
   * Store the uploaded files to temporary directory, and return a Map from form field name
   * to FileInfo.
   */
  def uploadFile: Directive1[Form] = {
    Directive[Tuple1[Form]] { inner =>
      extractMaterializer {implicit mat =>
        extractExecutionContext {implicit ec =>
          if (!uploads.tryAcquire()) {
            complete(org.apache.pekko.http.scaladsl.model.StatusCodes.ServiceUnavailable)
          } else {
            mapRouteResultFuture(_.andThen { case _ => uploads.release() }) {
              uploadFileImpl(mat, ec) { formFuture =>
                ctx => formFuture.flatMap { form =>
                  val result = try inner(Tuple1(form))(ctx) catch {
                    case NonFatal(ex) => Future.failed(ex)
                  }
                  result.andThen { case _ => form.cleanupTempFiles() }
                }.recoverWith {
                  case _: InvalidUpload =>
                    complete(org.apache.pekko.http.scaladsl.model.StatusCodes.BadRequest,
                      "Invalid multipart fields").apply(ctx)
                  case _: org.apache.pekko.http.scaladsl.model.EntityStreamSizeException =>
                    complete(org.apache.pekko.http.scaladsl.model.StatusCodes.PayloadTooLarge).apply(ctx)
                }
              }
            }
          }
        }
      }
    }
  }

  /**
   * Store the uploaded files to JarStore, and return a Map from form field name
   * to FilePath in JatStore.
   */
  def uploadFileTo(jarStore: JarStore): Directive1[Map[Name, FilePath]] = {
    Directive[Tuple1[Map[Name, FilePath]]] { inner =>
      extractMaterializer {implicit mat =>
        extractExecutionContext {implicit ec =>
          uploadFileImpl(jarStore)(mat, ec) { filesFuture =>
            ctx => {
              filesFuture.map(map => inner(Tuple1(map))).flatMap(route => route(ctx))
            }
          }
        }
      }
    }
  }

  // Downloads file from server
  def downloadFileFrom(jarStore: JarStore, filePath: String): Route = {
    val responseEntity = HttpEntity(
      MediaTypes.`application/octet-stream`,
      StreamConverters.fromInputStream(
        () => jarStore.getFile(filePath), CHUNK_SIZE
      ))
    complete(responseEntity)
  }

  private def uploadFileImpl(jarStore: JarStore)
    (implicit mat: Materializer, ec: ExecutionContext): Directive1[Future[Map[Name, FilePath]]] = {
    Directive[Tuple1[Future[Map[Name, FilePath]]]] { inner =>
      withSizeLimit(MaxRequestBytes) {
      entity(as[Multipart.FormData]) { formdata =>
        var parts = 0
        val created = new ConcurrentLinkedQueue[String]()
        val fileNameMap = formdata.parts.mapAsync(1) { part =>
          parts += 1
          if (parts != 1 || part.filename.isEmpty || part.name != "uploadfile") {
            part.entity.discardBytes()
            Future.failed(new InvalidUpload)
          } else {
            val path = UUID.randomUUID().toString + ".jar"
            created.add(path)
            val sink = StreamConverters.fromOutputStream(() => jarStore.createFile(path),
              autoFlush = true)
            part.entity.withSizeLimit(MaxFileBytes).dataBytes.runWith(sink).map { written =>
              written.status.get
              if (written.count <= 0) throw new InvalidUpload
              Map(part.name -> FilePath(path))
            }.recoverWith { case NonFatal(ex) =>
              jarStore.deleteFile(path)
              Future.failed(ex)
            }
          }
        }.runFold(Map.empty[Name, FilePath])(_ ++ _).recoverWith { case NonFatal(ex) =>
          created.iterator().asScala.foreach(jarStore.deleteFile)
          Future.failed(ex)
        }
        inner(Tuple1(fileNameMap))
      }}
    }
  }

  private def uploadFileImpl(implicit mat: Materializer, ec: ExecutionContext)
    : Directive1[Future[Form]] = {
    Directive[Tuple1[Future[Form]]] { inner =>
      withSizeLimit(MaxRequestBytes) {
        entity(as[Multipart.FormData]) { formdata =>
          val temporary = new ConcurrentLinkedQueue[File]()
          def cleanup(): Unit = temporary.iterator().asScala.foreach { file =>
            Files.deleteIfExists(file.toPath)
          }
          var seen = Set.empty[String]
          val fields = formdata.parts.mapAsync(1) { part =>
            if (!allowedFields.contains(part.name) || seen.contains(part.name)) {
              part.entity.discardBytes()
              throw new InvalidUpload
            }
            seen += part.name
            if (part.filename.isDefined) {
              val target = Files.createTempFile("gearpump-upload-", ".tmp").toFile
              temporary.add(target)
              val written = part.entity.withSizeLimit(MaxFileBytes).dataBytes
                .runWith(FileIO.toPath(target.toPath))
              written.map { result =>
                result.status.get
                Map(part.name -> Left(FileInfo(part.filename.get, target, result.count)))
              }
            } else {
              part.entity.withSizeLimit(MaxFieldBytes).dataBytes.runFold(ByteString.empty) {
                (total, chunk) => total ++ chunk
              }.map(value => Map(part.name -> Right(value.utf8String)))
            }
          }.runFold(Map.empty[Name, FormField])(_ ++ _)
          val form = fields.map(value => new Form(value, () => cleanup())).recoverWith {
            case NonFatal(ex) =>
              cleanup()
              Future.failed(ex)
          }
          inner(Tuple1(form))
        }
      }
    }
  }
}
