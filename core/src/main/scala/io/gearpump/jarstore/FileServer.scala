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

import org.apache.pekko.Done
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.Http.ServerBinding
import org.apache.pekko.http.scaladsl.marshalling.Marshal
import org.apache.pekko.http.scaladsl.model.{HttpEntity, HttpRequest, MediaTypes, Multipart, _}
import org.apache.pekko.http.scaladsl.model.Uri.{Path, Query}
import org.apache.pekko.http.scaladsl.server._
import org.apache.pekko.http.scaladsl.server.Directives._
import org.apache.pekko.http.scaladsl.unmarshalling.Unmarshal
import org.apache.pekko.stream.{IOResult, Materializer}
import org.apache.pekko.stream.scaladsl.{FileIO, Sink, Source}
import io.gearpump.jarstore.FileDirective._
import io.gearpump.jarstore.FileServer.Port
import java.io.File
import scala.concurrent.{ExecutionContext, Future}
import spray.json.DefaultJsonProtocol._
import spray.json.JsonFormat

/**
 * A simple file server implemented with Pekko HTTP to store/fetch large
 * binary files.
 */
class FileServer(system: ActorSystem, host: String, port: Int = 0, underlyingStore: JarStore) {
  import system.dispatcher
  implicit val actorSystem: ActorSystem = system
  implicit val materializer: Materializer = Materializer(actorSystem)
  implicit def ec: ExecutionContext = system.dispatcher

  private val config = system.settings.config
  private val tls = io.gearpump.security.ClusterTls.context(config)
  private val token = config.getString("gearpump.jarstore.access-token")
  require(token.matches("[A-Za-z0-9_-]{43,128}"), "Configure a random JAR-store access-token")
  private val jarStore = new QuotaJarStore(underlyingStore,
    config.getLong("gearpump.jarstore.max-storage-bytes"),
    config.getInt("gearpump.jarstore.max-artifacts"),
    config.getLong("gearpump.jarstore.max-artifact-bytes"))
  private def authenticated: Directive0 = optionalHeaderValueByName("Authorization").flatMap {
    case Some(value) if java.security.MessageDigest.isEqual(
        value.getBytes(java.nio.charset.StandardCharsets.UTF_8),
        ("Bearer " + token).getBytes(java.nio.charset.StandardCharsets.UTF_8)) => pass
    case _ => reject(AuthorizationFailedRejection)
  }
  val route: Route = authenticated {
    path("upload") {
      post { uploadFileTo(jarStore) { form =>
        val uploadedFilePath = form.headOption.map(_._2)

        if (uploadedFilePath.isDefined) {
          complete(uploadedFilePath.get.path)
        } else {
          failWith(new Exception("File not found in the uploaded form"))
        }
      }}
    } ~
      path("download") {
        get {
        parameters("file") { file: String =>
          downloadFileFrom(jarStore, file)
        }}
      } ~
      path("artifact") {
        delete { parameter("file") { file =>
          jarStore.deleteFile(file)
          complete(StatusCodes.NoContent)
        }}
      } ~
      pathEndOrSingleSlash {
        extractUri { uri =>
          val upload = uri.withPath(Uri.Path("/upload")).toString()
          val entity = HttpEntity(ContentTypes.`text/html(UTF-8)`,
            s"""
            |
            |<h2>Please specify a file to upload:</h2>
            |<form action="$upload" enctype="multipart/form-data" method="post">
            |<input type="file" name="datafile" size="40">
            |</p>
            |<div>
            |<input type="submit" value="Submit">
            |</div>
            |</form>
          """.stripMargin)
        complete(entity)
      }
    }
  }

  private var connection: Future[ServerBinding] = _

  def start: Future[Port] = {
    connection = Http().newServerAt(host, port).enableHttps(
      org.apache.pekko.http.scaladsl.ConnectionContext.httpsServer(
        () => io.gearpump.security.ClusterTls.serverEngine(tls))).bind(route)
    connection.map(address => Port(address.localAddress.getPort))
  }

  def stop: Future[Done] = {
    connection.flatMap(_.unbind())
  }
}

object FileServer {
  private def requireHttps(url: String): Uri = {
    val uri = Uri(url)
    require(uri.scheme == "https" && uri.authority.userinfo.isEmpty,
      "Artifact endpoints require HTTPS")
    uri
  }


  implicit def filePathFormat: JsonFormat[FilePath] = jsonFormat2(FilePath.apply)

  case class Port(port: Int)

  /**
   * Client of [[io.gearpump.jarstore.FileServer]]
   */
  class Client(system: ActorSystem, host: String, port: Int) {

    def this(system: ActorSystem, url: String) = {
      this(system, FileServer.requireHttps(url).authority.host.address(),
        FileServer.requireHttps(url).authority.port)
    }

    private implicit val actorSystem: ActorSystem = system
    private implicit val materializer: Materializer = Materializer(actorSystem)
    private implicit val ec: scala.concurrent.ExecutionContextExecutor = system.dispatcher

    val server = Uri(s"https://$host:$port")
    private val tls = io.gearpump.security.ClusterTls.context(system.settings.config)
    private val credential = org.apache.pekko.http.scaladsl.model.headers.RawHeader(
      "Authorization", "Bearer " + system.settings.config.getString("gearpump.jarstore.access-token"))
    val httpClient = Http(system).outgoingConnectionHttps(server.authority.host.address(),
      server.authority.port, connectionContext =
        org.apache.pekko.http.scaladsl.ConnectionContext.httpsClient(tls))

    def upload(file: File): Future[FilePath] = {
      val target = server.withPath(Path("/upload"))

      val request = entity(file).map { entity =>
        HttpRequest(HttpMethods.POST, uri = target, entity = entity).addHeader(credential)
      }

      val response = Source.future(request).via(httpClient).runWith(Sink.head)
      response.flatMap { some =>
        if (!some.status.isSuccess()) {
          some.discardEntityBytes()
          Future.failed(new java.io.IOException("Artifact upload rejected: " + some.status.intValue()))
        } else Unmarshal(some).to[String]
      }.map { path =>
        JarStore.validateFileName(path)
        FilePath(path, ArtifactDigest.sha256(file))
      }
    }

    def download(remoteFile: FilePath, saveAs: File): Future[IOResult] = {
      val uri = server.withPath(Path("/download")).withQuery(Query("file" -> remoteFile.path))
      require(remoteFile.sha256.matches("[0-9a-f]{64}"), "Missing artifact digest")
      JarStore.validateFileName(remoteFile.path)
      val temporary = java.nio.file.Files.createTempFile(saveAs.toPath.toAbsolutePath.getParent,
        "gearpump-download-", ".partial")
      val result = Source.single(HttpRequest(uri = uri).addHeader(credential)).via(httpClient)
        .runWith(Sink.head).flatMap { response =>
          if (!response.status.isSuccess()) {
            response.discardEntityBytes()
            Future.failed(new java.io.IOException("Artifact download rejected: " +
              response.status.intValue()))
          } else response.entity.withSizeLimit(
              system.settings.config.getLong("gearpump.jarstore.max-artifact-bytes"))
            .dataBytes.runWith(FileIO.toPath(temporary)).map { written =>
              written.status.get
              ArtifactDigest.verify(temporary.toFile, remoteFile.sha256)
              java.nio.file.Files.move(temporary, saveAs.toPath,
                java.nio.file.StandardCopyOption.ATOMIC_MOVE,
                java.nio.file.StandardCopyOption.REPLACE_EXISTING)
              written
            }
        }
      result.andThen { case _ => java.nio.file.Files.deleteIfExists(temporary) }
    }

    def delete(remoteFile: FilePath): Future[Unit] = {
      JarStore.validateFileName(remoteFile.path)
      val uri = server.withPath(Path("/artifact")).withQuery(Query("file" -> remoteFile.path))
      Source.single(HttpRequest(HttpMethods.DELETE, uri = uri).addHeader(credential))
        .via(httpClient).runWith(Sink.head).flatMap { response =>
          response.discardEntityBytes()
          if (response.status.isSuccess()) Future.successful(())
          else Future.failed(new java.io.IOException("Artifact deletion rejected"))
        }
    }

    private def entity(file: File)(implicit ec: ExecutionContext): Future[RequestEntity] = {
      val entity = HttpEntity(MediaTypes.`application/octet-stream`, file.length(),
        FileIO.fromPath(file.toPath, chunkSize = 100000))
      val body = Source.single(
        Multipart.FormData.BodyPart(
          "uploadfile",
          entity,
          Map("filename" -> file.getName)))
      val form = Multipart.FormData(body)

      Marshal(form).to[RequestEntity]
    }
  }
}
