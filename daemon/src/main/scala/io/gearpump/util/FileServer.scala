package io.gearpump.util


import java.io.File
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.marshalling.{ToResponseMarshallable, Marshal}
import akka.http.scaladsl.model.Uri.Path
import akka.http.scaladsl.model.{HttpEntity, HttpRequest, MediaTypes, Multipart, _}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.ActorMaterializer
import akka.stream.io.{SynchronousFileSink, SynchronousFileSource}
import akka.stream.scaladsl.{Sink, Source}
import io.gearpump.jarstore.FilePath
import io.gearpump.util.FileServer.Port
import spray.json.{JsonFormat, RootJsonFormat}
import scala.concurrent.{ExecutionContext, Future}
import spray.json.DefaultJsonProtocol._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import FileDirective._

class FileServer(system: ActorSystem, host: String, port: Int = 0, rootDirectory: File) {
  import system.dispatcher
  implicit val actorSystem = system
  implicit val materializer = ActorMaterializer()
  import FileServer.filePathFormat

  val route: Route = {
    path("upload") {
      uploadFileTo(rootDirectory) { fileMap =>
        complete(fileMap.head._2.file.getName)
      }
    } ~
      path("download") {
        parameters("file") { file =>
          downloadFile(new File(rootDirectory, file))
        }
      } ~
      pathEndOrSingleSlash {
        extractUri { uri =>
          val upload = uri.withPath(Uri.Path("/upload")).toString()
          val entity = HttpEntity(MediaTypes.`text/html`,
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

  private var connection: Future[ServerBinding] = null

  def start: Future[Port] = {
    connection = Http().bindAndHandle(Route.handlerFlow(route), host, port)
    connection.map(address => Port(address.localAddress.getPort))
  }

  def stop: Future[Unit] = {
    connection.flatMap(_.unbind())
  }
}

object FileServer {

  implicit def filePathFormat: JsonFormat[FilePath] = jsonFormat1(FilePath.apply)

  def newClient = null

  case class Port(port : Int)

  class Client(system: ActorSystem, host: String, port: Int) {

    def this(system: ActorSystem, url: String) = {
      this(system, Uri(url).authority.host.address(), Uri(url).authority.port)
    }

    private implicit val actorSystem = system
    private implicit val materializer = ActorMaterializer()
    private implicit val ec = system.dispatcher

    val server = Uri(s"http://$host:$port")
    val httpClient = Http(system).outgoingConnection(server.authority.host.address(), server.authority.port)

    def upload(file: File): Future[FilePath] = {
      val target = server.withPath(Path("/upload"))

      val request = entity(file).map{entity =>
        HttpRequest(HttpMethods.POST, uri = target, entity = entity)
      }

      val response = Source(request).via(httpClient).runWith(Sink.head)
      response.flatMap{some =>
        Unmarshal(some).to[String]
      }.map{path =>
        FilePath(path)
      }
    }

    def download(remoteFile: FilePath, saveAs: File): Future[Unit] = {
      val downoad = server.withPath(Path("/download")).withQuery("file" -> remoteFile.path)
      //download file to local
      val response = Source.single(HttpRequest(uri = downoad)).via(httpClient).runWith(Sink.head)
      val downloaded = response.flatMap { response =>
        response.entity.dataBytes.runWith(SynchronousFileSink(saveAs))
      }
      downloaded.map(written => Unit)
    }

    private def entity(file: File)(implicit ec: ExecutionContext): Future[RequestEntity] = {
      val entity =  HttpEntity(MediaTypes.`application/octet-stream`, file.length(), SynchronousFileSource(file, chunkSize = 100000))
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