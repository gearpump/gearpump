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
package org.apache.gearpump.jarstore

import java.io.File
import scala.concurrent.{ExecutionContext, Future}

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.marshalling.Marshal
import akka.http.scaladsl.model.Uri.{Path, Query}
import akka.http.scaladsl.model.{HttpEntity, HttpRequest, MediaTypes, Multipart, _}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server._
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{FileIO, Sink, Source}
import spray.json.DefaultJsonProtocol._
import spray.json.JsonFormat

import org.apache.gearpump.jarstore.FileDirective._
import org.apache.gearpump.jarstore.FileServer.Port

/**
 * A simple file server implemented with akka-http to store/fetch large
 * binary files.
 */
class FileServer(system: ActorSystem, host: String, port: Int = 0, jarStore: JarStore) {
  import system.dispatcher
  implicit val actorSystem = system
  implicit val materializer = ActorMaterializer()
  implicit def ec: ExecutionContext = system.dispatcher

  val route: Route = {
    path("upload") {
      uploadFileTo(jarStore) { form =>
        val uploadedFilePath = form.headOption.map(_._2)

        if (uploadedFilePath.isDefined) {
          complete(uploadedFilePath.get.path)
        } else {
          failWith(new Exception("File not found in the uploaded form"))
        }
      }
    } ~
      path("download") {
        parameters("file") { file: String =>
          downloadFileFrom(jarStore, file)
        }
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

  case class Port(port: Int)

  /**
   * Client of [[org.apache.gearpump.jarstore.FileServer]]
   */
  class Client(system: ActorSystem, host: String, port: Int) {

    def this(system: ActorSystem, url: String) = {
      this(system, Uri(url).authority.host.address(), Uri(url).authority.port)
    }

    private implicit val actorSystem = system
    private implicit val materializer = ActorMaterializer()
    private implicit val ec = system.dispatcher

    val server = Uri(s"http://$host:$port")
    val httpClient = Http(system).outgoingConnection(server.authority.host.address(),
      server.authority.port)

    def upload(file: File): Future[FilePath] = {
      val target = server.withPath(Path("/upload"))

      val request = entity(file).map { entity =>
        HttpRequest(HttpMethods.POST, uri = target, entity = entity)
      }

      val response = Source.fromFuture(request).via(httpClient).runWith(Sink.head)
      response.flatMap { some =>
        Unmarshal(some).to[String]
      }.map { path =>
        FilePath(path)
      }
    }

    def download(remoteFile: FilePath, saveAs: File): Future[Unit] = {
      val download = server.withPath(Path("/download")).withQuery(Query("file" -> remoteFile.path))
      // Download file to local
      val response = Source.single(HttpRequest(uri = download)).via(httpClient).runWith(Sink.head)
      val downloaded = response.flatMap { response =>
        response.entity.dataBytes.runWith(FileIO.toFile(saveAs))
      }
      downloaded.map(written => Unit)
    }

    private def entity(file: File)(implicit ec: ExecutionContext): Future[RequestEntity] = {
      val entity = HttpEntity(MediaTypes.`application/octet-stream`, file.length(),
        FileIO.fromFile(file, chunkSize = 100000))
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