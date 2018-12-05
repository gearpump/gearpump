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

package io.gearpump.experiments.distributeservice

import java.io.File

import scala.util.{Failure, Success, Try}
import akka.actor.{Actor, Stash}
import akka.io.IO
import io.gearpump.util.{FileUtils, LogUtil}
import org.apache.commons.httpclient.HttpClient
import org.apache.commons.httpclient.methods.{ByteArrayRequestEntity, GetMethod, PostMethod}
import spray.can.Http
import spray.http.HttpMethods._
import spray.http._
import io.gearpump.util.LogUtil

/**
 *
 * Should not use this to server too big files(more than 100MB), otherwise OOM may happen.
 *
 * port: set port to 0 if you want to bind to random port
 */
class FileServer(rootDir: File, host: String, port: Int) extends Actor with Stash {
  private val LOG = LogUtil.getLogger(getClass)

  implicit val system = context.system

  override def preStart(): Unit = {
    // Creates http server
    IO(Http) ! Http.Bind(self, host, port)
  }

  override def postStop(): Unit = {
    // Stop the server
    IO(Http) ! Http.Unbind
  }

  override def receive: Receive = {
    case Http.Bound(address) =>
      LOG.info(s"FileServer bound on port: ${address.getPort}")
      context.become(listen(address.getPort))
      unstashAll()
    case _ =>
      stash()
  }

  def listen(port: Int): Receive = {
    case FileServer.GetPort => {
      sender ! FileServer.Port(port)
    }
    case Http.Connected(remote, _) =>
      sender ! Http.Register(self)

    // Fetches files from remote uri
    case HttpRequest(GET, uri, _, _, _) =>
      val child = uri.path.toString()
      val payload = Try {
        val source = new File(rootDir, child)
        FileUtils.readFileToByteArray(source)
      }
      payload match {
        case Success(data) =>
          sender ! HttpResponse(entity = HttpEntity(data))
        case Failure(ex) =>
          LOG.error("failed to get file " + ex.getMessage)
          sender ! HttpResponse(status = StatusCodes.InternalServerError, entity = ex.getMessage)
      }
    // Save file to remote uri
    case post@HttpRequest(POST, uri, _, _, _) =>
      val child = uri.path.toString()

      val status = Try {
        val target = new File(rootDir, child)
        val payload = post.entity.data.toByteArray
        FileUtils.writeByteArrayToFile(target, payload)
        "OK"
      }
      status match {
        case Success(message) => sender ! HttpResponse(entity = message)
        case Failure(ex) =>
          LOG.error("save file failed " + ex.getMessage)
          sender ! HttpResponse(status = StatusCodes.InternalServerError, entity = ex.getMessage)
      }
  }
}

object FileServer {
  object GetPort
  case class Port(port: Int)

  def newClient: Client = new Client

  class Client {
    val client = new HttpClient()

    def save(uri: String, data: Array[Byte]): Try[Int] = {
      Try {
        val post = new PostMethod(uri)
        val entity = new ByteArrayRequestEntity(data)
        post.setRequestEntity(entity)
        client.executeMethod(post)
      }
    }

    def get(uri: String): Try[Array[Byte]] = {
      val get = new GetMethod(uri)
      val status = Try {
        client.executeMethod(get)
      }

      val data = status.flatMap { code =>
        if (code == 200) {
          Success(get.getResponseBody())
        } else {
          Failure(new Exception(s"We cannot get the data, the status code is $code"))
        }
      }
      data
    }
  }
}
