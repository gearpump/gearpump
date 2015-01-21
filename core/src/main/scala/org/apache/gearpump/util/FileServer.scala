/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.gearpump.util

import java.io.File

import akka.actor.{Actor, Stash}
import akka.io.IO
import org.apache.commons.httpclient.HttpClient
import org.apache.commons.httpclient.methods.{ByteArrayRequestEntity, GetMethod, PostMethod}
import org.apache.commons.io.FileUtils
import spray.can.Http
import spray.http.HttpMethods._
import spray.http._

import scala.util.{Failure, Success, Try}

/**
 *
 * Should not use this to server too big files(more than 100MB), otherwise OOM may happen.
 *
 * port: set port to 0 if you want to bind to random port
 */
class FileServer(rootDir: File, host: String, port : Int) extends Actor with Stash {
  private val LOG = LogUtil.getLogger(getClass)

  implicit val system = context.system

  override def preStart(): Unit = {
    // create http server
    IO(Http) ! Http.Bind(self, host, port)
  }

  override def postStop() : Unit = {
    //stop the server
    IO(Http) ! Http.Unbind
  }

  override def receive: Receive = {
    case Http.Bound(address) =>
      context.become(listen(address.getPort))
      unstashAll()
    case _ =>
      stash()
  }

  def listen(port : Int) : Receive = {
    case FileServer.GetPort => {
      LOG.info(s"retunning port: $port")
      sender ! FileServer.Port(port)
    }
    case Http.Connected(remote, _) =>
      sender ! Http.Register(self)

    // fetch file from remote uri
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
          LOG.error("get file failed, " + ex.getMessage, ex)
          sender ! HttpResponse(status = StatusCodes.InternalServerError, ex.getMessage)
      }
    //save file to remote uri
    case post @ HttpRequest(POST, uri, _, _, _) =>
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
          LOG.error("save file failed, ex")
          sender ! HttpResponse(status = StatusCodes.InternalServerError, ex.getMessage)
      }
  }
}

object FileServer {
  object GetPort
  case class Port(port : Int)

  def newClient = new Client

  class Client {
    val client = new HttpClient()

    def save(uri : String, data : Array[Byte]) : Try[Int] = {
      Try {
        val post = new PostMethod(uri)
        val entity = new ByteArrayRequestEntity(data)
        post.setRequestEntity(entity)
        client.executeMethod(post)
      }
    }

    def get(uri : String) : Try[Array[Byte]] = {
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