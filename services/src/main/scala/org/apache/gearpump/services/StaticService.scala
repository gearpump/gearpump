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

package org.apache.gearpump.services

import java.net.URL
import spray.http.StatusCodes
import spray.routing.HttpService

import java.util.jar.Manifest
import spray.http.HttpHeader
import spray.http.HttpResponse
import spray.http.HttpHeaders.Host
import spray.http.HttpEntity
import spray.http.MediaTypes._

trait StaticService extends HttpService {

  val version = {
    val manifest = getManifest(this.getClass)
    manifest.getMainAttributes.getValue("Implementation-Version")
  }

  def getManifest(myClass : Class[_]): Manifest = {
    val resource = "/" + myClass.getName().replace(".", "/") + ".class"
    val fullPath = myClass.getResource(resource).toString()
    val archivePath = fullPath.substring(0, fullPath.length() - resource.length())
    val input = new URL(archivePath + "/META-INF/MANIFEST.MF").openStream()
    new Manifest(input)
  }

  private def getHost: HttpHeader => Option[Host] = {
    case h: Host => Some(h)
    case x => None
  }

  val staticRoute =
    pathEndOrSingleSlash {
      getFromResource("index.html")
    } ~
    path("favicon.ico") {
      complete(StatusCodes.NotFound)
    } ~
    path("redirect") {
      headerValue(getHost) {host =>

        respondWithMediaType(`text/html`) {
          complete {
            new HttpResponse(StatusCodes.OK, HttpEntity(
              s"""
                |  <html>
                |  <title>Redirect to Gearpump Dashboard</title>
                |  <body>
                |    <script>window.location = "http://${host.host}:${host.port}"
                |    </script>
                |  </body>
                |  </html>
              """.stripMargin
            ))
          }
        }
      }
    } ~
    path(Rest) { path =>
      getFromResource("%s" format path)
    } ~
    pathPrefix("webjars") {
      get {
        getFromResourceDirectory("META-INF/resources/webjars")
      }
    } ~
    pathPrefix("api"/s"$REST_VERSION") {
      path("version") {
        get {
          complete(version)
        }
      }
    }

}
