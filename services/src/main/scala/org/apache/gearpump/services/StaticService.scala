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
import java.util.jar.Manifest

import spray.http.StatusCodes
import spray.routing.HttpService

trait StaticService extends HttpService {

  val version = {
    val manifest = getManifest(this.getClass)
    manifest.getMainAttributes.getValue("Implementation-Version")
  }

  private def getManifest(myClass : Class[_]): Manifest = {
    val resource = "/" + myClass.getName().replace(".", "/") + ".class"
    val fullPath = myClass.getResource(resource).toString()
    val archivePath = fullPath.substring(0, fullPath.length() - resource.length())
    val input = new URL(archivePath + "/META-INF/MANIFEST.MF").openStream()
    new Manifest(input)
  }

  val staticRoute = {
    pathEndOrSingleSlash {
      getFromResource("index.html")
    } ~
    path("favicon.ico") {
      complete(StatusCodes.NotFound)
    } ~
    path(Rest) { path =>
      getFromResource("%s" format path)
    } ~
    pathPrefix("webjars") {
      get {
        getFromResourceDirectory("META-INF/resources/webjars")
      }
    } ~
    path("version") {
      get {
        complete(version)
      }
    }
  }
}
