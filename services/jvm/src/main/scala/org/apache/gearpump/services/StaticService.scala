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

import spray.http.StatusCodes
import spray.routing.HttpService

import java.util.jar.Manifest

trait StaticService extends HttpService {

  val version = {
    val url = getClass.getClassLoader.getResource("META-INF/MANIFEST.MF")
    val manifest = new Manifest(url.openStream())
    manifest.getMainAttributes.getValue("Implementation-Version")
  }

  val staticRoute =
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
    pathPrefix("api"/s"$REST_VERSION") {
      path("version") {
        get {
          complete(version)
        }
      }
    }

}
