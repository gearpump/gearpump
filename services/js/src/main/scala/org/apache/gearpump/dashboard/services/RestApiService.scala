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

package org.apache.gearpump.dashboard.services

import com.greencatsoft.angularjs.core.{HttpPromise, HttpService, Scope, Timeout}
import com.greencatsoft.angularjs.{Factory, Service, injectable}

import scala.concurrent.Future
import scala.scalajs.concurrent.JSExecutionContext.Implicits.runNow
import scala.scalajs.js
import scala.scalajs.js.JSON
import scala.scalajs.js.annotation.JSExport

@JSExport
@injectable("RestApiService")
class RestApiService(http: HttpService, timeout: Timeout, options: OptionsService) extends Service {
  require(http != null, "Missing argument 'http'.")

  println("RestApiService")

  def subscribe(url: String, scope: Scope): Future[String] = {
    val future: Future[js.Any] = http.get(options.conf.restapiRoot + url)
    future.map(JSON.stringify(_))
  }

  def killApp(appId: String): HttpPromise = {
    val url = options.conf.restapiRoot + "/appmaster/" + appId
    http.delete(url)
  }

}

@JSExport
@injectable("RestApiService")
class RestApiServiceFactory(http: HttpService, timeout: Timeout, options: OptionsService)
  extends Factory[RestApiService] {

  println("RestApiServiceFactory")

  override def apply() = {
    new RestApiService(http, timeout, options)
  }
}

