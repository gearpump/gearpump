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

import com.greencatsoft.angularjs.core.{HttpPromise, HttpService, Timeout}
import com.greencatsoft.angularjs.extensions.{ModalInstance, ModalOptions, ModalService}
import com.greencatsoft.angularjs.{Factory, Service, injectable}

import scala.concurrent.Future
import scala.scalajs.concurrent.JSExecutionContext.Implicits.runNow
import scala.scalajs.js
import scala.scalajs.js.JSON
import scala.scalajs.js.annotation.JSExport

@JSExport
@injectable("RestApiService")
class RestApiService(http: HttpService, timeout: Timeout, confService: ConfService) extends Service {
  require(http != null, "Missing argument 'http'.")

  import confService.conf

  def subscribe(uri: String): Future[String] = {
    val url = conf.restapiRoot + uri
    val future: Future[js.Any] = http.get(url)
    future.map(JSON.stringify(_))
  }

  def getUrl(uri: String): Future[String] = {
    val url = '/' + uri
    val future: Future[js.Any] = http.get(url)
    future.map(_.toString)
  }

  def killApp(appId: String): HttpPromise = {
    val url = conf.restapiRoot + "/appmaster/" + appId
    http.delete(url)
  }

  def restartAppAsync(appId: String): HttpPromise = {
    val url = conf.restapiRoot + "/appmaster/" + appId + "/restart"
    http.post(url)
  }

  def appConfigLink(appId: String): String = {
    conf.restapiRoot + "/appmaster/" + appId + "/config"
  }

  def workerConfigLink(workerId: String): String = {
    conf.restapiRoot + "/worker/" + workerId +  "/config"
  }

  def masterConfigLink: String = {
    conf.restapiRoot + "/master/config"
  }

  //TODO methods below will be moved elsewhere when this class replaces restapi.js
  //def submitUserApp(files, formFormNames, args)
  //def replaceDagProcessor(appId, oldProcessorId, newProcessorDescription, onComplete)
  //def repeatHealthCheck(scope, onData)

}

@JSExport
@injectable("RestApiService")
class RestApiServiceFactory(http: HttpService, timeout: Timeout, modal: ModalService, options: ConfService)
  extends Factory[RestApiService] {

  def openModal: ModalInstance = {
    val options = ModalOptions()
    options.templateUrl = "views/services/serverproblemnotice.html"
    options.backdrop = false
    modal.open(options)
  }

  override def apply() = {
    new RestApiService(http, timeout, options)
  }
}

