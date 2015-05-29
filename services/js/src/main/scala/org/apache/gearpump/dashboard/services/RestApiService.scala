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

import com.greencatsoft.angularjs.core.{HttpService, Scope}
import com.greencatsoft.angularjs.{Factory, injectable}

import scala.concurrent.Future
import scala.scalajs.concurrent.JSExecutionContext.Implicits.runNow
import scala.scalajs.js
import scala.scalajs.js.JSON


@injectable("restApiService")
class RestApiService(http: HttpService) {
  require(http != null, "Missing argument 'http'.")

  def subscribe(url: String, scope: Scope): Future[js.Any] = {
    val future: Future[js.Any] = http.get(url)
    future.map(JSON.stringify(_))
  }
}

@injectable("restApiService")
class RestApiServiceFactory(http: HttpService) extends Factory[RestApiService] {
  override def apply() = new RestApiService(http)
}

