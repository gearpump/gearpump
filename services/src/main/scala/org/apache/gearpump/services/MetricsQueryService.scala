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

import akka.actor.{ActorRef, ActorSystem}
import akka.pattern.ask
import org.apache.gearpump._
import org.apache.gearpump.cluster.ClientToMaster.{QueryHistoryMetrics, QueryAppMasterConfig}
import org.apache.gearpump.cluster.MasterToClient.{HistoryMetrics, AppMasterConfig}
import org.apache.gearpump.metrics.Metrics.{MetricType, Meter, Histogram}

import org.apache.gearpump.util.{Constants, LogUtil}
import spray.http.StatusCodes
import spray.routing.HttpService
import upickle.Js

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

trait MetricsQueryService extends HttpService  {
  def master:ActorRef
  implicit val system: ActorSystem
  private val LOG = LogUtil.getLogger(getClass)

  def metricQueryRoute = get {
    implicit val ec: ExecutionContext = actorRefFactory.dispatcher
    implicit val timeout = Constants.FUTURE_TIMEOUT
    pathPrefix("api"/s"$REST_VERSION") {
      path("metrics" / "app" / IntNumber / Rest) { (appId, path) =>
        onComplete((master ? QueryHistoryMetrics(appId, path)).asInstanceOf[Future[HistoryMetrics]]) {
          case Success(value: HistoryMetrics) =>
            complete(upickle.write(value))
          case Failure(ex) =>
            complete(StatusCodes.InternalServerError, s"An error occurred: ${ex.getMessage}")
        }
      }
    }
  }
}