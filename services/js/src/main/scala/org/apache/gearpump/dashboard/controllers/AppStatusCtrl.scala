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
package org.apache.gearpump.dashboard.controllers

import com.greencatsoft.angularjs.{AbstractController, injectable}
import org.apache.gearpump.dashboard.services.{RestApiService, UtilService}
import org.apache.gearpump.shared.Messages.AppMasterData

import scala.scalajs.concurrent.JSExecutionContext.Implicits.runNow
import scala.scalajs.js
import scala.scalajs.js.annotation.JSExport
import scala.util.{Try, Failure, Success}

@JSExport
@injectable("AppStatusCtrl")
class AppStatusCtrl(scope: AppMasterScope, restApi: RestApiService, util: UtilService)
  extends AbstractController[AppMasterScope](scope) {

  def fetch(): Unit = {
    Try({
      if (scope.streamingDag.isDefined) {
        val streamingDag = scope.streamingDag.get
        restApi.subscribe(s"/appmaster/${scope.app.appId}") onComplete {
          case Success(data) =>
            val value = upickle.read[AppMasterData](data)
            scope.summary = js.Array[SummaryEntry](
              SummaryEntry(name = "Name", value = value.appName),
              SummaryEntry(name = "ID", value = value.appId),
              SummaryEntry(name = "Status", value = value.status),
              SummaryEntry(name = "Submission Time", value = util.stringToDateTime(value.submissionTime)),
              SummaryEntry(name = "Start Time", value = util.stringToDateTime(value.startTime)),
              SummaryEntry(name = "Stop Time", value = util.stringToDateTime(value.finishTime)),
              SummaryEntry(name = "Number of Tasks", value = streamingDag.getNumOfTasks),
              SummaryEntry(name = "Number of Executors", value = streamingDag.executors.size)
            )
          case Failure(t) =>
            println(s"Failed to appmaster status ${t.getMessage}")
        }
      }
    }) match {
      case Success(ok) =>
      case Failure(throwable) =>
        println(s"failed ${throwable.getMessage}")
    }
  }
  scope.$watch("streamingDag", fetch _)
}
