package org.apache.gearpump.dashboard.controllers

import com.greencatsoft.angularjs.{AbstractController, injectable}
import org.apache.gearpump.dashboard.services.{RestApiService, UtilService}
import org.apache.gearpump.shared.Messages.AppMasterData

import scala.scalajs.concurrent.JSExecutionContext.Implicits.runNow
import scala.scalajs.js
import scala.scalajs.js.annotation.JSExport
import scala.util.{Failure, Success}

@JSExport
@injectable("AppStatusCtrl")
class AppStatusCtrl(scope: AppMasterScope, restApi: RestApiService, util: UtilService)
  extends AbstractController[AppMasterScope](scope) {

  println("AppStatusCtrl")

  def fetch: Unit = {
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
          SummaryEntry(name = "Number of Tasks", value = Option(scope.streamingDag).map(_.getNumOfTasks)),
          SummaryEntry(name = "Number of Executors", value = Option(scope.streamingDag).map(_.executors.size))
        )
      case Failure(t) =>
        println(s"Failed to appmaster status ${t.getMessage}")
    }
  }

  fetch
}
