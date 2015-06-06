package org.apache.gearpump.dashboard.controllers

import com.greencatsoft.angularjs.core.{Route, RouteProvider, Scope}
import com.greencatsoft.angularjs.{AbstractController, Config, injectable}
import org.apache.gearpump.dashboard.services.RestApiService
import org.apache.gearpump.shared.Messages.WorkerDescription

import scala.scalajs.concurrent.JSExecutionContext.Implicits.runNow
import scala.scalajs.js
import scala.scalajs.js.UndefOrOps
import scala.scalajs.js.annotation.JSExport
import scala.util.{Failure, Success}

@JSExport
@injectable("WorkersConfig")
class WorkersConfig(routeProvider: RouteProvider) extends Config {
  println("WorkersConfig")
  routeProvider.when("/cluster/workers", Route("views/cluster/workers/workers.html", "Workers", "WorkersCtrl"))
}

trait WorkersScope extends Scope {
  var workers: js.Array[WorkerDescription] = js.native
  var slotsUsageClass: js.Function4[Int,String,String,String,String] = js.native
  var statusClass: js.Function3[js.UndefOr[String],String,String,String] = js.native
}

@JSExport
@injectable("WorkersCtrl")
class WorkersCtrl(scope: WorkersScope, restApi: RestApiService)
  extends AbstractController[WorkersScope](scope) {

  println("WorkersCtrl")

  def slotsUsageClass(usage: Int, good: String, concern: String, bad: String): String = {
    if(usage < 50) {
      good
    } else {
      if(usage < 75) {
        concern
      } else {
        bad
      }
    }
  }

  def statusClass(value: js.UndefOr[String], good: String, bad: String): String = {
    val status = UndefOrOps.getOrElse$extension(value)("")
    status match {
      case "active" =>
        good
      case _ =>
        bad
    }
  }

  scope.workers = js.Array[WorkerDescription]()
  scope.slotsUsageClass = slotsUsageClass _
  scope.statusClass = statusClass _

  restApi.subscribe("/workers") onComplete {
    case Success(value) =>
      val data = upickle.read[js.Array[WorkerDescription]](value)
      scope.workers = data
    case Failure(t) =>
      println(s"Failed to get workers ${t.getMessage}")
  }
}


