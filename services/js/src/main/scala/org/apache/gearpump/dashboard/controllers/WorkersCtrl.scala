package org.apache.gearpump.dashboard.controllers

import com.greencatsoft.angularjs.core.{Route, RouteProvider, Scope}
import com.greencatsoft.angularjs.{AbstractController, Config, injectable}
import org.apache.gearpump.dashboard.services.RestApiService
import org.apache.gearpump.shared.Messages.WorkerDescription

import scala.scalajs.concurrent.JSExecutionContext.Implicits.runNow
import scala.scalajs.js
import scala.scalajs.js.UndefOrOps
import scala.scalajs.js.annotation.{JSExportAll, JSExport}
import scala.util.{Failure, Success}

@JSExport
@JSExportAll
case class Slots(usage: Double, used: Int, total: Int)

@JSExport
@injectable("WorkersConfig")
class WorkersConfig(routeProvider: RouteProvider) extends Config {
  routeProvider.when("/cluster/workers", Route("views/cluster/workers/workers.html", "Workers", "WorkersCtrl"))
}

trait WorkersScope extends Scope {
  var workers: js.Array[WorkerDescription] = js.native
  var slotsUsageClass: js.Function4[Int,String,String,String,String] = js.native
  var statusClass: js.Function3[js.UndefOr[Any],String,String,String] = js.native
  var slots: js.Function1[WorkerDescription, Slots] = js.native
}

@JSExport
@injectable("WorkersCtrl")
class WorkersCtrl(scope: WorkersScope, restApi: RestApiService)
  extends AbstractController[WorkersScope](scope) {

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

  def statusClass(value: js.UndefOr[Any], good: String, bad: String): String = {
    val status = UndefOrOps.getOrElse$extension(value)("")
    status.toString match {
      case "active" =>
        good
      case _ =>
        bad
    }
  }

  def slots(worker: WorkerDescription): Slots = {
    var slotsUsed = worker.totalSlots - worker.availableSlots
    val usage = if(worker.totalSlots > 0) {
      Math.floor(100 * slotsUsed / worker.totalSlots)
    } else {
      0
    }
    Slots(usage=usage, used=slotsUsed, total=worker.totalSlots)
  }

  def extractHostname(worker: WorkerDescription): String = {
    var i = worker.actorPath.indexOf('@')
    var hostname = worker.actorPath.substring(i + 1)
    i = hostname.indexOf('/')
    hostname.substring(0, i)
  }

  scope.workers = js.Array[WorkerDescription]()
  scope.slotsUsageClass = slotsUsageClass _
  scope.statusClass = statusClass _
  scope.slots = slots _

  restApi.subscribe("/workers") onComplete {
    case Success(value) =>
      val data = upickle.read[js.Array[WorkerDescription]](value)
      scope.workers = data
    case Failure(t) =>
      println(s"Failed to get workers ${t.getMessage}")
  }
}


