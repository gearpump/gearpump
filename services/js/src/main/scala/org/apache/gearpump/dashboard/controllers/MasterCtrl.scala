package org.apache.gearpump.dashboard.controllers

import com.greencatsoft.angularjs.core.{Route, RouteProvider, Scope}
import com.greencatsoft.angularjs.{AbstractController, Config, injectable}
import org.apache.gearpump.dashboard.services.RestApiService
import org.apache.gearpump.shared.Messages.{MasterData, MasterDescription, MasterStatus}

import scala.scalajs.concurrent.JSExecutionContext.Implicits.runNow
import scala.scalajs.js
import scala.scalajs.js.UndefOrOps
import scala.scalajs.js.annotation.JSExport
import scala.util.{Failure, Success}

@JSExport
@injectable("MasterConfig")
class MasterConfig(routeProvider: RouteProvider) extends Config {
  println("MasterConfig")
  routeProvider.when("/cluster", Route.redirectTo("/cluster/master")).
    when("/cluster/master", Route("views/cluster/master.html", "Master", "MasterCtrl"))
}

trait MasterScope extends Scope {
  var master: MasterDescription = js.native
  var statusClass: js.Function3[js.UndefOr[js.Any],String,String,String] = js.native
}

@JSExport
@injectable("MasterCtrl")
class MasterCtrl(scope: MasterScope, restApi: RestApiService)
  extends AbstractController[MasterScope](scope) {

  println("MasterCtrl")

  def statusClass(value: js.UndefOr[js.Any], good: String, bad: String): String = {
    val status = UndefOrOps.getOrElse$extension(value)("")
    status.toString match {
      case MasterStatus.Synced =>
        good
      case MasterStatus.UnSynced =>
        bad
      case _ =>
        bad
    }
  }

  scope.master = MasterDescription(leader = ("127.0.0.1", 0),cluster=List.empty[(String,Int)], aliveFor=0, logFile="", masterStatus=MasterStatus.UnSynced, jarStore="", homeDirectory = "")
  scope.statusClass = statusClass _

  restApi.subscribe("/master") onComplete {
    case Success(value) =>
      val data = upickle.read[MasterData](value)
      scope.master = data.masterDescription
      println(s"leader=${scope.master.leader}")
    case Failure(t) =>
      println(s"Failed to get master ${t.getMessage}")
  }
}

