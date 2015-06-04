package org.apache.gearpump.dashboard.controllers

import scala.scalajs.concurrent.JSExecutionContext.Implicits.runNow
import com.greencatsoft.angularjs.core.{Route, RouteProvider, Scope}
import com.greencatsoft.angularjs.{Config, AbstractController, injectable}
import org.apache.gearpump.dashboard.services.RestApiService
import org.apache.gearpump.shared.Messages.{MasterDescription, MasterData}

import scala.scalajs.js
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
}

@JSExport
@injectable("MasterCtrl")
class MasterCtrl(scope: MasterScope, restApi: RestApiService)
  extends AbstractController[MasterScope](scope) {

  println("MasterCtrl")

  restApi.subscribe("/master", scope) onComplete {
    case Success(value) =>
      val data = upickle.read[MasterData](value)
      scope.master = data.masterDescription
      println(s"leader=${scope.master.leader}")
    case Failure(t) =>
      println(s"Failed to get master ${t.getMessage}")
  }
}

