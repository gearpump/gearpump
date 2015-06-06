package org.apache.gearpump.dashboard.controllers

import scala.scalajs.concurrent.JSExecutionContext.Implicits.runNow
import com.greencatsoft.angularjs.core.{Route, RouteProvider, Scope, Location}
import com.greencatsoft.angularjs.{Config, injectable, AbstractController}
import org.apache.gearpump.dashboard.services.RestApiService
import org.apache.gearpump.shared.Messages.{AppMasterData, AppMastersData}

import scala.scalajs.js
import scala.scalajs.js.annotation.JSExport
import scala.util.{Failure, Success}

trait AppsScope extends Scope {
  var apps: js.Array[AppMasterData] = js.native
  var view: js.Function1[String,Unit] = js.native
  var kill: js.Function1[String,Unit] = js.native
}

@JSExport
@injectable("AppsConfig")
class AppsConfig(routeProvider: RouteProvider) extends Config {
  println("AppsConfig")
  routeProvider.when ("/apps", Route("views/apps/apps.html", "Applications", "AppsCtrl") )
}

@JSExport
@injectable("AppsCtrl")
class AppsCtrl(scope: AppsScope, location: Location, restApi: RestApiService)
  extends AbstractController[AppsScope](scope) {

  println("AppsCtrl")

  scope.view = view _
  scope.kill = kill _
  scope.apps = js.Array[AppMasterData]()

  def view(id: String): Unit = {
    location.path("/apps/app/" + id)
  }

  def kill(id: String): Unit = {
    restApi.killApp(id)
  }

  restApi.subscribe("/appmasters") onComplete {
    case Success(value) =>
      val data = upickle.read[AppMastersData](value)
      scope.apps = data.appMasters.to[js.Array]
      println(s"apps length=${scope.apps.length}")
    case Failure(t) =>
      println(s"Failed to get master ${t.getMessage}")
  }

}
