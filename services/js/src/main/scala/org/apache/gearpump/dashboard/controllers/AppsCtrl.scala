package org.apache.gearpump.dashboard.controllers

import com.greencatsoft.angularjs.core.{Route, RouteProvider, Scope, Location}
import com.greencatsoft.angularjs.{Config, injectable, AbstractController}
import org.apache.gearpump.dashboard.services.RestApiService

import scala.scalajs.js
import scala.scalajs.js.annotation.JSExport

trait AppsScope extends Scope {
  var view: (String) => Unit = js.native
  var kill: (String) => Unit = js.native
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

  scope.view = view
  scope.kill = kill

  def view(id: String): Unit = {
    location.path("/apps/app/" + id)
  }

  def kill(id: String): Unit = {
    restApi.killApp(id)
  }

}
