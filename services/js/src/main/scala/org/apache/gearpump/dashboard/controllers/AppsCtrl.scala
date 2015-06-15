package org.apache.gearpump.dashboard.controllers

import scala.scalajs.concurrent.JSExecutionContext.Implicits.runNow
import com.greencatsoft.angularjs.core.{Route, RouteProvider, Scope, Location}
import com.greencatsoft.angularjs.{Config, injectable, AbstractController}
import org.apache.gearpump.dashboard.services.{UtilService, RestApiService}
import org.apache.gearpump.shared.Messages.{AppMasterData, AppMastersData, AppMasterActive}

import scala.scalajs.js
import scala.scalajs.js.UndefOr
import scala.scalajs.js.annotation.JSExport
import scala.util.{Failure, Success}

trait AppsScope extends Scope {
  var apps: js.Array[AppMasterData] = js.native
  var active: js.Function1[AppMasterData,Boolean] = js.native
  var kill: js.Function1[Int,Unit] = js.native
  var stringToDateTime: js.Function1[UndefOr[Any], String] = js.native
  var view: js.Function1[Int,Unit] = js.native
}

@JSExport
@injectable("AppsConfig")
class AppsConfig(routeProvider: RouteProvider) extends Config {
  println("AppsConfig")
  routeProvider.when ("/apps", Route("views/apps/apps.html", "Applications", "AppsCtrl") )
}

@JSExport
@injectable("AppsCtrl")
class AppsCtrl(scope: AppsScope, location: Location, restApi: RestApiService, util: UtilService)
  extends AbstractController[AppsScope](scope) {

  println("AppsCtrl")

  scope.apps = js.Array[AppMasterData]()
  scope.active = active _
  scope.kill = kill _
  scope.stringToDateTime = stringToDateTime _
  scope.view = view _

  def view(id: Int): Unit = {
    location.path("/apps/app/" + id)
  }

  def kill(id: Int): Unit = {
    restApi.killApp(id.toString)
  }

  def active(app: AppMasterData): Boolean = {
    val active = app.status == AppMasterActive
    active
  }

  def stringToDateTime(s: UndefOr[Any]): String = {
    util.stringToDateTime(s)
  }

  import js.JSConverters._

  restApi.subscribe("/appmasters") onComplete {
    case Success(value) =>
      val data = upickle.read[AppMastersData](value)
      scope.apps = data.appMasters.toJSArray
    case Failure(t) =>
      println(s"Failed to get master ${t.getMessage}")
  }

}
